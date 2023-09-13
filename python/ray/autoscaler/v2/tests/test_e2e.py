import os
import sys
import time
from typing import Dict

import pytest

import ray
from ray._private.resource_spec import HEAD_NODE_RESOURCE_NAME
from ray._private.test_utils import run_string_as_driver_nonblocking, wait_for_condition
from ray.autoscaler.v2.sdk import get_cluster_status
from ray.cluster_utils import AutoscalingCluster
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.state.api import list_placement_groups, list_tasks


def is_head_node_from_resource_usage(usage: Dict[str, float]) -> bool:
    if HEAD_NODE_RESOURCE_NAME in usage:
        return True
    return False


# TODO(rickyx): We are NOT able to counter multi-node inconsistency yet. The problem is
# right now, when node A (head node) has an infeasible task,
# node B just finished running previous task.
# the actual cluster view will be:
#   node A: 1 pending task (infeasible)
#   node B: 0 pending task, CPU used = 0
#
# However, when node B's state is not updated on node A, the cluster view will be:
#  node A: 1 pending task (infeasible)
#  node B: 0 pending task, but **CPU used = 1**
#
# @pytest.mark.parametrize("mode", (["single_node", "multi_node"]))
@pytest.mark.parametrize("mode", (["single_node"]))
def test_scheduled_task_no_pending_demand(mode):

    # So that head node will need to dispatch tasks to worker node.
    num_head_cpu = 0 if mode == "multi_node" else 1

    cluster = AutoscalingCluster(
        head_resources={"CPU": num_head_cpu},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 1},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 1,
            },
        },
    )

    driver_script = """
import time
import ray
@ray.remote(num_cpus=1)
def foo():
  return True

ray.init("auto")

while True:
    assert(ray.get(foo.remote()))
"""

    try:
        cluster.start()
        ray.init("auto")

        run_string_as_driver_nonblocking(driver_script)

        def tasks_run():
            tasks = list_tasks()

            # Waiting til the driver in the run_string_as_driver_nonblocking is running
            assert len(tasks) > 0

            return True

        wait_for_condition(tasks_run)

        for _ in range(30):
            # verify no pending task + with resource used.
            status = get_cluster_status()
            has_task_demand = len(status.resource_demands.ray_task_actor_demand) > 0
            has_task_usage = False

            for usage in status.cluster_resource_usage:
                if usage.resource_name == "CPU":
                    has_task_usage = usage.used > 0
            print(status.cluster_resource_usage)
            print(status.resource_demands.ray_task_actor_demand)
            assert not (has_task_demand and has_task_usage), status
            time.sleep(0.1)
    finally:
        # TODO(rickyx): refactor into a fixture for autoscaling cluster.
        ray.shutdown()
        cluster.shutdown()


def test_placement_group_consistent():
    # Test that continuously creating and removing placement groups
    # does not leak pending resource requests.
    import time

    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 1},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
    )
    driver_script = """

import ray
import time
# Import placement group APIs.
from ray.util.placement_group import (
    placement_group,
    placement_group_table,
    remove_placement_group,
)

ray.init("auto")

# Reserve all the CPUs of nodes, X= num of cpus, N = num of nodes
while True:
    pg = placement_group([{"CPU": 1}])
    ray.get(pg.ready())
    time.sleep(0.5)
    remove_placement_group(pg)
    time.sleep(0.5)
"""

    try:
        cluster.start()
        ray.init("auto")

        run_string_as_driver_nonblocking(driver_script)

        def pg_created():
            pgs = list_placement_groups()
            assert len(pgs) > 0

            return True

        wait_for_condition(pg_created)

        for _ in range(30):
            # verify no pending request + resource used.
            status = get_cluster_status()
            has_pg_demand = len(status.resource_demands.placement_group_demand) > 0
            has_pg_usage = False
            for usage in status.cluster_resource_usage:
                has_pg_usage = has_pg_usage or "bundle" in usage.resource_name
            print(has_pg_demand, has_pg_usage)
            assert not (has_pg_demand and has_pg_usage), status
            time.sleep(0.1)
    finally:
        ray.shutdown()
        cluster.shutdown()


def test_placement_group_removal_idle_node():
    # Test that nodes become idle after placement group removal.
    cluster = AutoscalingCluster(
        head_resources={"CPU": 2},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 2},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
    )
    try:
        cluster.start()
        ray.init("auto")

        # Schedule a pg on nodes
        pg = placement_group([{"CPU": 2}] * 3, strategy="STRICT_SPREAD")
        ray.get(pg.ready())

        time.sleep(2)
        remove_placement_group(pg)

        from ray.autoscaler.v2.sdk import get_cluster_status

        def verify():
            cluster_state = get_cluster_status()

            # Verify that nodes are idle.
            assert len((cluster_state.healthy_nodes)) == 3
            for node in cluster_state.healthy_nodes:
                assert node.node_status == "IDLE"
                assert node.resource_usage.idle_time_ms >= 1000

            return True

        wait_for_condition(verify)
    finally:
        ray.shutdown()
        cluster.shutdown()


def test_object_store_memory_idle_node(shutdown_only):

    ray.init()

    obj = ray.put("hello")

    def verify():
        state = get_cluster_status()
        for node in state.healthy_nodes:
            assert node.node_status == "RUNNING"
            assert node.used_resources()["object_store_memory"] > 0
            return True

    wait_for_condition(verify)

    del obj

    import time

    time.sleep(1)

    def verify():
        state = get_cluster_status()
        for node in state.healthy_nodes:
            assert node.node_status == "IDLE"
            assert node.used_resources()["object_store_memory"] == 0
            assert node.resource_usage.idle_time_ms >= 1000
            return True

    wait_for_condition(verify)


def test_serve_num_replica_idle_node():
    # Test that nodes become idle after serve scaling down.
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 4},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 30,
            },
        },
        idle_timeout_minutes=999,
    )

    from ray import serve

    @serve.deployment(ray_actor_options={"num_cpus": 2})
    class Deployment:
        def __call__(self):
            return "hello"

    try:
        cluster.start(override_env={"RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S": "2"})
        # 5 workers nodes should be busy and have 2(replicas) * 2(cpus per replicas)
        # = 4 cpus used
        serve.run(Deployment.options(num_replicas=10).bind())

        expected_num_workers = 5

        def verify():
            cluster_state = get_cluster_status()

            # Verify that nodes are busy.
            assert len((cluster_state.healthy_nodes)) == expected_num_workers + 1
            for node in cluster_state.healthy_nodes:
                assert node.node_status == "RUNNING"
                if not is_head_node_from_resource_usage(node.total_resources()):
                    available = node.available_resources()
                    assert available["CPU"] == 0
            return True

        wait_for_condition(verify)

        # Downscale to 1 replicas, 4 workers nodes should be idle.
        serve.run(Deployment.options(num_replicas=1).bind())

        def verify():
            cluster_state = get_cluster_status()
            # We should only have 1 running worker for the 1 replica, the rest idle.
            expected_idle_workers = expected_num_workers - 1

            assert len((cluster_state.healthy_nodes)) == expected_num_workers + 1
            idle_nodes = []

            for node in cluster_state.healthy_nodes:
                if not is_head_node_from_resource_usage(node.total_resources()):
                    available = node.available_resources()
                    if node.node_status == "IDLE":
                        assert available["CPU"] == 4
                        idle_nodes.append(node)
            from rich import print

            print(cluster_state.healthy_nodes)
            assert len(idle_nodes) == expected_idle_workers
            return True

        # A long sleep is needed for serve proxy to be removed.
        wait_for_condition(verify, timeout=15, retry_interval_ms=1000)

    finally:
        ray.shutdown()
        cluster.shutdown()


def test_non_corrupted_resources():
    """
    Test that when node's local gc happens due to object store pressure,
    the message doesn't corrupt the resource view on the gcs.A
    See issue https://github.com/ray-project/ray/issues/39644
    """
    num_worker_nodes = 5
    cluster = AutoscalingCluster(
        head_resources={"CPU": 2, "object_store_memory": 100 * 1024 * 1024},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 2},
                "node_config": {},
                "min_workers": num_worker_nodes,
                "max_workers": num_worker_nodes,
            },
        },
    )

    driver_script = """

import ray
import time

ray.init("auto")

@ray.remote(num_cpus=1)
def foo():
    ray.put(bytearray(1024*1024* 50))


while True:
    ray.get([foo.remote() for _ in range(50)])
"""

    try:
        # This should trigger many COMMANDS messages from TryGlobalGC on GCS.
        cluster.start(
            _system_config={
                "debug_dump_period_milliseconds": 10,
                "raylet_report_resources_period_milliseconds": 10000,
                "global_gc_min_interval_s": 1,
                "local_gc_interval_s": 1,
                "high_plasma_storage_usage": 0.2,
                "raylet_check_gc_period_milliseconds": 10,
            },
        )
        ray.init("auto")

        from ray.autoscaler.v2.sdk import get_cluster_status

        def nodes_up():
            cluster_state = get_cluster_status()
            return len(cluster_state.healthy_nodes) == num_worker_nodes + 1

        wait_for_condition(nodes_up)

        # Schedule tasks
        run_string_as_driver_nonblocking(driver_script)
        start = time.time()

        # Check the cluster state for 10 seconds
        while time.time() - start < 10:
            cluster_state = get_cluster_status()

            # Verify total cluster resources never change
            assert len((cluster_state.healthy_nodes)) == num_worker_nodes + 1
            assert cluster_state.total_resources()["CPU"] == 2 * (num_worker_nodes + 1)

    finally:
        ray.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
