from collections import defaultdict
from typing import Dict
import ray
from ray.experimental.state.common import ListApiOptions, StateResource
from ray._private.test_utils import (
    raw_metrics,
    run_string_as_driver_nonblocking,
    wait_for_condition,
)
from ray.experimental.state.api import StateApiClient, list_tasks

from ray._private.worker import RayContext

_SYSTEM_CONFIG = {
    "task_events_report_interval_ms": 100,
    "metrics_report_interval_ms": 200,
    "enable_timeline": False,
}


def aggregate_task_event_metric(info: RayContext) -> Dict:
    """
    Aggregate metrics of task events into:
        {
            "REPORTED": ray_gcs_task_manager_task_events_reported,
            "STORED": ray_gcs_task_manager_task_events_stored,
            "DROPPED_PROFILE_EVENT":
                ray_gcs_task_manager_task_events_dropped PROFILE_EVENT,
            "DROPPED_STATUS_EVENT":
                ray_gcs_task_manager_task_events_dropped STATUS_EVENT,
        }
    """
    res = raw_metrics(info)
    task_events_info = defaultdict(int)
    if "ray_gcs_task_manager_task_events_dropped" in res:
        for sample in res["ray_gcs_task_manager_task_events_dropped"]:
            if "Type" in sample.labels and sample.labels["Type"] != "":
                task_events_info["DROPPED_" + sample.labels["Type"]] += sample.value

    if "ray_gcs_task_manager_task_events_stored" in res:
        for sample in res["ray_gcs_task_manager_task_events_stored"]:
            task_events_info["STORED"] += sample.value

    if "ray_gcs_task_manager_task_events_reported" in res:
        for sample in res["ray_gcs_task_manager_task_events_reported"]:
            task_events_info["REPORTED"] += sample.value

    return task_events_info


def test_status_task_events_metrics(shutdown_only):
    info = ray.init(num_cpus=1, _system_config=_SYSTEM_CONFIG)

    # Start a task
    @ray.remote
    def f():
        pass

    for _ in range(10):
        ray.get(f.remote())

    def verify():
        metric = aggregate_task_event_metric(info)
        assert metric["REPORTED"] >= 10, (
            "At least 10 tasks events should be reported. "
            "Could be more than 10 with multiple flush."
        )
        assert metric["STORED"] == 10, "10 task's events should be stored."

        return True

    wait_for_condition(
        verify,
        timeout=20,
        retry_interval_ms=100,
    )


def test_fault_tolerance_parent_failed(shutdown_only):
    ray.init(num_cpus=4, _system_config=_SYSTEM_CONFIG)
    import time

    # Each parent task spins off 2 child task, where each child spins off
    # 1 grand_child task.
    NUM_CHILD = 2

    @ray.remote
    def grand_child():
        time.sleep(999)

    @ray.remote
    def child():
        ray.get(grand_child.remote())

    @ray.remote
    def parent():
        for _ in range(NUM_CHILD):
            child.remote()
        # Sleep for a bit and kill itself.
        time.sleep(3)
        raise ValueError("parent task is expected to fail")

    parent.remote()

    def verify():
        tasks = list_tasks()
        assert len(tasks) == 5, (
            "Incorrect number of tasks are reported. "
            "Expected length: 1 parent + 2 child + 2 grand_child tasks"
        )
        print(tasks)
        for task in tasks:
            assert task["state"] == "FAILED"

        return True

    wait_for_condition(
        verify,
        timeout=10,
        retry_interval_ms=500,
    )


def test_handle_driver_tasks(shutdown_only):
    ray.init(_system_config=_SYSTEM_CONFIG)

    job_id = ray.get_runtime_context().get_job_id()
    script = """
import ray
import time
ray.init("auto")

@ray.remote
def f():
    time.sleep(3)


ray.get(f.remote())
"""
    run_string_as_driver_nonblocking(script)

    client = StateApiClient()

    def list_tasks(exclude_driver):
        return client.list(
            StateResource.TASKS,
            # Filter out this driver
            options=ListApiOptions(
                exclude_driver=exclude_driver, filters=[("job_id", "!=", job_id)]
            ),
            raise_on_missing_output=True,
        )

    # Check driver running
    def verify():
        tasks_with_driver = list_tasks(exclude_driver=False)
        assert len(tasks_with_driver) == 2, tasks_with_driver
        task_types = {task["type"] for task in tasks_with_driver}
        assert task_types == {"NORMAL_TASK", "DRIVER_TASK"}

        for task in tasks_with_driver:
            if task["type"] == "DRIVER_TASK":
                assert task["state"] == "RUNNING", task

        return True

    wait_for_condition(verify, timeout=15, retry_interval_ms=1000)

    # Check driver finishes
    def verify():
        tasks_with_driver = list_tasks(exclude_driver=False)
        assert len(tasks_with_driver) == 2, tasks_with_driver
        for task in tasks_with_driver:
            if task["type"] == "DRIVER_TASK":
                assert task["state"] == "FINISHED", task

        tasks_no_driver = list_tasks(exclude_driver=True)
        assert len(tasks_no_driver) == 1, tasks_no_driver
        return True

    wait_for_condition(verify)
