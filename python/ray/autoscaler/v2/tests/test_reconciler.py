# coding: utf-8
import os
import sys
import unittest

import pytest
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    LaunchNodeError,
)  # noqa

from ray.autoscaler.v2.instance_manager.reconciler import CloudProviderReconciler
from ray.autoscaler.v2.tests.util import create_instance
from ray.core.generated.instance_manager_pb2 import Instance


class TestCloudProviderReconciler:
    @staticmethod
    def test_allocated_with_request_ids():
        # Same type different requests.
        instances = [
            create_instance(
                "i-1",
                Instance.REQUESTED,
                launch_request_id="l1",
                instance_type="type-1",
            ),
            create_instance(
                "i-2",
                Instance.REQUESTED,
                launch_request_id="l2",
                instance_type="type-1",
            ),
        ]

        cloud_instances = {
            "c-1": CloudInstance(
                cloud_instance_id="c-1",
                node_type="type-1",
                request_id="l1",
                is_running=True,
            ),
            "c-2": CloudInstance(
                cloud_instance_id="c-2",
                node_type="type-1",
                request_id="l2",
                is_running=True,
            ),
        }

        updates = CloudProviderReconciler.reconcile(
            cloud_instances, [], instances, ray_install_needed=True
        )
        assert len(updates) == 2
        for update in updates:
            assert update.new_instance_status == Instance.ALLOCATED
            if update.instance_id == "i-1":
                assert update.cloud_instance_id == "c-1"
            elif update.instance_id == "i-2":
                assert update.cloud_instance_id == "c-2"

    @staticmethod
    def test_allocated_partial_request():
        # Different types same request.
        instances = [
            create_instance(
                "i-1",
                Instance.REQUESTED,
                launch_request_id="l1",
                instance_type="type-1",
            ),
            create_instance(
                "not_allocated",
                Instance.REQUESTED,
                launch_request_id="l1",
                instance_type="type-2",
            ),
            create_instance(
                "i-3",
                Instance.ALLOCATED,
                launch_request_id="l1",
                instance_type="type-1",
                cloud_instance_id="c-2",
            ),
        ]

        cloud_instances = {
            "c-1": CloudInstance(
                cloud_instance_id="c-1",
                node_type="type-1",
                request_id="l1",
                is_running=True,
            ),
            # This already fulfilled.
            "c-2": CloudInstance(
                cloud_instance_id="c-2",
                node_type="type-2",
                request_id="l1",
                is_running=False,
            ),
        }

        updates = CloudProviderReconciler.reconcile(
            cloud_instances, [], instances, True
        )
        assert len(updates) == 1
        assert updates[0].instance_id == "i-1"
        assert updates[0].new_instance_status == Instance.ALLOCATED
        assert updates[0].cloud_instance_id == "c-1"

    @staticmethod
    def test_allocated_queued():
        # Test queued instances could be allocated if they have been requested.
        instances = [
            create_instance(
                "i-1",
                Instance.QUEUED,
                launch_request_id="l1",
                instance_type="type-1",
            ),
            # This has never been requested w/o a launch_request_id.
            create_instance(
                "i-2",
                Instance.QUEUED,
                instance_type="type-1",
            ),
        ]

        cloud_instances = {
            "c-1": CloudInstance(
                cloud_instance_id="c-1",
                node_type="type-1",
                request_id="l1",
                is_running=False,
            )
        }

        updates = CloudProviderReconciler.reconcile(
            cloud_instances, [], instances, True
        )
        assert len(updates) == 1
        assert updates[0].instance_id == "i-1"
        assert updates[0].new_instance_status == Instance.ALLOCATED
        assert updates[0].cloud_instance_id == "c-1"

    @staticmethod
    def test_launch_failures():
        # Test that launch failures will fail the allocation within the same request
        instances = [
            create_instance(
                "i-1",
                Instance.REQUESTED,
                launch_request_id="l1",
                instance_type="type-1",
            ),
            create_instance(
                "i-2",
                Instance.REQUESTED,
                launch_request_id="l1",
                instance_type="type-2",
            ),
        ]

        cloud_errors = [
            LaunchNodeError(
                request_id="l1",
                count=1,
                node_type="type-1",
                timestamp_ns=1,
                exception=None,
                details="",
            )
        ]

        cloud_instances = {
            # type-2 is ok.
            "c-1": CloudInstance(
                cloud_instance_id="c-1",
                node_type="type-2",
                request_id="l1",
                is_running=False,
            ),
        }

        updates = CloudProviderReconciler.reconcile(
            cloud_instances, cloud_errors, instances, True
        )
        assert len(updates) == 2
        for u in updates:
            if u.instance_id == "i-1":
                assert u.new_instance_status == Instance.ALLOCATION_FAILED
            elif u.instance_id == "i-2":
                assert u.new_instance_status == Instance.ALLOCATED
                assert u.cloud_instance_id == "c-1"
            else:
                assert False, f"unexpected instance {u.instance_id}"

    @staticmethod
    def test_launch_partial_failure():
        # When the cloud provider fails to launch some instances (but reported
        # errors fro all) we should still allocate the ones that succeeded.
        # This would happen since we don't guarantee atomicity of the launch
        # requests.

        instances = [
            create_instance(
                "i-1",
                Instance.REQUESTED,
                launch_request_id="l1",
                instance_type="type-1",
            ),
            create_instance(
                "i-2",
                Instance.REQUESTED,
                launch_request_id="l1",
                instance_type="type-1",
            ),
        ]

        cloud_errors = [
            LaunchNodeError(
                request_id="l1",
                count=2,  # The request failed.
                node_type="type-1",
                timestamp_ns=1,
                exception=None,
                details="",
            )
        ]

        cloud_instances = {
            "c-1": CloudInstance(
                cloud_instance_id="c-1",
                node_type="type-1",
                request_id="l1",
                is_running=True,
            ),
        }

        updates = CloudProviderReconciler.reconcile(
            cloud_instances, cloud_errors, instances, True
        )

        assert len(updates) == 2
        status_updates = {u.new_instance_status for u in updates}
        assert status_updates == {Instance.ALLOCATED, Instance.ALLOCATION_FAILED}

    @staticmethod
    @pytest.mark.parametrize("ray_install_needed", [True, False])
    def test_optional_ray_install(ray_install_needed: bool):
        # Test that instances should be allocated if ray_install_needed is False.
        # or ray_install if True.

        # Should just be ALLOCATED if cloud instance is not yet running (e.g. pending)
        instances = [
            create_instance(
                "i-1",
                Instance.ALLOCATED,
                launch_request_id="l1",
                instance_type="type-1",
                cloud_instance_id="c-1",
            ),
        ]

        cloud_instances = {
            "c-1": CloudInstance(
                cloud_instance_id="c-1",
                node_type="type-1",
                request_id="l1",
                is_running=False,
            )
        }

        updates = CloudProviderReconciler.reconcile(
            cloud_instances, [], instances, ray_install_needed
        )
        assert len(updates) == 0

        # But if the instance is running, we should transition it to RAY_INSTALLING if
        # ray_install_needed is True.
        cloud_instances["c-1"].is_running = True

        updates = CloudProviderReconciler.reconcile(
            cloud_instances, [], instances, ray_install_needed
        )
        if ray_install_needed:
            assert len(updates) == 1
            assert updates[0].new_instance_status == Instance.RAY_INSTALLING
        else:
            assert len(updates) == 0

    @staticmethod
    @pytest.mark.parametrize(
        "instance_status_name",
        [
            Instance.InstanceStatus.Name(Instance.ALLOCATED),
            Instance.InstanceStatus.Name(Instance.RAY_INSTALLING),
            Instance.InstanceStatus.Name(Instance.RAY_RUNNING),
            Instance.InstanceStatus.Name(Instance.RAY_STOPPING),
            Instance.InstanceStatus.Name(Instance.RAY_STOPPED),
            Instance.InstanceStatus.Name(Instance.STOPPING),
        ],
    )
    def test_stopped_cloud_instances(instance_status_name: str):
        # Test that if a cloud instance crashed or was terminated, we should mark the
        # instance as terminated.

        instances = [
            create_instance(
                "i-1",
                Instance.InstanceStatus.Value(instance_status_name),
                launch_request_id="l1",
                instance_type="type-1",
                cloud_instance_id="c-1",
            ),
        ]

        cloud_instances = {}

        updates = CloudProviderReconciler.reconcile(
            cloud_instances, [], instances, True
        )

        assert len(updates) == 1
        assert updates[0].new_instance_status == Instance.STOPPED


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
