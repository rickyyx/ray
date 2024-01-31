# coding: utf-8
import os
import sys

import pytest
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    LaunchNodeError,
)  # noqa

from ray.autoscaler.v2.instance_manager.reconciler import CloudProviderReconciler
from ray.autoscaler.v2.tests.util import create_instance
from ray.core.generated.instance_manager_pb2 import Instance


def test_cloud_instance_allocated_with_request_ids():
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

    updates = CloudProviderReconciler.reconcile(cloud_instances, [], instances)
    assert len(updates) == 2
    for update in updates:
        assert update.new_instance_status == Instance.ALLOCATED
        if update.instance_id == "i-1":
            assert update.cloud_instance_id == "c-1"
        elif update.instance_id == "i-2":
            assert update.cloud_instance_id == "c-2"


def test_cloud_instance_allocated_partial_request():
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
            is_running=True,
        ),
    }

    updates = CloudProviderReconciler.reconcile(cloud_instances, [], instances)
    assert len(updates) == 1
    assert updates[0].instance_id == "i-1"
    assert updates[0].new_instance_status == Instance.ALLOCATED
    assert updates[0].cloud_instance_id == "c-1"


def test_cloud_instance_allocated_queued():
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
            is_running=True,
        )
    }

    updates = CloudProviderReconciler.reconcile(cloud_instances, [], instances)
    assert len(updates) == 1
    assert updates[0].instance_id == "i-1"
    assert updates[0].new_instance_status == Instance.ALLOCATED
    assert updates[0].cloud_instance_id == "c-1"


def test_cloud_instance_launch_failures():
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
        )
    ]

    cloud_instances = {
        # type-2 is ok.
        "c-1": CloudInstance(
            cloud_instance_id="c-1",
            node_type="type-2",
            request_id="l1",
            is_running=True,
        ),
    }

    updates = CloudProviderReconciler.reconcile(
        cloud_instances, cloud_errors, instances
    )
    assert len(updates) == 2
    for u in updates:
        if u.instance_id == "i-1":
            assert u.new_instance_status == Instance.ALLOCATION_FAILED
        elif u.instance_id == "i-2":
            assert u.new_instance_status == Instance.ALLOCATED
            assert u.cloud_instance_id == "c-1"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
