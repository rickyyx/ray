# coding: utf-8
import os
import sys
import unittest

import pytest  # noqa

import mock

from ray.autoscaler.v2.instance_manager.config import InstanceReconcileConfig
from ray.autoscaler.v2.instance_manager.reconciler import StuckInstanceReconciler
from ray.autoscaler.v2.tests.util import create_instance
from ray.core.generated.instance_manager_pb2 import Instance

s_to_ns = int(1e9)


@mock.patch("time.time_ns", mock.MagicMock(return_value=10 * s_to_ns))
def test_stuck_requested_instances():
    config = InstanceReconcileConfig(
        request_status_timeout_s=5,
        max_num_request_to_allocate=2,
    )
    instances = [
        create_instance(
            "no-update",
            Instance.REQUESTED,
            status_times=[(Instance.REQUESTED, 9 * s_to_ns)],
        ),
        create_instance(
            "retry",
            Instance.REQUESTED,
            status_times=[(Instance.REQUESTED, 2 * s_to_ns)],
        ),
        create_instance(
            "failed",
            Instance.REQUESTED,
            status_times=[
                (Instance.REQUESTED, 1 * s_to_ns),
                (Instance.REQUESTED, 2 * s_to_ns),
            ],
        ),
        create_instance(
            "success",
            Instance.ALLOCATED,
            status_times=[
                (Instance.REQUESTED, 1 * s_to_ns),
                (Instance.ALLOCATED, 2 * s_to_ns),
            ],
        ),
    ]
    updates = StuckInstanceReconciler.reconcile(instances, config)
    expected_status = {
        "retry": Instance.QUEUED,
        "failed": Instance.ALLOCATION_FAILED,
    }
    assert expected_status == {
        update.instance_id: update.new_instance_status for _, update in updates.items()
    }


@unittest.mock.patch("time.time_ns")
@pytest.mark.parametrize(
    "cur_status,expect_status",
    [
        (Instance.ALLOCATED, Instance.STOPPING),
        (Instance.RAY_INSTALLING, Instance.RAY_INSTALL_FAILED),
        (Instance.STOPPING, Instance.STOPPING),
    ],
)
def test_stuck_allocated_instances(mock_time_ns, cur_status, expect_status):
    timeout_s = 5
    cur_time_s = 20
    mock_time_ns.return_value = cur_time_s * s_to_ns
    config = InstanceReconcileConfig(
        allocate_status_timeout_s=timeout_s,
        stopping_status_timeout_s=timeout_s,
        ray_install_status_timeout_s=timeout_s,
    )
    instances = [
        create_instance(
            "no-update",
            cur_status,
            status_times=[(cur_status, (cur_time_s - timeout_s + 1) * s_to_ns)],
        ),
        create_instance(
            "updated",
            cur_status,
            status_times=[(cur_status, (cur_time_s - timeout_s - 1) * s_to_ns)],
        ),
    ]
    updates = StuckInstanceReconciler.reconcile(instances, config)
    print(updates)
    expected_status = {
        "updated": expect_status,
    }
    assert expected_status == {
        update.instance_id: update.new_instance_status for _, update in updates.items()
    }


@unittest.mock.patch("time.time_ns")
def test_warn_stuck_transient_instances(mock_time_ns):
    cur_time_s = 10
    mock_time_ns.return_value = cur_time_s * s_to_ns
    timeout_s = 5

    config = InstanceReconcileConfig(
        transient_status_warn_interval_s=timeout_s,
    )
    status = Instance.RAY_STOPPING
    instances = [
        create_instance(
            "no-warn",
            status,
            status_times=[(status, (cur_time_s - timeout_s + 1) * s_to_ns)],
        ),
        create_instance(
            "warn",
            status,
            status_times=[(status, (cur_time_s - timeout_s - 1) * s_to_ns)],
        ),
    ]
    mock_logger = mock.MagicMock()
    updates = StuckInstanceReconciler.reconcile(instances, config, _logger=mock_logger)
    print(updates)
    assert updates == {}
    assert mock_logger.warning.call_count == 1
    assert "Instance warn is stuck" in mock_logger.warning.call_args[0][0]


@unittest.mock.patch("time.time_ns")
def test_stuck_instances_no_op(mock_time_ns):
    # Large enough to not trigger any timeouts
    mock_time_ns.return_value = 999999 * s_to_ns

    config = InstanceReconcileConfig()

    all_status = set(Instance.InstanceStatus.values())
    reconciled_stuck_statuses = {
        Instance.REQUESTED,
        Instance.ALLOCATED,
        Instance.RAY_INSTALLING,
        Instance.STOPPING,
    }
    transient_statuses = {
        Instance.RAY_STOPPING,
        Instance.RAY_INSTALL_FAILED,
        Instance.RAY_STOPPED,
        Instance.QUEUED,
    }
    no_op_statuses = all_status - reconciled_stuck_statuses - transient_statuses

    for status in no_op_statuses:
        instances = [
            create_instance(
                "no-op",
                status,
                status_times=[(status, 1 * s_to_ns)],
            ),
        ]
        mock_logger = mock.MagicMock()
        updates = StuckInstanceReconciler.reconcile(
            instances, config, _logger=mock_logger
        )
        assert updates == {}
        assert mock_logger.warning.call_count == 0


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
