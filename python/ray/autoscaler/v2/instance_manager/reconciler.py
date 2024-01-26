import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Dict, List, Optional

from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.autoscaler.v2.instance_manager.config import InstanceReconcileConfig
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    CloudInstanceId,
    CloudInstanceProviderError,
    ICloudInstanceProvider,
)
from ray.core.generated.autoscaler_pb2 import ClusterResourceState
from ray.core.generated.instance_manager_pb2 import Instance as IMInstance
from ray.core.generated.instance_manager_pb2 import (
    InstanceUpdateEvent as IMInstanceUpdateEvent,
)

logger = logging.getLogger(__name__)


class IReconciler(ABC):
    @staticmethod
    @abstractmethod
    def reconcile(*args, **kwargs) -> Dict[str, IMInstanceUpdateEvent]:
        """
        Reconcile the status of Instance Manager(IM) instances to a valid state.
        It should yield a list of instance update events that will be applied to the
        instance storage by the IM.

        There should not be any other side effects other than producing the instance
        update events. This method is expected to be idempotent and deterministic.

        Returns:
            A dict of instance id to instance update event.

        """
        pass


class StuckInstanceReconciler(IReconciler):
    @staticmethod
    def reconcile(
        instances: List[IMInstance],
        config: InstanceReconcileConfig,
        _logger: logging.Logger = logger,
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """
        ***********************
        **STUCK STATES
        ***********************
        For instances that's stuck in the following states, we will try to recover
        them:

        - REQUESTED -> QUEUED
            WHEN: happens when the cloud instance provider is not able to allocate
                cloud instances, and the instance is in REQUESTED state for longer than
                the timeout.
            ACTION: We will retry the allocation by setting it back to QUEUED.

        - ALLOCATED -> STOPPING
            WHEN: happens when the cloud instance provider is responsible for
                installing and running ray on the cloud instance, and the cloud instance
                is not able to start ray for some reason.
            ACTION: We will terminate the instances since it's a leak after a timeout
                for installing/running ray, setting it to STOPPING.

        - RAY_INSTALLING -> RAY_INSTALL_FAILED
            WHEN: this happens when the cloud instance provider is responsible for
                installing ray on the cloud instance, and the cloud instance is not
                able to start ray for some reason (e.g. immediate crash after ray start)
            ACTION: We will fail the installation and terminate the instance after a
                timeout, transitioning to RAY_INSTALL_FAILED.

        # TODO(rickyx): Maybe we should add a TO_STOP status so we could do retry
        # stopping better
        - STOPPING -> STOPPING
            WHEN: the cloud node provider taking long or failed to terminate the cloud
                instance.
            ACTION: We will retry the termination by setting it to STOPPING, triggering
                another termination request to the cloud instance provider.

        ***********************
        **TRANSIENT STATES
        ***********************
        These states are expected to be transient, when they don't, it usually indicates
        a bug in the instance manager or the cloud instance provider as the process loop
        which is none-blocking is now somehow not making progress.

        # TODO(rickyx): When we added the timeout for draining, we should also add
        # timeout for the RAY_STOPPING states.
        - RAY_STOPPING:
            WHEN: this state should be transient, instances in this state are being
                drained through the drain protocol (and the drain request should
                eventually finish).
            ACTION: There's no drain request timeout so we will wait for the drain to
                finish, and print errors if it's taking too long.

        - RAY_INSTALL_FAILED or RAY_STOPPED
            WHEN: these state should be transient, cloud instances should be terminating
                or terminated soon. This could happen if the subscribers for the status
                transition failed to act upon the status update.
            ACTION: Print errors if stuck for too long - this is a bug.

        - QUEUED
            WHEN: if there's many instances queued to be launched, and we limit the
                number of concurrent instance launches, the instance might be stuck
                in QUEUED state for a long time.
            ACTION: No actions, print warnings if needed (or adjust the scaling-up
                configs)

        ***********************
        **LONG LASTING STATES
        ***********************
        These states are expected to be long lasting. So we will not take any actions.

        - RAY_RUNNING:
            Normal state - unlimited time.
        - STOPPED:
            Normal state - terminal, and unlimited time.
        - ALLOCATION_FAILED:
            Normal state - terminal, and unlimited time.

        """

        instances_by_status = defaultdict(list)
        for instance in instances:
            instances_by_status[instance.status].append(instance)

        im_updates = {}
        im_updates.update(
            StuckInstanceReconciler._reconcile_requested(
                instances_by_status[IMInstance.REQUESTED],
                request_status_timeout_s=config.request_status_timeout_s,
                max_num_request_to_allocate=config.max_num_request_to_allocate,
            )
        )
        for cur_status, next_status, timeout in [
            (
                IMInstance.ALLOCATED,
                IMInstance.STOPPING,
                config.allocate_status_timeout_s,
            ),
            (
                IMInstance.RAY_INSTALLING,
                IMInstance.RAY_INSTALL_FAILED,
                config.ray_install_status_timeout_s,
            ),
            (
                IMInstance.STOPPING,
                IMInstance.STOPPING,
                config.stopping_status_timeout_s,
            ),
        ]:
            im_updates.update(
                StuckInstanceReconciler._reconcile_status(
                    instances_by_status[cur_status],
                    status_timeout_s=timeout,
                    cur_status=cur_status,
                    new_status=next_status,
                )
            )

        for status in [
            IMInstance.RAY_STOPPING,
            IMInstance.RAY_INSTALL_FAILED,
            IMInstance.RAY_STOPPED,
            IMInstance.QUEUED,
        ]:
            StuckInstanceReconciler._warn_transient_status(
                instances_by_status[status],
                status=status,
                warn_interval_s=config.transient_status_warn_interval_s,
                logger=_logger,
            )

        return im_updates

    @staticmethod
    def _reconcile_requested(
        instances: List[IMInstance],
        request_status_timeout_s: int,
        max_num_request_to_allocate: int,
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """Change REQUESTED instances to QUEUED if they are stuck in REQUESTED state."""

        def _retry_or_fail_allocation(
            instance: IMInstance,
        ) -> Optional[IMInstanceUpdateEvent]:
            all_request_times_ns = sorted(
                InstanceUtil.get_status_transition_times_ns(
                    instance, select_instance_status=IMInstance.REQUESTED
                )
            )
            # Fail the allocation if we have tried too many times.
            if len(all_request_times_ns) >= max_num_request_to_allocate:
                return IMInstanceUpdateEvent(
                    instance_id=instance.instance_id,
                    new_instance_status=IMInstance.ALLOCATION_FAILED,
                    details=(
                        "Failed to allocate cloud instance after "
                        f"{len(all_request_times_ns)} attempts"
                    ),
                )

            # Retry the allocation if we have waited for too long.
            last_request_time_ns = all_request_times_ns[-1]
            if time.time_ns() - last_request_time_ns > request_status_timeout_s * 1e9:
                return IMInstanceUpdateEvent(
                    instance_id=instance.instance_id,
                    new_instance_status=IMInstance.QUEUED,
                    details="Retry allocation after "
                    "timeout={request_status_timeout_s}s",
                )
            return None

        updates = {}
        for ins in instances:
            update = _retry_or_fail_allocation(ins)
            if update:
                updates[ins.instance_id] = update

        return updates

    @staticmethod
    def _reconcile_status(
        instances: List[IMInstance],
        status_timeout_s: int,
        cur_status: IMInstance.InstanceStatus,
        new_status: IMInstance.InstanceStatus,
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """Change any instances that have not transitioned to the new status
        to the new status."""
        updates = {}
        for instance in instances:
            status_times_ns = InstanceUtil.get_status_transition_times_ns(
                instance, select_instance_status=cur_status
            )
            assert len(status_times_ns) == 1, (
                f"instance {instance.instance_id} has {len(status_times_ns)} "
                f"{IMInstance.InstanceStatus.Name(cur_status)} status"
            )

            status_time_ns = status_times_ns[0]

            if time.time_ns() - status_time_ns > status_timeout_s * 1e9:
                updates[instance.instance_id] = IMInstanceUpdateEvent(
                    instance_id=instance.instance_id,
                    new_instance_status=new_status,
                    details=(
                        "Failed to transition from "
                        f"{IMInstance.InstanceStatus.Name(cur_status)} after "
                        f"{status_timeout_s}s. "
                        "Transitioning to "
                        f"{IMInstance.InstanceStatus.Name(new_status)}."
                    ),
                )

        return updates

    @staticmethod
    def _warn_transient_status(
        instances: List[IMInstance],
        status: IMInstance.InstanceStatus,
        warn_interval_s: int,
        logger: logging.Logger,
    ):
        """Warn if any instance is stuck in a transient status for too long."""
        for instance in instances:
            status_times_ns = InstanceUtil.get_status_transition_times_ns(
                instance, select_instance_status=status
            )
            assert len(status_times_ns) == 1
            status_time_ns = status_times_ns[0]

            if time.time_ns() - status_time_ns > warn_interval_s * 1e9:
                logger.warning(
                    f"Instance {instance.instance_id} is stuck in "
                    f"{IMInstance.InstanceStatus.Name(status)} for too long."
                )


class RayStateReconciler(IReconciler):
    @staticmethod
    def reconcile(
        ray_cluster_resource_state: ClusterResourceState, instances: List[IMInstance]
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """
        Reconcile the instances states for Ray node state changes.

        1. Newly running ray nodes -> RAY_RUNNING status change.
            When an instance in (ALLOCATED or RAY_INSTALLING) successfully launched the
            ray node, we will discover the newly launched ray node and transition the
            instance to RAY_RUNNING.

        2. Newly dead ray nodes -> RAY_STOPPED status change.
            When an instance not in RAY_STOPPED is no longer running the ray node, we
            will transition the instance to RAY_STOPPED.
        """
        pass


class CloudProviderReconciler(IReconciler):
    @staticmethod
    def reconcile(
        provider: ICloudInstanceProvider, instances: List[IMInstance]
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """
        Reconcile the instance storage with the node provider.
        This is responsible for transitioning the instance status of:
        1. to ALLOCATED: when a REQUESTED instance could be assigned to an unassigned
            cloud instance.
        2. to STOPPED: when an ALLOCATED instance no longer has the assigned cloud
            instance found in node provider.
        3. to ALLOCATION_FAILED: when a REQUESTED instance failed to be assigned to
            an unassigned cloud instance.
        4. to STOPPING: when an instance being terminated fails to be terminated
            for some reasons, we will retry the termination.
        """
        pass

    @staticmethod
    def _reconcile_allocated(
        non_terminated_cloud_nodes: Dict[CloudInstanceId, CloudInstance]
    ) -> Dict[str, IMInstanceUpdateEvent]:

        """
        For any REQUESTED instances, if there's any unassigned non-terminated
        cloud instance that matches the instance type from the same request,
        assign it and transition it to ALLOCATED.

        """

        # Find all requested instances, by launch request id.

        # Find all non_terminated cloud_nodes by launch request id.

        # For the same request, find any unassigned non-terminated cloud instance
        # that matches the instance type. Assign them to REQUESTED instances
        # and transition them to ALLOCATED.
        pass

    @staticmethod
    def _reconcile_failed_to_allocate(
        self, cloud_provider_errors: List[CloudInstanceProviderError]
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """
        For any REQUESTED instances, if there's errors in allocating the cloud instance,
        transition the instance to ALLOCATION_FAILED.
        """

        # Find all requested instances, by launch request id.

        # Find all launch errors by launch request id.

        # For the same request, transition the instance to ALLOCATION_FAILED.
        # TODO(rickyx): we don't differentiate transient errors (which might be
        # retryable) and permanent errors (which are not retryable).
        pass

    @staticmethod
    def _reconcile_stopped(
        non_terminated_cloud_nodes: Dict[CloudInstanceId, CloudInstance]
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """
        For any IM (instance manager) instance with a cloud node id, if the mapped
        cloud instance is no longer running, transition the instance to STOPPED.
        """

        # Find all instances with cloud instance id, by cloud instance id.

        # Check if any matched cloud instance is still running.
        # If not, transition the instance to STOPPED. (The instance will have the cloud
        # instance id removed, and GCed later.)
        pass

    @staticmethod
    def _reconcile_failed_to_terminate(
        cloud_provider_errors: List[CloudInstanceProviderError],
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """
        For any STOPPING instances, if there's errors in terminating the cloud instance,
        we will retry the termination by setting it to STOPPING again.
        """
        pass
