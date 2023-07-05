from dataclasses import dataclass
from typing import Dict, List, Optional

from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeAvailabilitySummary,
)

NODE_DEATH_CAUSE_RAYLET_DIED = "RayletUnexpectedlyDied"


@dataclass(frozen=True, eq=True)
class ResourceUsage:
    # Resource name.
    resource_name: str = ""
    # Total resource.
    total: float = 0.0
    # Resource used.
    used: float = 0.0


@dataclass(frozen=True, eq=True)
class NodeUsage:
    # The node resource usage.
    usage: List[ResourceUsage]
    # How long the node has been idle.
    idle_time_ms: int


@dataclass(frozen=True, eq=True)
class NodeInfo:
    # The instance type name, e.g. p3.2xlarge
    instance_type_name: str
    # The detailed state of the node/request.
    # E.g. idle, running, setting-up, etc.
    node_status: str
    # ray node type name.
    ray_node_type_name: str
    # Cloud instance id.
    instance_id: str
    # Ip address of the node when alive.
    ip_address: str
    # ray node id. None if still pending.
    node_id: Optional[str] = None
    # Resource usage breakdown if node alive.
    resource_usage: Optional[NodeUsage] = None
    # Failure detail if the node failed.
    failure_detail: Optional[str] = None


@dataclass(frozen=True, eq=True)
class PendingLaunchRequest:
    # The instance type name, e.g. p3.2xlarge
    instance_type_name: str
    # ray node type name.
    ray_node_type_name: str
    # count.
    count: int


@dataclass(frozen=True, eq=True)
class ResourceRequestByCount:
    # Bundles in the demand.
    bundle: Dict[str, float]
    # Number of bundles with the same shape.
    count: int

    def __str__(self) -> str:
        return f"[{self.count} {self.bundle}]"


@dataclass(frozen=True, eq=True)
class ResourceDemand:
    # The bundles in the demand with shape and count info.
    bundles_by_count: List[ResourceRequestByCount]


@dataclass(frozen=True, eq=True)
class PlacementGroupResourceDemand(ResourceDemand):
    # Placement group strategy.
    strategy: str


    def __str__(self) -> str:
        s = ""
        s += f"{self.strategy}:  "
        for bundle in self.bundles_by_count[:-1]:
            s += f"{bundle},"
        s += f"{self.bundles_by_count[-1]}"
        return s


@dataclass(frozen=True, eq=True)
class RayTaskActorDemand(ResourceDemand):
    pass


@dataclass(frozen=True, eq=True)
class ClusterConstraintDemand(ResourceDemand):
    pass


@dataclass(frozen=True, eq=True)
class ResourceDemandSummary:
    # Placement group demand.
    placement_group_demand: List[PlacementGroupResourceDemand]
    # Ray task actor demand.
    ray_task_actor_demand: List[RayTaskActorDemand]
    # Cluster constraint demand.
    cluster_constraint_demand: List[ClusterConstraintDemand]


@dataclass(frozen=True, eq=True)
class Stats:
    # How long it took to get the GCS request.
    gcs_request_time_s: Optional[float] = None
    # How long it took to get all live instances from node provider.
    none_terminated_node_request_time_s: Optional[float] = None
    # How long for autoscaler to process the scaling decision.
    autoscaler_iteration_time_s: Optional[float] = None


@dataclass(frozen=True, eq=True)
class ClusterStatus:
    # Healthy nodes information (alive)
    healthy_nodes: List[NodeInfo]
    # Pending launches.
    pending_launches: List[PendingLaunchRequest]
    # Pending nodes.
    pending_nodes: List[NodeInfo]
    # Failures
    failed_nodes: List[NodeInfo]
    # Resource usage summary for entire cluster.
    cluster_resource_usage: List[ResourceUsage]
    # Demand summary.
    resource_demands: ResourceDemandSummary
    # Query metics
    stats: Stats
    # TODO(rickyx): Not sure if this is actually used.
    # We don't have any tests that cover this is actually
    # being produced. And I have not seen this either.
    # Node availability info.
    node_availability: Optional[NodeAvailabilitySummary]

    def to_str(self, verbose_lvl=0):
        # This could be what the `ray status` is getting.
        return "not implemented"
