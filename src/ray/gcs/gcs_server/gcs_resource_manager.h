// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <gtest/gtest_prod.h>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/common/ray_syncer/ray_syncer.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/raylet/scheduling/cluster_resource_data.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet/scheduling/cluster_task_manager.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

class GcsMonitorServerTest;

using raylet::ClusterTaskManager;
namespace gcs {
/// Ideally, the logic related to resource calculation should be moved from
/// `gcs_resoruce_manager` to `cluster_resource_manager`, and all logic related to
/// resource modification should directly depend on `cluster_resource_manager`, while
/// `gcs_resoruce_manager` is still responsible for processing resource-related RPC
/// request. We will split several small PR to achieve this goal, so as to prevent one PR
/// from being too large to review.
///
/// 1). Remove `node_resource_usages_` related code as it could be calculated from
/// `cluster_resource_manager`
/// 2). Move all resource-write-related logic out from `gcs_resource_manager`
/// 3). Move `placement_group_load_` from `gcs_resource_manager` to
/// `placement_group_manager` and make `gcs_resource_manager` depend on
/// `placement_group_manager`

/// Gcs resource manager interface.
/// It is responsible for handing node resource related rpc requests and it is used for
/// actor and placement group scheduling. It obtains the available resources of nodes
/// through heartbeat reporting. Non-thread safe.
class GcsResourceManager : public rpc::NodeResourceInfoHandler,
                           public rpc::AutoscalerStateHandler,
                           public syncer::ReceiverInterface {
 public:
  /// Create a GcsResourceManager.
  explicit GcsResourceManager(
      instrumented_io_context &io_context,
      ClusterResourceManager &cluster_resource_manager,
      NodeID local_node_id,
      std::shared_ptr<ClusterTaskManager> cluster_task_manager = nullptr);

  virtual ~GcsResourceManager() {}

  /// Handle the resource update.
  void ConsumeSyncMessage(std::shared_ptr<const syncer::RaySyncMessage> message) override;

  /// Handle get resource rpc request.
  void HandleGetResources(rpc::GetResourcesRequest request,
                          rpc::GetResourcesReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get available resources of all nodes.
  void HandleGetAllAvailableResources(
      rpc::GetAllAvailableResourcesRequest request,
      rpc::GetAllAvailableResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Handle report resource usage rpc from a raylet.
  void HandleReportResourceUsage(rpc::ReportResourceUsageRequest request,
                                 rpc::ReportResourceUsageReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get all resource usage rpc request.
  void HandleGetAllResourceUsage(rpc::GetAllResourceUsageRequest request,
                                 rpc::GetAllResourceUsageReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  /// AutoscalerStateHandler implementation
  void HandleGetClusterResourceState(
      rpc::autoscaler::GetClusterResourceStateRequest request,
      rpc::autoscaler::GetClusterResourceStateReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleReportAutoscalingState(
      rpc::autoscaler::ReportAutoscalingStateRequest request,
      rpc::autoscaler::ReportAutoscalingStateReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// AutoscalerStateHandler implementation ends.

  /// Handle a node registration.
  ///
  /// \param node The specified node to add.
  void OnNodeAdd(const rpc::GcsNodeInfo &node);

  /// Handle a node death.
  ///
  /// \param node_id The specified node id.
  void OnNodeDead(const NodeID &node_id);

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  std::string ToString() const;

  std::string DebugString() const;

  /// Add resources changed listener.
  void AddResourcesChangedListener(std::function<void()> listener);

  // Update node normal task resources.
  void UpdateNodeNormalTaskResources(const NodeID &node_id,
                                     const rpc::ResourcesData &heartbeat);

  /// Update resource usage of given node.
  ///
  /// \param node_id Node id.
  /// \param request Request containing resource usage.
  void UpdateNodeResourceUsage(const NodeID &node_id,
                               const rpc::ResourcesData &resources);

  /// Process a new resource report from a node, independent of the rpc handler it came
  /// from.
  void UpdateFromResourceReport(const rpc::ResourcesData &data);

  /// Update the placement group load information so that it will be reported through
  /// heartbeat.
  ///
  /// \param placement_group_load placement group load protobuf.
  void UpdatePlacementGroupLoad(
      const std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load);

  /// Update the resource loads.
  ///
  /// \param data The resource loads reported by raylet.
  void UpdateResourceLoads(const rpc::ResourcesData &data);

  /// Returns the mapping from node id to latest resource report.
  ///
  /// \returns The mapping from node id to latest resource report.
  const absl::flat_hash_map<NodeID, rpc::ResourcesData> &NodeResourceReportView() const;

 private:
  /// Aggregate nodes' pending task info.
  ///
  /// \param resources_data A node's pending task info (by shape).
  /// \param aggregate_load[out] The aggregate pending task info (across the cluster).
  void FillAggregateLoad(const rpc::ResourcesData &resources_data,
                         std::unordered_map<google::protobuf::Map<std::string, double>,
                                            rpc::ResourceDemand> *aggregate_load);

  /// @brief  Add a new node state.
  /// @param instance_id Instance id that's associated with the cloud provider if
  /// relevant.
  /// @param node_id Node id.
  /// @param version Current cluster state version.
  void AddNewNodeState(const std::string &instance_id,
                       const std::string &node_id,
                       int64_t version);

  /// @brief  Remove a node state.
  /// @param node_id Node id.
  void RemoveNodeState(const NodeID &node_id);

  /// @brief Get the current cluster resource state version.
  ///
  /// When the cluster state (that's currently reported to autoscaler) that represents the
  /// resource state of the cluster is updated, a monotonically increasing version number
  /// will be incremented. Current cluster state consists of :
  ///   - node membership: when node is added/removed/drained.
  ///   - cluster resources: available/total resources of the cluster.
  ///   - resource requests: e.g. placement group requests, tasks/actors resource requests
  ///     or cluster resources requested explicitly.
  ///
  /// Right now, consecutive versions are NOT guaranteed to be different. The version is
  /// used for 2 major reasons:
  ///   1. So that autoscaler can detect if the cluster state has changed since last time
  ///     and reconcile inconsistency that might arise due to network partition or cloud
  ///     providers errors.
  ///   2. So that GCS could dump cluster state with versions for backtracing and
  ///   debugging.
  int64_t GetClusterResourceStateVersion() const {
    return cluster_resource_state_version_;
  }

  /// @brief Increment the cluster resource state version.
  ///
  /// @return Return the new version.
  /// Not thread safe.
  int64_t IncrementClusterResourceStateVersion() {
    return ++cluster_resource_state_version_;
  }

  /// @brief Get the current cluster resource state.
  /// @param reply The reply to be filled.
  ///
  /// See rpc::autoscaler::GetClusterResourceStateReply::node_states for more details.
  void GetNodeStates(rpc::autoscaler::GetClusterResourceStateReply *reply);

  /// @brief Get the resource requests state.
  /// @param reply The reply to be filled.
  ///
  /// See rpc::autoscaler::GetClusterResourceStateReply::pending_resource_requests for
  /// more details.
  void GetPendingResourceRequests(rpc::autoscaler::GetClusterResourceStateReply *reply);

  /// @brief Get the gang resource requests (e.g. from placement group) state.
  /// @param reply The reply to be filled.
  ///
  /// See rpc::autoscaler::GetClusterResourceStateReply::pending_gang_resource_requests
  /// for more
  void GetPendingGangResourceRequests(
      rpc::autoscaler::GetClusterResourceStateReply *reply);

  /// @brief Get the cluster resource constraints state.
  /// @param reply The reply to be filled.
  ///
  /// See rpc::autoscaler::GetClusterResourceStateReply::cluster_resource_constraints for
  /// more details. This is requested through autoscaler SDK for request_resources().
  void GetClusterResourceConstraints(
      rpc::autoscaler::GetClusterResourceStateReply *reply);

  /// io context. This is to ensure thread safety. Ideally, all public
  /// funciton needs to post job to this io_context.
  instrumented_io_context &io_context_;

  /// Newest resource usage of all nodes.
  absl::flat_hash_map<NodeID, rpc::ResourcesData> node_resource_usages_;

  /// Placement group load information that is used for autoscaler.
  absl::optional<std::shared_ptr<rpc::PlacementGroupLoad>> placement_group_load_;
  /// The resources changed listeners.
  std::vector<std::function<void()>> resources_changed_listeners_;

  /// AutoscalerStateService only states.

  absl::flat_hash_map<NodeID, rpc::NodeState> node_states_;

  // The default value of the last seen version for the request is 0, which indicates
  // no version has been seen. When node is added, the version will be incremented.
  // Therefore, a cluster in init state with at least 1 node, will have the version > 0.
  int64_t cluster_resource_state_version_ = 0;

  /// The last seen autoscaler state version. Use 0 as the default value to indicate
  /// no previous autoscaler state has been reported.
  int64_t last_seen_autoscaler_state_version_ = 0;

  /// AutoscalerStateService only states ends.

  /// Debug info.
  enum CountType {
    GET_RESOURCES_REQUEST = 0,
    GET_ALL_AVAILABLE_RESOURCES_REQUEST = 1,
    REPORT_RESOURCE_USAGE_REQUEST = 2,
    GET_ALL_RESOURCE_USAGE_REQUEST = 3,
    CountType_MAX = 4,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};

  ClusterResourceManager &cluster_resource_manager_;
  NodeID local_node_id_;
  std::shared_ptr<ClusterTaskManager> cluster_task_manager_;
  /// Num of alive nodes in the cluster.
  size_t num_alive_nodes_ = 0;

  friend GcsMonitorServerTest;

  FRIEND_TEST(GcsResourceManagerAutoscalerStateTest, TestClusterStateVersion);
};

}  // namespace gcs
}  // namespace ray

namespace std {
template <>
struct hash<google::protobuf::Map<std::string, double>> {
  size_t operator()(google::protobuf::Map<std::string, double> const &k) const {
    size_t seed = k.size();
    for (auto &elem : k) {
      seed ^= std::hash<std::string>()(elem.first);
      seed ^= std::hash<double>()(elem.second);
    }
    return seed;
  }
};

template <>
struct equal_to<google::protobuf::Map<std::string, double>> {
  bool operator()(const google::protobuf::Map<std::string, double> &left,
                  const google::protobuf::Map<std::string, double> &right) const {
    if (left.size() != right.size()) {
      return false;
    }
    for (const auto &entry : left) {
      auto iter = right.find(entry.first);
      if (iter == right.end() || iter->second != entry.second) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace std
