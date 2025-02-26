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

#include "ray/gcs/gcs_server/gcs_heartbeat_manager.h"

#include "ray/common/constants.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/pb_util.h"

namespace ray {
namespace gcs {

GcsHeartbeatManager::GcsHeartbeatManager(
    instrumented_io_context &io_service,
    std::function<void(const NodeID &)> on_node_death_callback)
    : io_service_(io_service),
      on_node_death_callback_(std::move(on_node_death_callback)),
      num_heartbeats_timeout_(RayConfig::instance().num_heartbeats_timeout()),
      gcs_failover_worker_reconnect_timeout_(
          RayConfig::instance().gcs_failover_worker_reconnect_timeout()),
      periodical_runner_(io_service) {
  RAY_LOG(INFO) << "GcsHeartbeatManager start, num_heartbeats_timeout="
                << num_heartbeats_timeout_ << ", initial_num_heartbeats_timeout="
                << gcs_failover_worker_reconnect_timeout_;

  io_service_thread_.reset(new std::thread([this] {
    SetThreadName("heartbeat");
    /// The asio work to keep io_service_ alive.
    boost::asio::io_service::work io_service_work_(io_service_);
    io_service_.run();
  }));
}

void GcsHeartbeatManager::Initialize(const GcsInitData &gcs_init_data) {
  for (const auto &item : gcs_init_data.Nodes()) {
    if (item.second.state() == rpc::GcsNodeInfo::ALIVE) {
      AddNodeInternal(item.second, gcs_failover_worker_reconnect_timeout_);
    }
  }
}

void GcsHeartbeatManager::Start() {
  io_service_.post(
      [this] {
        if (!is_started_) {
          periodical_runner_.RunFnPeriodically(
              [this] { DetectDeadNodes(); },
              RayConfig::instance().raylet_heartbeat_period_milliseconds(),
              "GcsHeartbeatManager.deadline_timer.detect_dead_nodes");
          is_started_ = true;
        }
      },
      "GcsHeartbeatManager.Start");
}

void GcsHeartbeatManager::Stop() {
  io_service_.stop();
  if (io_service_thread_->joinable()) {
    io_service_thread_->join();
  }
}

void GcsHeartbeatManager::RemoveNode(const NodeID &node_id) {
  io_service_.dispatch(
      [this, node_id] {
        node_map_.left.erase(node_id);
        heartbeats_.erase(node_id);
      },
      "GcsHeartbeatManager::RemoveNode");
}

void GcsHeartbeatManager::AddNode(const rpc::GcsNodeInfo &node_info) {
  AddNodeInternal(node_info, num_heartbeats_timeout_);
}

void GcsHeartbeatManager::AddNodeInternal(const rpc::GcsNodeInfo &node_info,
                                          int64_t heartbeats_counts) {
  auto node_id = NodeID::FromBinary(node_info.node_id());
  auto node_addr = node_info.node_manager_address() + ":" +
                   std::to_string(node_info.node_manager_port());
  io_service_.post(
      [this, node_id, node_addr, heartbeats_counts] {
        node_map_.insert(NodeIDAddrBiMap::value_type(node_id, node_addr));
        heartbeats_.emplace(node_id, heartbeats_counts);
      },
      "GcsHeartbeatManager.AddNode");
}

void GcsHeartbeatManager::HandleReportHeartbeat(
    rpc::ReportHeartbeatRequest request,
    rpc::ReportHeartbeatReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.heartbeat().node_id());
  auto iter = heartbeats_.find(node_id);
  if (iter == heartbeats_.end()) {
    // Reply the raylet with an error so the raylet can crash itself.
    GCS_RPC_SEND_REPLY(
        send_reply_callback, reply, Status::Disconnected("Node has been dead"));
    return;
  }

  iter->second = num_heartbeats_timeout_;
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsHeartbeatManager::HandleCheckAlive(rpc::CheckAliveRequest request,
                                           rpc::CheckAliveReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  reply->set_ray_version(kRayVersion);
  for (const auto &addr : request.raylet_address()) {
    reply->mutable_raylet_alive()->Add(node_map_.right.count(addr) != 0);
  }

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsHeartbeatManager::DetectDeadNodes() {
  std::vector<NodeID> dead_nodes;
  for (auto &current : heartbeats_) {
    current.second = current.second - 1;
    if (current.second == 0) {
      RAY_LOG(WARNING) << "Node timed out: " << current.first;
      dead_nodes.push_back(current.first);
    }
  }
  for (const auto &node_id : dead_nodes) {
    RemoveNode(node_id);
    if (on_node_death_callback_) {
      on_node_death_callback_(node_id);
    }
  }
}

}  // namespace gcs
}  // namespace ray
