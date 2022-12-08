// Copyright 2022 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_task_manager.h"

#include <atomic>  // std::atomic

#include "ray/common/ray_config.h"
#include "ray/common/status.h"

namespace ray {
namespace gcs {

void GcsTaskManager::Stop() {
  io_service_.stop();
  if (io_service_thread_->joinable()) {
    io_service_thread_->join();
  }
}

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents(
    absl::optional<JobID> job_id) {
  std::vector<rpc::TaskEvents> results;
  if (job_id) {
    // Get all task events with job id
    for (const auto &task_event : task_events_) {
      if (task_event.has_task_info() &&
          JobID::FromBinary(task_event.task_info().job_id()) == *job_id) {
        results.push_back(task_event);
      }
    }
    return results;
  }

  // Return all task events
  return task_events_;
}

absl::optional<rpc::TaskEvents>
GcsTaskManager::GcsTaskManagerStorage::AddOrReplaceTaskEvent(
    rpc::TaskEvents events_by_task) {
  RAY_LOG_EVERY_MS(INFO, 10000)
      << "GcsTaskManagerStorage currently stores " << task_events_.size()
      << " task event entries, approximate size="
      << 1.0 * num_bytes_task_events_ / 1024 / 1024 << "MiB";

  TaskID task_id = TaskID::FromBinary(events_by_task.task_id());
  int32_t attempt_number = events_by_task.attempt_number();
  TaskAttempt task_attempt = std::make_pair<>(task_id, attempt_number);

  // GCS perform merging of events/updates for a single task attempt from multiple
  // reports.
  auto itr = task_attempt_index_.find(task_attempt);
  if (itr != task_attempt_index_.end()) {
    // Existing task attempt entry, merge.
    auto idx = itr->second;
    auto &existing_events = task_events_.at(idx);

    // Update the events.
    num_bytes_task_events_ -= existing_events.ByteSizeLong();
    existing_events.MergeFrom(events_by_task);
    num_bytes_task_events_ += existing_events.ByteSizeLong();

    return absl::nullopt;
  }

  // A new task event, add to storage and index.

  // If limit enforced, replace one.
  if (max_num_task_events_ > 0 && task_events_.size() >= max_num_task_events_) {
    RAY_LOG_EVERY_MS(WARNING, 10000)
        << "Max number of tasks event (" << max_num_task_events_
        << ") allowed is reached. Old task events will be overwritten. Set "
           "`RAY_task_events_max_num_task_in_gcs` to a higher value to "
           "store more.";

    num_bytes_task_events_ -= task_events_[next_idx_to_overwrite_].ByteSizeLong();
    num_bytes_task_events_ += events_by_task.ByteSizeLong();

    // Change the underlying storage.
    auto &to_replaced = task_events_.at(next_idx_to_overwrite_);
    std::swap(to_replaced, events_by_task);
    auto replaced = events_by_task;

    // Update index.
    TaskAttempt replaced_attempt = std::make_pair<>(
        TaskID::FromBinary(replaced.task_id()), replaced.attempt_number());
    task_attempt_index_.erase(replaced_attempt);
    task_attempt_index_[task_attempt] = next_idx_to_overwrite_;

    // Update iter.
    next_idx_to_overwrite_ = (next_idx_to_overwrite_ + 1) % max_num_task_events_;

    return replaced;
  }

  // Add to index.
  task_attempt_index_[task_attempt] = task_events_.size();
  // Add a new task events.
  task_events_.push_back(std::move(events_by_task));
  return absl::nullopt;
}

void GcsTaskManager::HandleGetTaskEvents(rpc::GetTaskEventsRequest request,
                                         rpc::GetTaskEventsReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting task status:" << request.ShortDebugString();
  io_service_.post(
      [this, reply, send_reply_callback, request = std::move(request)]() {
        // Select candidate events by indexing.
        std::vector<rpc::TaskEvents> task_events;
        if (request.has_job_id()) {
          task_events =
              task_event_storage_->GetTaskEvents(JobID::FromBinary(request.job_id()));
        } else {
          task_events = task_event_storage_->GetTaskEvents();
        }

        // Filter query specific event fields
        if (!request.has_event_type()) {
          for (auto &task_event : task_events) {
            auto events = reply->add_events_by_task();
            events->Swap(&task_event);
          }
          reply->set_num_events_dropped(total_num_status_task_events_dropped_ +
                                        total_num_profile_task_events_dropped_);
        } else if (request.event_type() == rpc::TaskEventType::PROFILE_EVENT) {
          for (auto &task_event : task_events) {
            if (!task_event.has_profile_events()) {
              continue;
            }
            AddProfileEvent(reply, task_event);
          }
          reply->set_num_events_dropped(total_num_profile_task_events_dropped_);
        } else if (request.event_type() == rpc::TaskEventType::STATUS_EVENT) {
          for (auto &task_event : task_events) {
            if (!task_event.has_state_updates()) {
              continue;
            }
            AddStatusUpdateEvent(reply, task_event);
          }
          reply->set_num_events_dropped(total_num_status_task_events_dropped_);
        } else {
          UNREACHABLE;
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      },
      "GcsTaskManager::HandleGetTaskEvents");
  return;
}

void GcsTaskManager::HandleAddTaskEventData(rpc::AddTaskEventDataRequest request,
                                            rpc::AddTaskEventDataReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Adding task state event:" << request.data().ShortDebugString();
  // Dispatch to the handler
  io_service_.post(
      [this, reply, send_reply_callback, request = std::move(request)]() {
        auto data = request.data();
        size_t num_to_process = data.events_by_task_size();
        // Update counters.
        total_num_profile_task_events_dropped_ += data.num_profile_task_events_dropped();
        total_num_status_task_events_dropped_ += data.num_status_task_events_dropped();

        for (auto &events_by_task : *data.mutable_events_by_task()) {
          total_num_task_events_reported_++;
          auto task_id = TaskID::FromBinary(events_by_task.task_id());
          // TODO(rickyx): add logic to handle too many profile events for a single task
          // attempt.
          auto replaced_task_events =
              task_event_storage_->AddOrReplaceTaskEvent(std::move(events_by_task));

          if (replaced_task_events) {
            if (replaced_task_events->has_state_updates()) {
              // TODO(rickyx): should we un-flatten the status updates into a list of
              // StatusEvents? so that we could get an accurate number of status change
              // events being dropped like profile events.
              total_num_status_task_events_dropped_++;
            }
            if (replaced_task_events->has_profile_events()) {
              total_num_profile_task_events_dropped_ +=
                  replaced_task_events->profile_events().events_size();
            }
          }
          RAY_LOG(DEBUG) << "Processed a task event. [task_id=" << task_id.Hex() << "]";
        }
        RAY_LOG_EVERY_MS(INFO, 10000)
            << "GcsTaskManager currently has [total_num_task_events_reported="
            << total_num_task_events_reported_
            << "],[total_num_status_task_events_dropped="
            << total_num_status_task_events_dropped_
            << "],[total_num_profile_task_events_dropped="
            << total_num_profile_task_events_dropped_ << "].";

        // Processed all the task events
        RAY_LOG(DEBUG) << "Processed all " << num_to_process << " task events";
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      },
      "GcsTaskManager::HandleAddTaskEventData");
}

void GcsTaskManager::AddProfileEvent(rpc::GetTaskEventsReply *reply,
                                     rpc::TaskEvents &task_event) {
  auto profile_event = reply->add_events_by_task();
  profile_event->set_task_id(task_event.task_id());
  profile_event->set_attempt_number(task_event.attempt_number());
  profile_event->mutable_profile_events()->Swap(task_event.mutable_profile_events());
}

void GcsTaskManager::AddStatusUpdateEvent(rpc::GetTaskEventsReply *reply,
                                          rpc::TaskEvents &task_event) {
  auto status_event = reply->add_events_by_task();
  status_event->set_task_id(task_event.task_id());
  status_event->set_attempt_number(task_event.attempt_number());
  status_event->mutable_task_info()->Swap(task_event.mutable_task_info());
  status_event->mutable_state_updates()->Swap(task_event.mutable_state_updates());
}

}  // namespace gcs
}  // namespace ray