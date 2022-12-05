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

#include "ray/core_worker/task_event_buffer.h"

namespace ray {
namespace core {

namespace worker {

TaskEventBufferImpl::TaskEventBufferImpl(std::unique_ptr<gcs::GcsClient> gcs_client)
    : work_guard_(boost::asio::make_work_guard(io_service_)),
      periodical_runner_(io_service_),
      gcs_client_(std::move(gcs_client)) {}

Status TaskEventBufferImpl::Start(bool auto_flush) {
  absl::MutexLock lock(&mutex_);
  auto report_interval_ms = RayConfig::instance().task_events_report_interval_ms();
  RAY_CHECK(report_interval_ms > 0)
      << "RAY_task_events_report_interval_ms should be > 0 to use TaskEventBuffer.";

  buffer_.reserve(RayConfig::instance().task_events_max_num_task_events_in_buffer());

  // Reporting to GCS, set up gcs client and and events flushing.
  auto status = gcs_client_->Connect(io_service_);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to connect to GCS, TaskEventBuffer will stop now. [status="
                   << status.ToString() << "].";

    return status;
  }

  io_thread_ = std::thread([this]() {
#ifndef _WIN32
    // Block SIGINT and SIGTERM so they will be handled by the main thread.
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);
#endif
    SetThreadName("task_event_buffer.io");
    io_service_.run();
    RAY_LOG(INFO) << "Task event buffer io service stopped.";
  });

  if (!auto_flush) {
    return Status::OK();
  }

  RAY_LOG(INFO) << "Reporting task events to GCS every " << report_interval_ms << "ms.";
  periodical_runner_.RunFnPeriodically([this] { FlushEvents(/* forced */ false); },
                                       report_interval_ms,
                                       "CoreWorker.deadline_timer.flush_task_events");
  return Status::OK();
}

void TaskEventBufferImpl::Stop() {
  RAY_LOG(INFO) << "Shutting down TaskEventBuffer.";

  // Shutting down the io service to exit the io_thread. This should prevent
  // any other callbacks to be run on the io thread.
  io_service_.stop();
  if (io_thread_.joinable()) {
    RAY_LOG(DEBUG) << "Joining io thread from TaskEventBuffer";
    io_thread_.join();
  }

  {
    absl::MutexLock lock(&mutex_);
    // It's now safe to disconnect the GCS client since it will not be used by any
    // callbacks.
    gcs_client_->Disconnect();
  }
}

void TaskEventBufferImpl::AddTaskEvent(rpc::TaskEvents task_events) {
  absl::MutexLock lock(&mutex_);

  auto limit = RayConfig::instance().task_events_max_num_task_events_in_buffer();
  if (limit > 0 && buffer_.size() >= static_cast<size_t>(limit)) {
    // Too many task events, start overriding older ones.
    buffer_[next_idx_to_overwrite_] = std::move(task_events);
    next_idx_to_overwrite_ = (next_idx_to_overwrite_ + 1) % limit;
    num_task_events_dropped_++;
    return;
  }
  buffer_.push_back(std::move(task_events));
}

void TaskEventBufferImpl::FlushEvents(bool forced) {
  std::vector<rpc::TaskEvents> task_events;
  size_t num_task_events_dropped = 0;
  {
    absl::MutexLock lock(&mutex_);

    RAY_LOG_EVERY_MS(INFO, 15000)
        << "Pushed task state events to GCS. [total_bytes="
        << (1.0 * total_events_bytes_) / 1024 / 1024
        << "MiB][total_count=" << total_num_events_
        << "][total_task_events_dropped=" << num_task_events_dropped_
        << "][cur_buffer_size=" << buffer_.size() << "].";

    // Skip if GCS hasn't finished processing the previous message.
    if (grpc_in_progress_ && !forced) {
      RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
          << "GCS hasn't replied to the previous flush events call (likely overloaded). "
             "Skipping reporting task state events and retry later."
          << "[cur_buffer_size=" << buffer_.size() << "].";
      return;
    }

    if (buffer_.size() == 0) {
      return;
    }

    task_events.reserve(
        RayConfig::instance().task_events_max_num_task_events_in_buffer());
    buffer_.swap(task_events);
    next_idx_to_overwrite_ = 0;
    num_task_events_dropped = num_task_events_dropped_;
    num_task_events_dropped_ = 0;
  }

  // Merge multiple events from a single task attempt run into one task event.
  absl::flat_hash_map<std::pair<std::string, int>, rpc::TaskEvents> task_events_map;
  for (auto event : task_events) {
    auto &task_events_itr =
        task_events_map[std::make_pair(event.task_id(), event.attempt_number())];
    task_events_itr.MergeFrom(event);
  }

  // Convert to rpc::TaskEventsData
  auto data = std::make_unique<rpc::TaskEventData>();
  data->set_num_task_events_dropped(num_task_events_dropped);
  auto num_task_events = task_events_map.size();
  for (auto itr : task_events_map) {
    auto events_by_task = data->add_events_by_task();
    events_by_task->Swap(&itr.second);
  }

  {
    // Sending the protobuf to GCS.
    absl::MutexLock lock(&mutex_);
    // Some debug tracking.
    total_num_events_ += num_task_events;
    total_events_bytes_ += data->ByteSizeLong();

    auto on_complete = [this, num_task_events](const Status &status) {
      absl::MutexLock lock(&mutex_);
      if (!status.ok()) {
        RAY_LOG(WARNING) << "Failed to push " << num_task_events
                         << " task state events to GCS. Data will be lost. [status="
                         << status.ToString() << "]";
      } else {
        RAY_LOG(DEBUG) << "Push " << num_task_events << " task state events to GCS.";
      }
      grpc_in_progress_ = false;
    };

    // The flag should be unset when on_complete is invoked.
    grpc_in_progress_ = true;
    auto status =
        gcs_client_->Tasks().AsyncAddTaskEventData(std::move(data), on_complete);
    if (!status.ok()) {
      // If we couldn't even send the data by invoking client side callbacks, there's
      // something seriously wrong, and losing data in this case should not be too
      // worse. So we will silently drop these task events.
      RAY_LOG(WARNING)
          << "Failed to push task state events to GCS. Data will be lost. [status="
          << status.ToString() << "]";
      grpc_in_progress_ = false;

      // Fail to send, currently dropping events.
      num_task_events_dropped_ += num_task_events;
    }
  }
}

}  // namespace worker

}  // namespace core
}  // namespace ray
