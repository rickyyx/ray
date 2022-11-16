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

#pragma once

#include <memory>  // std::shared_ptr
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"               // absl::Mutex
#include "absl/types/optional.h"                      // std::unique_ptr
#include "ray/common/asio/instrumented_io_context.h"  // instrumented_io_context
#include "ray/common/asio/periodical_runner.h"        // PeriodicRunner
#include "ray/common/id.h"                            // TaskID
#include "ray/common/task/task_spec.h"                // TaskSpecification
#include "ray/gcs/gcs_client/gcs_client.h"            // Gcs::GcsClient
#include "src/ray/protobuf/gcs.pb.h"                  // rpc::TaskEventData

namespace ray {
namespace core {

namespace worker {

/// An in-memory buffer for storing task state events, and flushing them periodically to
/// GCS. Task state events will be recorded by other core components, i.e. core worker
/// and raylet.
/// If any of the gRPC call failed, the task events will be silently dropped. This
/// is probably fine since this usually indicated a much worse issue.
/// If GCS failed to respond quickly enough on the next flush, no gRPC will be made and
/// reporting of events to GCS will be delayed until GCS replies the gRPC.
///
/// TODO(rickyx): The buffer could currently grow unbounded in memory if GCS is
/// overloaded/unavailable.
///
///
/// This class is thread-safe.
class TaskEventBuffer {
 public:
  /// Constructor
  ///
  /// \param io_service IO service to run the periodic flushing routines.
  /// \param gcs_client GCS client
  TaskEventBuffer(instrumented_io_context &io_service,
                  const std::shared_ptr<gcs::GcsClient> &gcs_client);

  /// Add a task event with optional task metadata info.
  ///
  /// \param task_id Task ID of the task.
  /// \param task_info Immutable TaskInfoEntry of metadata for the task.
  /// \param task_status Current task status to be recorded.
  void AddTaskStatusEvent(TaskID task_id,
                          rpc::TaskStatus task_status,
                          std::unique_ptr<rpc::TaskInfoEntry> task_info,
                          std::unique_ptr<rpc::TaskStateEntry> task_state_update)
      LOCKS_EXCLUDED(mutex_);

  void AddProfileEvent(TaskID task_id,
                       rpc::ProfileEventEntry event,
                       const std::string &component_type,
                       const std::string &component_id,
                       const std::string &node_ip_address) LOCKS_EXCLUDED(mutex_);

  /// Flush all of the events that have been added since last flush to the GCS.
  /// If previous flush's gRPC hasn't been replied and `forced` is false, the flush will
  /// be skipped until the next invocation.
  ///
  /// \param forced True if it should be flushed regardless of previous gRPC event's
  /// state.
  void FlushEvents(bool forced) LOCKS_EXCLUDED(mutex_);

 private:
  /// Mutex guarding task_events_map_.
  absl::Mutex mutex_;

  /// ASIO IO service event loop. Must be started by the caller.
  instrumented_io_context &io_service_;

  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;

  /// Client to the GCS used to push profile events to it.
  std::shared_ptr<gcs::GcsClient> gcs_client_;

  /// Current task event data to be pushed to GCS.
  rpc::TaskEventData task_events_data_ GUARDED_BY(mutex_);

  /// Flag to toggle event recording on/off.
  bool recording_on_ = false;

  /// True if there's a pending gRPC call. It's a simple way to prevent overloading
  /// GCS with too many calls. There is no point sending more events if GCS could not
  /// process them quick enough.
  /// TODO(rickyx): When there are so many workers, we might even want to proxy those to
  /// the agent/raylet to further prevent overloading GCS.
  std::atomic<bool> grpc_in_progress_ = false;

  /// Stats tracking for debugging and monitoring.
  size_t total_events_bytes_ = 0;
  size_t total_num_events_ = 0;
};

}  // namespace worker

}  // namespace core
}  // namespace ray