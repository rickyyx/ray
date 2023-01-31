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

#include "ray/core_worker/context.h"

#include <google/protobuf/util/json_util.h>

#include "ray/common/runtime_env_common.h"

namespace ray {
namespace core {
namespace {
const rpc::JobConfig kDefaultJobConfig{};
}

/// per-thread context for core worker.
struct WorkerThreadContext {
  explicit WorkerThreadContext(const TaskID &task_id)
      : current_task_id_(task_id), task_index_(0), put_counter_(0) {
    SetCurrentTaskId(task_id, /*attempt_number=*/0);
  }

  uint64_t GetNextTaskIndex() { return ++task_index_; }

  uint64_t GetTaskIndex() { return task_index_; }

  /// Returns the next put object index. The index starts at the number of
  /// return values for the current task in order to keep the put indices from
  /// conflicting with return object indices. 1 <= idx <= NumReturns() is reserved for
  /// return objects, while idx > NumReturns is available for put objects.
  ObjectIDIndexType GetNextPutIndex() {
    // If current_task_ is nullptr, we assume that we're in the event loop thread and
    // are executing async tasks; in this case, we're using a fake, random task ID
    // for put objects, so there's no risk of creating put object IDs that conflict with
    // return object IDs (none of the latter are created). The put counter will never
    // reset and will therefore continue to increment for the lifetime of the event
    // loop thread (ResetCurrentTask and SetCurrenTask will never be called in the
    // thread), so there's no risk of conflicting put object IDs, either.
    // See https://github.com/ray-project/ray/issues/10324 for further details.
    auto num_returns = current_task_ != nullptr ? current_task_->NumReturns() : 0;
    return num_returns + ++put_counter_;
  }

  const TaskID &GetCurrentTaskID() const { return current_task_id_; }

  std::shared_ptr<const TaskSpecification> GetCurrentTask() const {
    return current_task_;
  }

  void SetCurrentTaskId(const TaskID &task_id, uint64_t attempt_number) {
    current_task_id_ = task_id;
    if (!current_task_id_.IsNil()) {
      current_internal_task_id_ = TaskID::ForExecutionAttempt(task_id, attempt_number);
    } else {
      current_internal_task_id_ = TaskID::Nil();
    }
  }

  const TaskID &GetCurrentInternalTaskId() const { return current_internal_task_id_; }

  const PlacementGroupID &GetCurrentPlacementGroupId() const {
    return current_placement_group_id_;
  }

  void SetCurrentPlacementGroupId(const PlacementGroupID &placement_group_id) {
    current_placement_group_id_ = placement_group_id;
  }

  void SetPlacementGroupCaptureChildTasks(bool placement_group_capture_child_tasks) {
    placement_group_capture_child_tasks_ = placement_group_capture_child_tasks;
  }

  bool PlacementGroupCaptureChildTasks() const {
    return placement_group_capture_child_tasks_;
  }

  void SetCurrentTask(const TaskSpecification &task_spec) {
    RAY_CHECK(task_index_ == 0);
    RAY_CHECK(put_counter_ == 0);
    SetCurrentTaskId(task_spec.TaskId(), task_spec.AttemptNumber());
    SetCurrentPlacementGroupId(task_spec.PlacementGroupBundleId().first);
    SetPlacementGroupCaptureChildTasks(task_spec.PlacementGroupCaptureChildTasks());
    current_task_ = std::make_shared<const TaskSpecification>(task_spec);
  }

  void ResetCurrentTask() {
    SetCurrentTaskId(TaskID::Nil(), /*attempt_number=*/0);
    task_index_ = 0;
    put_counter_ = 0;
  }

 private:
  /// The task ID for current task.
  TaskID current_task_id_;

  /// The internal task ID, used to seed any TaskIDs or ObjectIDs generated by
  /// the current task (via ray.put or .remote calls). As tasks can be executed
  /// multiple times, this ID represents the n-th execution of the current
  /// task.
  /// See https://github.com/ray-project/ray/issues/20713.
  TaskID current_internal_task_id_;

  /// The current task.
  std::shared_ptr<const TaskSpecification> current_task_;

  /// Number of tasks that have been submitted from current task.
  uint64_t task_index_;

  static_assert(sizeof(task_index_) == TaskID::Size() - ActorID::Size(),
                "Size of task_index_ doesn't match the unique bytes of a TaskID.");

  /// A running counter for the number of object puts carried out in the current task.
  /// Used to calculate the object index for put object ObjectIDs.
  ObjectIDIndexType put_counter_;

  static_assert(sizeof(put_counter_) == ObjectID::Size() - TaskID::Size(),
                "Size of put_counter_ doesn't match the unique bytes of an ObjectID.");

  /// Placement group id that the current task belongs to.
  /// NOTE: The top level `WorkerContext` will also have placement_group_id
  ///   which is set when actors are created. It is because we'd like to keep track
  ///   thread local placement group id for tasks, and the process placement group id for
  ///   actors.
  PlacementGroupID current_placement_group_id_;

  /// Whether or not child tasks are captured in the parent's placement group implicitly.
  bool placement_group_capture_child_tasks_ = false;
};

thread_local std::shared_ptr<WorkerThreadContext> WorkerContext::thread_context_ =
    nullptr;

WorkerContext::WorkerContext(WorkerType worker_type,
                             const WorkerID &worker_id,
                             const JobID &job_id)
    : worker_type_(worker_type),
      worker_id_(worker_id),
      current_job_id_(job_id),
      job_config_(),
      current_actor_id_(ActorID::Nil()),
      current_actor_placement_group_id_(PlacementGroupID::Nil()),
      placement_group_capture_child_tasks_(false),
      main_thread_id_(std::this_thread::get_id()),
      mutex_() {
  // For worker main thread which initializes the WorkerContext,
  // set task_id according to whether current worker is a driver.
  // (For other threads it's set to random ID via GetThreadContext).
  RAY_CHECK(WorkerType::DRIVER != worker_type_ || !job_id.IsNil())
      << "job id is required for initializing driver's exec context's task id.";
  auto task_id =
      WorkerType::DRIVER == worker_type_ ? TaskID::ForDriverTask(job_id) : TaskID::Nil();
  InitThreadContext(task_id);
  GetThreadContext().SetCurrentTaskId(task_id, /*attempt_number=*/0);
}

// WorkerContext::~WorkerContext() { thread_context_ = nullptr; }

const WorkerType WorkerContext::GetWorkerType() const { return worker_type_; }

const WorkerID &WorkerContext::GetWorkerID() const { return worker_id_; }

uint64_t WorkerContext::GetNextTaskIndex() {
  return GetThreadContext().GetNextTaskIndex();
}

uint64_t WorkerContext::GetTaskIndex() { return GetThreadContext().GetTaskIndex(); }

ObjectIDIndexType WorkerContext::GetNextPutIndex() {
  return GetThreadContext().GetNextPutIndex();
}

void WorkerContext::MaybeInitializeJobInfo(const JobID &job_id,
                                           const rpc::JobConfig &job_config) {
  absl::WriterMutexLock lock(&mutex_);
  if (current_job_id_.IsNil()) {
    current_job_id_ = job_id;
  }
  if (!job_config_.has_value()) {
    job_config_ = job_config;
  }
  RAY_CHECK(current_job_id_ == job_id);
}

int64_t WorkerContext::GetTaskDepth() const {
  absl::ReaderMutexLock lock(&mutex_);
  return task_depth_;
}

JobID WorkerContext::GetCurrentJobID() const {
  absl::ReaderMutexLock lock(&mutex_);
  return current_job_id_;
}

rpc::JobConfig WorkerContext::GetCurrentJobConfig() const {
  absl::ReaderMutexLock lock(&mutex_);
  return job_config_.has_value() ? job_config_.value() : kDefaultJobConfig;
}

const TaskID &WorkerContext::GetCurrentTaskID() const {
  return GetThreadContext().GetCurrentTaskID();
}

const TaskID &WorkerContext::GetCurrentInternalTaskId() const {
  return GetThreadContext().GetCurrentInternalTaskId();
}

const PlacementGroupID &WorkerContext::GetCurrentPlacementGroupId() const {
  absl::ReaderMutexLock lock(&mutex_);
  // If the worker is an actor, we should return the actor's placement group id.
  if (current_actor_id_ != ActorID::Nil()) {
    return current_actor_placement_group_id_;
  } else {
    return GetThreadContext().GetCurrentPlacementGroupId();
  }
}

bool WorkerContext::ShouldCaptureChildTasksInPlacementGroup() const {
  absl::ReaderMutexLock lock(&mutex_);
  // If the worker is an actor, we should return the actor's placement group id.
  if (current_actor_id_ != ActorID::Nil()) {
    return placement_group_capture_child_tasks_;
  } else {
    return GetThreadContext().PlacementGroupCaptureChildTasks();
  }
}

const std::shared_ptr<rpc::RuntimeEnvInfo> WorkerContext::GetCurrentRuntimeEnvInfo()
    const {
  absl::ReaderMutexLock lock(&mutex_);
  return runtime_env_info_;
}

const std::string &WorkerContext::GetCurrentSerializedRuntimeEnv() const {
  absl::ReaderMutexLock lock(&mutex_);
  return runtime_env_info_->serialized_runtime_env();
}

std::shared_ptr<json> WorkerContext::GetCurrentRuntimeEnv() const {
  absl::ReaderMutexLock lock(&mutex_);
  return runtime_env_;
}

void WorkerContext::SetCurrentTaskId(const TaskID &task_id, uint64_t attempt_number) {
  GetThreadContext().SetCurrentTaskId(task_id, attempt_number);
}

void WorkerContext::SetCurrentActorId(const ActorID &actor_id) LOCKS_EXCLUDED(mutex_) {
  absl::WriterMutexLock lock(&mutex_);
  if (!current_actor_id_.IsNil()) {
    RAY_CHECK(current_actor_id_ == actor_id);
    return;
  }
  current_actor_id_ = actor_id;
}

void WorkerContext::SetTaskDepth(int64_t depth) { task_depth_ = depth; }

void WorkerContext::SetCurrentTask(const TaskSpecification &task_spec) {
  InitThreadContext(task_spec.TaskId());
  {
    absl::WriterMutexLock lock(&mutex_);
    SetTaskDepth(task_spec.GetDepth());
    RAY_CHECK(current_job_id_ == task_spec.JobId());
    if (task_spec.IsNormalTask()) {
      current_task_is_direct_call_ = true;
    } else if (task_spec.IsActorCreationTask()) {
      if (!current_actor_id_.IsNil()) {
        RAY_CHECK(current_actor_id_ == task_spec.ActorCreationId());
      }
      current_actor_id_ = task_spec.ActorCreationId();
      current_actor_is_direct_call_ = true;
      current_actor_max_concurrency_ = task_spec.MaxActorConcurrency();
      current_actor_is_asyncio_ = task_spec.IsAsyncioActor();
      is_detached_actor_ = task_spec.IsDetachedActor();
      current_actor_placement_group_id_ = task_spec.PlacementGroupBundleId().first;
      placement_group_capture_child_tasks_ = task_spec.PlacementGroupCaptureChildTasks();
    } else if (task_spec.IsActorTask()) {
      RAY_CHECK(current_actor_id_ == task_spec.ActorId());
    } else {
      RAY_CHECK(false);
    }
    if (task_spec.IsNormalTask() || task_spec.IsActorCreationTask()) {
      // TODO(architkulkarni): Once workers are cached by runtime env, we should
      // only set runtime_env_ once and then RAY_CHECK that we
      // never see a new one.
      runtime_env_info_.reset(new rpc::RuntimeEnvInfo());
      *runtime_env_info_ = task_spec.RuntimeEnvInfo();
      if (!IsRuntimeEnvEmpty(runtime_env_info_->serialized_runtime_env())) {
        runtime_env_.reset(new json());
        *runtime_env_ = json::parse(runtime_env_info_->serialized_runtime_env());
      }
    }
  }
  GetThreadContext().SetCurrentTask(task_spec);
}

void WorkerContext::ResetCurrentTask() { GetThreadContext().ResetCurrentTask(); }

std::shared_ptr<const TaskSpecification> WorkerContext::GetCurrentTask() const {
  return GetThreadContext().GetCurrentTask();
}

const ActorID &WorkerContext::GetCurrentActorID() const {
  absl::ReaderMutexLock lock(&mutex_);
  return current_actor_id_;
}

bool WorkerContext::CurrentThreadIsMain() const {
  absl::ReaderMutexLock lock(&mutex_);
  return std::this_thread::get_id() == main_thread_id_;
}

bool WorkerContext::ShouldReleaseResourcesOnBlockingCalls() const {
  // Check if we need to release resources when we block:
  //  - Driver doesn't acquire resources and thus doesn't need to release.
  //  - We only support lifetime resources for direct actors, which can be
  //    acquired when the actor is created, per call resources are not supported,
  //    thus we don't need to release resources for direct actor call.
  return worker_type_ != WorkerType::DRIVER && !CurrentActorIsDirectCall() &&
         CurrentThreadIsMain();
}

// TODO(edoakes): simplify these checks now that we only support direct call mode.
bool WorkerContext::CurrentActorIsDirectCall() const {
  absl::ReaderMutexLock lock(&mutex_);
  return current_actor_is_direct_call_;
}

bool WorkerContext::CurrentTaskIsDirectCall() const {
  absl::ReaderMutexLock lock(&mutex_);
  return current_task_is_direct_call_ || current_actor_is_direct_call_;
}

int WorkerContext::CurrentActorMaxConcurrency() const {
  absl::ReaderMutexLock lock(&mutex_);
  return current_actor_max_concurrency_;
}

bool WorkerContext::CurrentActorIsAsync() const {
  absl::ReaderMutexLock lock(&mutex_);
  return current_actor_is_asyncio_;
}

bool WorkerContext::CurrentActorDetached() const {
  absl::ReaderMutexLock lock(&mutex_);
  return is_detached_actor_;
}

void WorkerContext::InitThreadContext(const TaskID &task_id) {
  absl::WriterMutexLock lock(&mutex_);
  auto thread_ctx = std::make_shared<WorkerThreadContext>(task_id);
  auto itr_inserted =
      all_exec_threads_contexts_.insert({std::this_thread::get_id(), thread_ctx});
  current_exec_context_ = itr_inserted.first->second;
}

std::shared_ptr<WorkerThreadContext> WorkerContext::GetThreadContextInternal() const {
  // For non threaded actor/async actors, Ray will only execute one task sequentially.
  if (!current_actor_is_asyncio_ && current_actor_max_concurrency_ == 1) {
    RAY_CHECK(current_exec_context_ != nullptr);
    return current_exec_context_;
  }

  const auto itr = all_exec_threads_contexts_.find(std::this_thread::get_id());
  if (itr == all_exec_threads_contexts_.end()) {
    // Ray isn't aware of this current thread, there could be 2 possible cases:
    //  1. Its a thread owned by async actor's asyncio event loop.
    //  2. Its a thread started in user level code.
    return nullptr;
  }

  RAY_CHECK(itr->second != nullptr);
  return itr->second;
}

WorkerThreadContext &WorkerContext::GetThreadContext() const {
  if (thread_context_ == nullptr) {
    absl::ReaderMutexLock lock(&mutex_);
    auto ctx = GetThreadContextInternal();
    if (ctx == nullptr) {
      // Not able to find a proxied thread context.
      RAY_CHECK(!current_job_id_.IsNil())
          << "can't access thread context when job_id is not assigned";
      thread_context_ =
          std::make_unique<WorkerThreadContext>(TaskID::FromRandom(current_job_id_));
    } else {
      // Found a proxy thread ctx.
      thread_context_ = ctx;
    }
  }
  RAY_CHECK(thread_context_ != nullptr);

  return *thread_context_;
}
}  // namespace core
}  // namespace ray
