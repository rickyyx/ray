import time
from abc import ABC
from collections import defaultdict
from typing import Optional

from ray.experimental.state.api import list_tasks
from ray.hack.schema import Action, Diagnosis, DiagnosisDetail, DiagnosisLevel


def _is_pending_task(t):
    t_state = t.get("state", "NIL")
    return "PENDING" in t_state


def _group_tasks_by(tasks, group_by_key_fn, value_fn):
    res = defaultdict(list)
    for t in tasks:
        key = group_by_key_fn(t)
        val = value_fn(t)
        if type(val) is list:
            res[key].extend(val)
        else:
            res[key].append(val)

    return res


class Check(ABC):
    def to_diagnosis(self) -> Optional[Diagnosis]:
        return None

class FailedTasks(Check):
    def to_diagnosis(self) -> Optional[Diagnosis]:
        failed_tasks = self.get_failed_tasks()

    def get_failed_tasks(self):
        faield_tasks = list_tasks(detail=True, filters=[("state", "=", "FAILED")])





class StuckTasks(Check):
    def to_diagnosis(self) -> Optional[Diagnosis]:
        stuck_tasks = self.get_stuck_tasks()
        if len(stuck_tasks) > 0:
            max_stuck = max(stuck_tasks, key=lambda x: x["duration_sec"])
            details = DiagnosisDetail(
                msg=f"{len(stuck_tasks)} pending tasks, max pending task {max_stuck['name']}() in {max_stuck['state']} for {max_stuck['duration_sec']} secs"
            )
            actions = [Action(msg="Use 'ray summary tasks' to find out stuck tasks.")]
            return Diagnosis(
                level=DiagnosisLevel.WARNING, actions=actions, details=details,
                name="Checking pending tasks"
            )

    def get_stuck_tasks(self, allowed_max_duration_ms=3 * 1000):
        """
        return {
            name: xx,
            state: xx,
            duration:sec: xx
        }
        """
        tasks = list_tasks(detail=True)
        pending_tasks = [t for t in tasks if _is_pending_task(t)]
        pending_tasks_by_name = _group_tasks_by(
            pending_tasks, lambda t: (t["name"], t["state"]), lambda t: t["events"]
        )

        # Get pending duration and states
        now_ms = time.time() * 1000
        stuck_tasks = []
        for name_and_state, task_events in pending_tasks_by_name.items():
            name, state = name_and_state
            state_ts = [e["created_ms"] for e in task_events if e["state"] == state]
            state_durations_ms = [now_ms - ts_ms for ts_ms in state_ts]
            max_duration_ms = max(state_durations_ms)

            if max_duration_ms > allowed_max_duration_ms:
                stuck_tasks.append(
                    {
                        "name": name,
                        "state": state,
                        "duration_sec": max_duration_ms / 1000,
                    }
                )

        return stuck_tasks
