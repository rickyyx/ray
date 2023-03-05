import time
from abc import ABC
from rich import print
from collections import defaultdict
from typing import Optional
import logging

from ray.experimental.state.api import list_nodes, list_tasks
from ray.hack.schema import Action, Diagnosis, DiagnosisDetail, DiagnosisLevel

from prometheus_api_client import PrometheusConnect
from prometheus_api_client.utils import parse_datetime


PROM_SERVER_ADDRESS = "http://localhost:9090"

logger =logging.getLogger(__name__)


def _is_pending_task(t):
    t_state = t.get("state", "NIL")
    return "PENDING" in t_state


def _group_tasks_by(tasks, group_by_key_fn, value_fn):
    res = defaultdict(list)
    for t in tasks:
        key = group_by_key_fn(t)
        if value_fn is not None:
            val = value_fn(t)
        else:
            val = t

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
        failed_tasks = list_tasks(detail=True, filters=[("state", "=", "FAILED")])
        if len(failed_tasks) == 0:
            return []
        
        failed_task_by_name_err = _group_tasks_by(
            failed_tasks, lambda t: (t["name"], t["error_type"])
        )
        
        failed_task_by_name_err_count = [{
            "task": name,
            "err_type": err,
            "count": len(tasks)
        } for name, err, tasks in failed_task_by_name_err.items()]

        return failed_task_by_name_err_count
    
class IdleNodes(Check):

    idle_cpu_threshold_percentage = 90
    idle_cpu_window_sec = 30
    last_active_task_threshold_sec = 5
    name = "Check idle nodes"

    def head_node_ip(self) -> str:
        import ray._private.services as services
        address = services.canonicalize_bootstrap_address(addr=None)
        assert address is not None
        head_node_ip = address.split(":")[0]
        return head_node_ip


    def to_diagnosis(self) -> Optional[Diagnosis]:
        head_node_ip = self.head_node_ip()
        print(f"head node ip: {head_node_ip}")
        # worker_nodes = list_nodes(detail=True, filters=[("node_ip", "!=", head_node_ip )])
        worker_nodes = list_nodes(detail=True)

        for node in worker_nodes:
            idle = self.check_if_idle(node["node_ip"])
            print(f"{node} is idle = {idle}")

        return Diagnosis.OK(self.name)

    def check_if_idle(self, node_ip) -> bool:
        prom = PrometheusConnect(url =PROM_SERVER_ADDRESS, disable_ssl=True)
        start_time = parse_datetime(f"{self.idle_cpu_window_sec}sec")
        end_time = parse_datetime("now")

        node_cpu_percentage_data = prom.custom_query_range(
            f"ray_node_cpu_utilization{{instance=~'{node_ip}:\\\d+'}}",
            start_time=start_time,
            end_time=end_time,
            step=15.0
        )

        # TODO: multiple node? 
        assert len(node_cpu_percentage_data) == 1 
        cpu_per = node_cpu_percentage_data[0]["values"]

        over_threshold_pts = [value for value in cpu_per if float(value[1]) > self.idle_cpu_threshold_percentage]
        if len(over_threshold_pts) != 0:
            print(over_threshold_pts)
            # Some cpu spikes 
            return False
            # return Diagnosis(
            #     name= self.name,
            #     details=DiagnosisDetail(
            #     msg=f"Not all cores idle in the past {self.idle_cpu_window_sec} secs."
            #     ),
            #     level=DiagnosisLevel.OK
            # )
        
        # Check last run task timestamp
        last_active_task = self.get_last_active_task()
        print(last_active_task)

        if last_active_task["state"] not in ("FINISHED", "FAILED"):
            return False

        assert last_active_task.get("end_time_ms", None) is not None
        last_active_ms = last_active_task["end_time_ms"]
        now_ms = time.time() * 1000
        dur_ms = now_ms - last_active_ms
        print(f"dur since last active: {dur_ms / 1000} secs")
        if dur_ms <= self.last_active_task_threshold_sec * 1000:
            # Was active 
            return False

        # Not active recently
        return True


    def get_last_active_task(self):
        tasks = list_tasks(detail=True)

        non_end_tasks = [t for t in tasks if t.get("end_time_ms", None) is None]
        if len(non_end_tasks) > 0:
            latest_created_task = max(non_end_tasks, key=lambda t: t["creation_time_ms"])
            return latest_created_task
        
        # All finished tasks
        latest_ended_task = max(tasks, key=lambda t: t["end_time_ms"])
        return latest_ended_task


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
        return Diagnosis.OK("check stuck tasks")

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
