import torch
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import subprocess
from dataclasses import dataclass, field
import os
import ray
from ray.air import session
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig
from torch.distributed.elastic.multiprocessing import SignalException, Std
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import logging

logger = logging.getLogger(__name__)


@dataclass
class LaunchConfig:
    """
    Creates a rendezvous config.

    Args:
        min_nodes: Minimum amount of nodes that the user function will
                        be launched on. Elastic agent ensures that the user
                        function start only when the min_nodes amount enters
                        the rendezvous.
        max_nodes: Maximum amount of nodes that the user function
                        will be launched on.
        nproc_per_node: On each node the elastic agent will launch
                            this amount of workers that will execute user
                            defined function.
        rdzv_backend: rdzv_backend to use in the rendezvous (zeus-adapter, etcd).
        rdzv_endpoint: The endpoint of the rdzv sync. storage.
        rdzv_configs: Key, value pair that specifies rendezvous specific configuration.
        rdzv_timeout: Legacy argument that specifies timeout for the rendezvous. It is going
            to be removed in future versions, see the note below. The default timeout is 900 seconds.
        run_id: The unique run id of the job (if not passed a unique one will be
                deduced from run environment - flow workflow id in flow - or auto generated).
        role: User defined role of the worker (defaults to "trainer").
        max_restarts: The maximum amount of restarts that elastic agent will conduct
                    on workers before failure.
        monitor_interval: The interval in seconds that is used by the elastic_agent
                        as a period of monitoring workers.
        start_method: The method is used by the elastic agent to start the
                    workers (spawn, fork, forkserver).
        log_dir: base log directory where log files are written. If not set,
                one is created in a tmp dir but NOT removed on exit.
        redirects: configuration to redirect stdout/stderr to log files.
                Pass a single ``Std`` enum to redirect all workers,
                or a mapping keyed by local_rank to selectively redirect.
        tee: configuration to "tee" stdout/stderr to console + log file.
        metrics_cfg: configuration to initialize metrics.
        local_addr: address of the local node if any. If not set, a lookup on the local
                machine's FQDN will be performed.
    ..note:
        `rdzv_timeout` is a legacy argument that will be removed in future.
        Set the timeout via `rdzv_configs['timeout']`

    """

    min_nodes: int
    max_nodes: int
    nproc_per_node: int
    run_id: str = ""
    role: str = "default_role"
    rdzv_endpoint: str = ""
    rdzv_backend: str = "etcd"
    rdzv_configs: Dict[str, Any] = field(default_factory=dict)
    rdzv_timeout: int = -1
    max_restarts: int = 3
    monitor_interval: float = 30
    start_method: str = "spawn"
    log_dir: Optional[str] = None
    redirects: Union[Std, Dict[int, Std]] = Std.NONE
    tee: Union[Std, Dict[int, Std]] = Std.NONE
    metrics_cfg: Dict[str, str] = field(default_factory=dict)
    local_addr: Optional[str] = None
    pwd: Optional[str] = "."

    def __post_init__(self):
        default_timeout = 900
        if self.rdzv_timeout != -1:
            self.rdzv_configs["timeout"] = self.rdzv_timeout
        elif "timeout" not in self.rdzv_configs:
            self.rdzv_configs["timeout"] = default_timeout


class elastic_launch:
    """
    Launches an torchelastic agent on the container that invoked the entrypoint.

        1. Pass the ``entrypoint`` arguments as non ``kwargs`` (e.g. no named parameters)/
           ``entrypoint`` can be a function or a command.
        2. The return value is a map of each worker's output mapped
           by their respective global rank.

    Usage

    ::

    def worker_fn(foo):
        # ...

    def main():
        # entrypoint is a function.
        outputs = elastic_launch(LaunchConfig, worker_fn)(foo)
        # return rank 0's output
        return outputs[0]

        # entrypoint is a command and ``script.py`` is the python module.
        outputs = elastic_launch(LaunchConfig, "script.py")(args)
        outputs = elastic_launch(LaunchConfig, "python")("script.py")
    """

    def __init__(
        self,
        config: LaunchConfig,
        entrypoint: Union[Callable, str, None],
    ):
        self._config = config
        self._entrypoint = entrypoint

    def get_scaling_config(self) -> ScalingConfig:
        return ScalingConfig(
            trainer_resources={"CPU": 0},
            num_workers=self._config.min_nodes * self._config.nproc_per_node,
            use_gpu=False,
            placement_strategy="SPREAD",
        )
    

        # We cannot really enforce number of workers per node with above ^
        # Below is how we spread workers with resource calculation.
        # import psutil
        # # NOTE(rickyx): Assuming homogeneous nodes here.
        # num_cpus_per_node = psutil.cpu_count(logical=False)
        # return ScalingConfig(
        #     trainer_resources={"CPU": 0},
        #     num_workers=self._config.min_nodes * self._config.nproc_per_node,
        #     resources_per_worker={"CPU": min(num_cpus_per_node // self._config.nproc_per_node, 1)},
        #     use_gpu=False,
        # )



    def __call__(self, *args):
        def train_loop_per_worker():
            if isinstance(self._entrypoint, Callable):
                self._entrypoint(*args)
            else:
                commands = [self._entrypoint] + list(args)
                subprocess.run(commands, check=True, capture_output=True)
            session.report({"msg:": "Finished training!"})

        scaling_config = self.get_scaling_config()

        logger.info(f"Scaling config: {scaling_config}")
        # initialize the ray runtime here
        ray.init(
            num_cpus=scaling_config.num_workers,
            runtime_env={"working_dir": os.path.abspath(self._config.pwd)},
        )
        logger.info(
            f"Ray runtime initialized with {scaling_config.num_workers} cpus, working dir {os.path.abspath(self._config.pwd)}"
        )

        # Define scaling and run configs
        trainer = TorchTrainer(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config=scaling_config,
        )

        result = trainer.fit()
        print(result.metrics)
