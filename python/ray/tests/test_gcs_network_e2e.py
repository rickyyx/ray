import pytest
import sys
import threading
from time import sleep

from pytest_docker_tools import container, fetch, network
from pytest_docker_tools.exceptions import ContainerFailed
from pytest_docker_tools import wrappers
from http.client import HTTPConnection


class Container(wrappers.Container):
    def ready(self):
        self._container.reload()
        if self.status == "exited":
            return True
            # raise ContainerFailed(
            #     self,
            #     f"Container {self.name} has already exited before "
            #     "we noticed it was ready",
            # )

        if self.status != "running":
            return False

        networks = self._container.attrs["NetworkSettings"]["Networks"]
        for (_, n) in networks.items():
            if not n["IPAddress"]:
                return False

        if "Ray runtime started" in super().logs():
            return True
        return False

    def client(self):
        port = self.ports["8000/tcp"][0]
        return HTTPConnection(f"localhost:{port}")


# Networks
network_zero = network(
    name="network_zero",
    driver="bridge",
    internal=True,
    options={
        "com.docker.network.bridge.name": "net0",
    },
)
network_one = network(
    name="network_one",
    driver="bridge",
    internal=True,
    options={
        "com.docker.network.bridge.name": "net1",
    },
)

network_two = network(
    name="network_two",
    driver="bridge",
    internal=True,
    options={
        "com.docker.network.bridge.name": "net2",
    },
)


@pytest.fixture
def head(header_node):
    yield header_node


driver_script = """
import ray
ray.init()

@ray.remote
def remote_fn():
    return 1

obj_ref = remote_fn.remote()

assert ray.get(obj_ref) == 1
"""

worker_script = """
import ray
"""


worker_node = container(
    image="ray_ci:v1",
    network="{network_one.name}",
    environment={"RAY_GCS_KV_GET_MAX_RETRY": "1"},
    # command="ray start --address gcs:6379".split(" "),
    command=["ls", "&&", "tail", "-f", "/dev/null"],
    ports={
        "8000/tcp": None,
    },
    detach=True,
)


@pytest.fixture
def worker(worker_node):
    yield worker_node


net_script = """
import psutil
import pprint

pprint.pprint(psutil.net_if_addrs())

"""


def node(docker_client, networks, **kwargs):
    init_network = networks[0]
    container = docker_client.containers.run(
        image="ray_ci:v1",
        network=init_network.name,
        ports={
            "8000/tcp": None,
        },
        detach=True,
        environment={"RAY_GCS_KV_GET_MAX_RETRY": "1"},
        command="tail -f /dev/null",
        **kwargs,
    )

    if len(networks) >= 1:
        for net in networks[1:]:
            net.connect(container)

        container.restart()
        container.reload()

    return container


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_connected_network_ok(docker_client, network_zero):
    gcs_node = None
    worker_node = None
    try:
        gcs_node = node(docker_client, [network_zero], name="gcs")
        worker_node = node(docker_client, [network_zero])

        # Run GCS node
        exit_code, output = gcs_node.exec_run(cmd="ray start --head")
        assert exit_code == 0, f"GCS failed to run: {output.decode()}"

        # Run Worker node
        exit_code, output = worker_node.exec_run(cmd="ray start --address gcs:6379")
        assert exit_code == 0, f"Worker failed to run: {output.decode()}"

    finally:
        if gcs_node is not None:
            gcs_node.remove(force=True)
        if worker_node is not None:
            worker_node.remove(force=True)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_disconnected_network_fail(docker_client, network_zero, network_one):
    gcs_node = None
    worker_node = None
    try:
        gcs_node = node(docker_client, [network_zero], name="gcs")
        worker_node = node(docker_client, [network_one])

        # Run GCS node
        exit_code, output = gcs_node.exec_run(cmd="ray start --head")
        assert exit_code == 0, f"GCS failed to run: {output.decode()}"

        # Run Worker node should fail to connect to GCS
        exit_code, output = worker_node.exec_run(cmd="ray start --address gcs:6379")
        assert exit_code != 0, f"Worker should error: {output.decode()}"
        assert "Unable to connect to GCS" in output.decode(), output.decode()

    finally:
        if gcs_node is not None:
            gcs_node.remove(force=True)
        if worker_node is not None:
            worker_node.remove(force=True)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_worker_multi_network_auto_select_ok(
    docker_client, network_zero, network_one, network_two
):
    gcs_node = None
    worker_node = None
    try:
        gcs_node = node(docker_client, [network_zero], name="gcs")
        worker_node = node(docker_client, [network_one, network_zero, network_two])

        # Run GCS node
        exit_code, output = gcs_node.exec_run(cmd="ray start --head")
        assert exit_code == 0, f"GCS failed to run: {output.decode()}"

        # Run Worker node should fail to connect to GCS
        exit_code, output = worker_node.exec_run(cmd="ray start --address gcs:6379")
        assert exit_code == 0, f"Worker failed to run: {output.decode()}"

    finally:
        if gcs_node is not None:
            gcs_node.remove(force=True)
        if worker_node is not None:
            worker_node.remove(force=True)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_head_multi_network_select_wrong_fail(
    docker_client, network_zero, network_one, network_two
):
    gcs_node = None
    worker_node = None
    try:
        gcs_node = node(docker_client, [network_zero, network_one], name="gcs")
        worker_node = node(docker_client, [network_one])

        # Run GCS node
        exit_code, output = gcs_node.exec_run(
            cmd=f"python -c '{net_script.format(address='127.6.3.0', port=6379)}'"
        )
        print(output.decode())

        exit_code, output = worker_node.exec_run(
            cmd=f"python -c '{net_script.format(address='127.6.3.0', port=6379)}'"
        )
        print(output.decode())

        bind_gcs_addr = gcs_node.attrs["NetworkSettings"]["Networks"]["network_zero"][
            "IPAddress"
        ]
        exit_code, output = gcs_node.exec_run(
            cmd=f"ray start --head --node-ip-address={bind_gcs_addr}"
        )
        assert exit_code == 0, f"GCS failed to run: {output.decode()}"
        print(output.decode())

        # Parse GCS address from the log output. Pretty hacky...
        import re

        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        output_plain = ansi_escape.sub("", output.decode())
        gcs_addr = re.search(
            r"Local node IP: (\d+.\d+.\d+.\d+)", output_plain
        ).groups()[0]

        assert (
            gcs_addr == bind_gcs_addr
        ), "Head node not running at the selected address"

        # Run Worker node should fail to connect to GCS
        exit_code, output = worker_node.exec_run(
            cmd=f"ray start --address {gcs_addr}:6379"
        )
        assert (
            exit_code != 0
        ), f"Worker should fail to connect to GCS: {output.decode()}"

    finally:
        if gcs_node is not None:
            gcs_node.remove(force=True)
        if worker_node is not None:
            worker_node.remove(force=True)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_head_multi_network_select_ok(
    docker_client, network_zero, network_one, network_two
):
    gcs_node = None
    worker_node = None
    try:
        gcs_node = node(
            docker_client, [network_zero, network_one, network_two], name="gcs"
        )
        worker_node = node(docker_client, [network_zero])

        # Run GCS node
        # exit_code, output = gcs_node.exec_run(
        #     cmd=f"python -c '{net_script.format(address='127.6.3.0', port=6379)}'"
        # )

        # exit_code, output = worker_node.exec_run(
        #     cmd=f"python -c '{net_script.format(address='127.6.3.0', port=6379)}'"
        # )
        bind_gcs_addr = gcs_node.attrs["NetworkSettings"]["Networks"]["network_zero"][
            "IPAddress"
        ]
        exit_code, output = gcs_node.exec_run(
            cmd=f"ray start --head --node-ip-address={bind_gcs_addr}"
        )
        assert exit_code == 0, f"GCS failed to run: {output.decode()}"

        # Parse GCS address from the log output. Pretty hacky...
        import re

        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        output_plain = ansi_escape.sub("", output.decode())
        gcs_addr = re.search(
            r"Local node IP: (\d+.\d+.\d+.\d+)", output_plain
        ).groups()[0]

        assert (
            gcs_addr == bind_gcs_addr
        ), "Head node not running at the selected address"

        # Running worker node should fail to connect to GCS
        exit_code, output = worker_node.exec_run(
            cmd=f"ray start --address {gcs_addr}:6379"
        )
        assert (
            exit_code == 0
        ), f"Worker should not fail to connect to GCS: {output.decode()}"

    finally:
        if gcs_node is not None:
            gcs_node.remove(force=True)
        if worker_node is not None:
            worker_node.remove(force=True)


@pytest.mark.skip()
def test_multiple_network_ok(docker_client, network_one, network_zero):
    c = None
    try:
        c = node(
            docker_client,
            [network_one, network_zero],
        )
        exit_code, output = c.exec_run(
            cmd=f"python -c '{net_script.format(address='127.6.3.0', port=6379)}'"
        )
        # exit_code, output = c.wait(timeout=10)
        assert exit_code == 1111, output.decode()
    finally:
        if c is not None:
            # Clean up
            c.remove(force=True)
            # c.wait(timeout=10)


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
