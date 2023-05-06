"""Test that runtime_env raises good errors if ray[default] is not installed.


In CI, this file is run with a minimal ray installation (`pip install ray`.)

To run this test file locally, remove some dependency that appears in
ray[default] but not in ray (e.g., `pip uninstall aiohttp`) and set
`export RAY_MINIMAL=1`.

"""

import os
import sys
import pytest

import ray
from ray.exceptions import RuntimeEnvSetupError


def _test_task_and_actor():
    @ray.remote
    def f():
        return 1

    with pytest.raises(RuntimeEnvSetupError, match="install virtualenv"):
        ray.get(f.options(runtime_env={"pip": ["requests"]}).remote())

    @ray.remote
    class A:
        def task(self):
            return 1

    a = A.options(runtime_env={"pip": ["requests"]}).remote()

    with pytest.raises(RuntimeEnvSetupError, match="install virtualenv"):
        ray.get(a.task.remote())


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows."
)
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") != "1",
    reason="This test is only run in CI with a minimal Ray installation.",
)
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25553 --port 0"],
    indirect=True,
)
def test_ray_client_task_actor(call_ray_start):
    ray.init("ray://localhost:25553")
    _test_task_and_actor()


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows."
)
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") != "1",
    reason="This test is only run in CI with a minimal Ray installation.",
)
def test_task_actor(shutdown_only):
    ray.init()
    _test_task_and_actor()


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows."
)
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") != "1",
    reason="This test is only run in CI with a minimal Ray installation.",
)
def test_ray_init(shutdown_only):
    ray.init(runtime_env={"pip": ["requests"]})

    @ray.remote
    def f():
        return 1

    with pytest.raises(RuntimeEnvSetupError, match="install virtualenv"):
        ray.get(f.remote())


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows."
)
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") != "1",
    reason="This test is only run in CI with a minimal Ray installation.",
)
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25552 --port 0"],
    indirect=True,
)
def test_ray_client_init(call_ray_start):
    with pytest.raises(ConnectionAbortedError) as excinfo:
        ray.init("ray://localhost:25552", runtime_env={"pip": ["requests"]})
    assert "install virtualenv" in str(excinfo.value)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
