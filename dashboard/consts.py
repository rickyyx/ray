from ray.ray_constants import env_integer

DASHBOARD_LOG_FILENAME = "dashboard.log"
DASHBOARD_AGENT_PORT_PREFIX = "DASHBOARD_AGENT_PORT_PREFIX:"
DASHBOARD_AGENT_LOG_FILENAME = "dashboard_agent.log"
DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_SECONDS = 2
RETRY_REDIS_CONNECTION_TIMES = 10
CONNECT_REDIS_INTERNAL_SECONDS = 2
PURGE_DATA_INTERVAL_SECONDS = 60 * 10
ORGANIZE_DATA_INTERVAL_SECONDS = 2
DASHBOARD_RPC_ADDRESS = "dashboard_rpc"
GCS_SERVER_ADDRESS = "GcsServerAddress"
# GCS check alive
GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR = env_integer(
    "GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR", 10
)
GCS_CHECK_ALIVE_INTERVAL_SECONDS = env_integer("GCS_CHECK_ALIVE_INTERVAL_SECONDS", 5)
GCS_CHECK_ALIVE_RPC_TIMEOUT = env_integer("GCS_CHECK_ALIVE_RPC_TIMEOUT", 10)
GCS_RETRY_CONNECT_INTERVAL_SECONDS = env_integer(
    "GCS_RETRY_CONNECT_INTERVAL_SECONDS", 2
)
# aiohttp_cache
AIOHTTP_CACHE_TTL_SECONDS = 2
AIOHTTP_CACHE_MAX_SIZE = 128
AIOHTTP_CACHE_DISABLE_ENVIRONMENT_KEY = "RAY_DASHBOARD_NO_CACHE"
# Named signals
SIGNAL_NODE_INFO_FETCHED = "node_info_fetched"
SIGNAL_NODE_SUMMARY_FETCHED = "node_summary_fetched"
SIGNAL_JOB_INFO_FETCHED = "job_info_fetched"
SIGNAL_WORKER_INFO_FETCHED = "worker_info_fetched"
# Default value for datacenter (the default value in protobuf)
DEFAULT_LANGUAGE = "PYTHON"
DEFAULT_JOB_ID = "ffff"
# Cache TTL for bad runtime env. After this time, delete the cache and retry to create
# runtime env if needed.
BAD_RUNTIME_ENV_CACHE_TTL_SECONDS = env_integer(
    "BAD_RUNTIME_ENV_CACHE_TTL_SECONDS", 60 * 10
)
# Default timeout for stopping routines/tasks on cancellation
DEFAULT_CANCEL_WAIT_TIMEOUT_SECONDS = 3
