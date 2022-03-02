import time

from .subprocess_utils import execute_cmd_on_shell


def restart_memcached(port):
    memcached_pid = f"lsof -i :{port} | grep LISTEN | cut -d ' ' -f2"
    kill_memcached = f"kill `{memcached_pid}`"
    # the memcached process from earlier run is usually already dead
    execute_cmd_on_shell(kill_memcached)
    time.sleep(3)
    start_memcached = f"memcached -p {port}"
    execute_cmd_on_shell(start_memcached, wait=False)

