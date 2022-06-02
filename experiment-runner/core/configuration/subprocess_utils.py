# Created by Johannes A. Ebster
import subprocess

VM_DBIS_NOSQL_HOST = "server-vm-dbis-nosql"
VM_DBIS_SCRIPTS = "~/ifistorage/scripts"
LOAD_PATH = f"source {VM_DBIS_SCRIPTS}/setup_path.sh"


def execute_script_on_remote_machine(script_name, wait=True):
    ret_val = execute_cmd_on_shell(cmd=f"ssh {VM_DBIS_NOSQL_HOST} '{LOAD_PATH}; {VM_DBIS_SCRIPTS}/{script_name}'",
                                   wait=wait)
    if wait:
        exit_code = ret_val
        if exit_code != 0:
            raise RuntimeError(f"script {script_name} returned non-zero exit-code.")
    else:
        return ret_val


def execute_cmd_on_shell(cmd, wait=True):
    p = subprocess.Popen(cmd, shell=True)
    if wait:
        return p.wait()
    return p


def execute_cmd(cmd, wait=True):
    p = subprocess.Popen(cmd, shell=False)
    if wait:
        return p.wait()
    return p
