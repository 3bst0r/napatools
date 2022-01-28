import subprocess

VM_DBIS_NOSQL_HOST = "vm-dbis-nosql"
VM_DBIS_SCRIPTS = "~/ifistorage/scripts"


def execute_script_on_remote_machine(script_name, wait=True):
    ret_val = execute_subprocess(cmd=f"ssh {VM_DBIS_NOSQL_HOST} '{VM_DBIS_SCRIPTS}/{script_name}'", wait=wait)
    if wait:
        exit_code = ret_val
        if exit_code != 0:
            raise RuntimeError(f"script {script_name} returned non-zero exit-code.")
    else:
        return ret_val


def execute_subprocess(cmd, wait=True):
    p = subprocess.Popen(cmd, shell=True)
    if wait:
        return p.wait()
    return p
