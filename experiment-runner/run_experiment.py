# Created by Johannes A. Ebster
import sys
import time

from yaml import load
from core.configuration.YCSBCommandBuilder import YCSBCommandBuilder
from core.configuration.config_parameters import *
from core.configuration.IndexesManager import IndexesManager
import core.configuration.subprocess_utils as subprocess_utils
import core.configuration.db_restore_scripts as db_restore_scripts
import core.configuration.memcached_utils as memcached
import argparse
import os
import subprocess

YCSB_PATH = 'YCSB_PATH'

MONITOR_CPU_SCRIPT = "monitor_cpu.sh"

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


def reraise(e, *args):
    e.args = args + e.args
    raise e.with_traceback(e.__traceback__)


def main():
    args = parse_args()

    # save/load files from/to supplied directory
    os.chdir(args.current_working_dir)

    default_cfg = load_default_cfg(args.default_config)
    experiment_spec = load_experiment_spec()

    # save all output files to out/ directory within current_working_dir
    out_dir_path = os.path.join(args.current_working_dir, 'out')
    os.makedirs(out_dir_path, exist_ok=True)
    os.chdir(out_dir_path)

    run_experiment(default_cfg=default_cfg, experiment_spec=experiment_spec)


def parse_args():
    parser = argparse.ArgumentParser(description='Execute a YCSB-JSON experiment.')
    parser.add_argument('--cwd', dest="current_working_dir",
                        help="current working directory, to read spec file and put results",
                        required=False, default=os.getcwd())
    parser.add_argument('--default_config', dest="default_config",
                        help="yaml file that specifies default config properties",
                        required=False, default=os.path.join(os.getcwd(), "core/configuration", "defaults.yml"))
    args = parser.parse_args()
    return args


def load_default_cfg(default_cfg_filename):
    return load_yaml(default_cfg_filename)


def load_experiment_spec():
    return load_yaml('experiment-spec.yml')


def load_yaml(filename):
    with open(filename, "r") as yamlfile:
        return load(yamlfile, Loader=Loader)


def clean_up(default_cfg, experiment_spec):
    db = experiment_spec[DB]
    port = default_cfg[DB_SPECIFIC_PROPS][db][SOE_STORAGE_PORT]
    memcached.kill_memcached(port)


def run_experiment(default_cfg, experiment_spec):
    validate_spec(experiment_spec)

    print("stopping all db services on remote machine...")
    stop_all_db_services_on_remote_machine()
    print("stopped all db services on remote machine.")

    db = experiment_spec[DB]
    print(f"starting {db} on remote machine...")
    start_db_on_remote_machine(db)
    print(f"started {db} on remote machine.")

    wait_seconds = 10
    print(f"waiting for {wait_seconds} seconds...")
    time.sleep(wait_seconds)
    print(f"finished waiting.")

    if experiment_spec[RESTORE_DB_BEFORE_RUN]:
        restore_before_run(default_cfg, experiment_spec)

    if experiment_spec[LOAD_BEFORE_RUN]:
        load_before_run(default_cfg, experiment_spec)

    # input("Press Enter to continue...")

    print("setting up indexes for experiment...")
    setup_indexes_for_experiment(experiment_spec)
    print("finished setting up indexes for experiment.")

    """ turned off because it will run forever
    print("starting cpu monitoring...")
    start_cpu_monitoring_on_remote_server()
    print("started cpu monitoring on remote server.")
    """

    print("running ycsb...")
    ycsb_run(default_cfg=default_cfg, experiment_spec=experiment_spec)
    print("ycsb run finished")

    print("cleaning up...")
    clean_up(default_cfg=default_cfg, experiment_spec=experiment_spec)
    print("finished cleaning up")


def validate_spec(experiment_spec):
    try:
        if OPERATION in experiment_spec and WORKLOAD in experiment_spec:
            raise ValueError(f"Both {OPERATION} and {WORKLOAD} are present in spec")
        if OPERATION not in experiment_spec and WORKLOAD not in experiment_spec:
            raise ValueError(f"Either {OPERATION} or {WORKLOAD} must be present in spec")
        if DB not in experiment_spec:
            raise ValueError(f"{DB} must be present in spec")
        if EXPERIMENT_NAME not in experiment_spec:
            raise ValueError(f"{EXPERIMENT_NAME} must be present in spec")
        if RESTORE_DB_BEFORE_RUN not in experiment_spec:
            raise ValueError(f"{RESTORE_DB_BEFORE_RUN} must be present in spec")
        if LOAD_BEFORE_RUN not in experiment_spec:
            raise ValueError(f"{LOAD_BEFORE_RUN} must be present in spec")
        if COMMAND in experiment_spec:
            raise ValueError(f"{COMMAND} can no longer be specified in spec")
    except ValueError as e:
        reraise(e, 'experiment_spec validation failed')


def stop_all_db_services_on_remote_machine():
    subprocess_utils.execute_script_on_remote_machine("stop_all_db_services")


def start_db_on_remote_machine(db):
    subprocess_utils.execute_script_on_remote_machine(f"start_{db}_db_service")


def load_before_run(default_cfg, experiment_spec):
    db = experiment_spec[DB]
    port = default_cfg[DB_SPECIFIC_PROPS][db][SOE_STORAGE_PORT]
    print(f"Restarting memcached on port {port}...")
    memcached.restart_memcached(port)
    print("Successfully restarted memcached.")
    print("Executing YCSB load phase...")
    ycsb_load(default_cfg, experiment_spec)
    print("YCSB load phase finished.")


def restore_before_run(default_cfg, experiment_spec):
    db = experiment_spec[DB]
    print(f"Restoring {db} db...")
    db_restore_scripts.restore_db(db)
    print(f"Finished restoring {db} db.")


def setup_indexes_for_experiment(experiment_spec):
    indexes_manager = IndexesManager(experiment_spec)
    indexes_manager.setup_indexes()


def start_cpu_monitoring_on_remote_server():
    return subprocess_utils.execute_script_on_remote_machine(MONITOR_CPU_SCRIPT, wait=False)


def ycsb_load(default_cfg, experiment_spec):
    experiment_spec[COMMAND] = 'load'
    exec_ycsb(default_cfg, experiment_spec)


def ycsb_run(default_cfg, experiment_spec):
    experiment_spec[COMMAND] = 'run'
    exec_ycsb(default_cfg, experiment_spec)


def exec_ycsb(default_cfg, experiment_spec):
    ycsb_path = os.getenv(YCSB_PATH)
    if not ycsb_path:
        print(f"environment variable {YCSB_PATH} has to be set")
        sys.exit(1)
    ycsb_sh = f"{ycsb_path}/bin/ycsb.sh"
    ycsb_command_builder = YCSBCommandBuilder(experiment_spec=experiment_spec, default_config=default_cfg)
    ycsb_command = ycsb_command_builder.build_command()
    log_output_filename = ycsb_command_builder.get_log_output_filename()
    if DEBUG_JAVA in experiment_spec and experiment_spec[DEBUG_JAVA]:
        debug_java = f". set_java_opts_debug; "
        print("remote debugging enabled. don't forget to start remote debugger.")
    else:
        debug_java = ""
    command = f"{debug_java}sh {ycsb_sh} {ycsb_command}"
    with open(log_output_filename, "w") as log_output_file:
        p = subprocess.Popen(command, cwd=ycsb_path, stdout=log_output_file, stderr=log_output_file, shell=True)
    p.wait()


if __name__ == '__main__':
    main()
