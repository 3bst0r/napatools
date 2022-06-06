# Created by Johannes A. Ebster
import os

from .config_parameters import DB, WORKLOAD, COMMAND, MONGODB, COUCHBASE, ARANGODB, DB_SPECIFIC_PROPS, \
    YCSB_DEFAULT_PROPS, YCSB_DEFAULT_RUN_PROPS, YCSB_DEFAULT_LOAD_PROPS, OPERATION, EXPERIMENT_NAME, POSTGRESQL, \
    YCSB_RUN_PROPS

HDR_HISTOGRAM_OUTPUT_PATH = "hdrhistogram.output.path"
RAW_OUTPUT_FILE = 'measurement.raw.output_file'
PROPERTY_FLAG = " -p "


class YCSBCommandBuilder:

    def build_command(self) -> str:
        return f"{self.command} {self.db} -s -P {self.workload} " \
               f"{self.db_specific_props()} " \
               f"{self.ycsb_default_props()} " \
               f"{self.ycsb_default_run_props()} " \
               f"{self.ycsb_default_load_props()} " \
               f"{self.ycsb_operation_props()} " \
               f"{self.ycsb_run_props()} " \
               f"{self.output_props()} "

    def __init__(self, experiment_spec: dict, default_config: dict):
        self.default_config = default_config
        self.experiment_spec = experiment_spec
        self.experiment_name = experiment_spec[EXPERIMENT_NAME]
        self.workload = self.set_workload(experiment_spec)
        self.operation = experiment_spec[OPERATION] if OPERATION in experiment_spec else None
        self.db = experiment_spec[DB]
        self.command = experiment_spec[COMMAND] if COMMAND in experiment_spec else 'run'

    @staticmethod
    def set_workload(experiment_config):
        workloads0 = "workloads/soe/workloads0"
        return workloads0 if OPERATION in experiment_config else f"workloads/{experiment_config[WORKLOAD]}"

    @staticmethod
    def flatten_params(params: dict) -> list:
        result = []
        for p in params.items():
            if isinstance(p, tuple):
                result.append(YCSBCommandBuilder.to_parameter(p[0], p[1]))
            else:
                result.append(p)
        return result

    def to_parameters(self, params: dict) -> str:
        params = self.flatten_params(params)
        joined = "".join(params)
        return f"{joined}"

    def db_specific_props(self) -> str:
        db_specific_props = self.default_config[DB_SPECIFIC_PROPS]
        arangodb = ARANGODB
        couchbase = COUCHBASE
        mongodb = MONGODB
        postgresql = POSTGRESQL
        if self.db == arangodb:
            props = db_specific_props[arangodb]
        elif self.db == mongodb:
            props = db_specific_props[mongodb]
        elif self.db == couchbase:
            props = db_specific_props[couchbase]
        elif self.db == postgresql:
            props = db_specific_props[postgresql]
        else:
            raise ValueError(f"unknown db: {self.db}")
        return self.to_parameters(props)

    def ycsb_default_props(self) -> str:
        ycsb_default_props = self.default_config[YCSB_DEFAULT_PROPS]
        return self.to_parameters(ycsb_default_props)

    def ycsb_default_run_props(self) -> str:
        if self.command != "run":
            return " "
        else:
            ycsb_default_run_props = self.default_config[YCSB_DEFAULT_RUN_PROPS]
            return self.to_parameters(ycsb_default_run_props)

    def ycsb_run_props(self):
        if self.command != "run" or YCSB_RUN_PROPS not in self.experiment_spec:
            return " "
        else:
            ycsb_run_props = self.experiment_spec[YCSB_RUN_PROPS]
            return self.to_parameters(ycsb_run_props)

    def ycsb_default_load_props(self) -> str:
        if self.command != "load":
            return " "
        else:
            ycsb_default_load_props = self.default_config[YCSB_DEFAULT_LOAD_PROPS]
            return self.to_parameters(ycsb_default_load_props)

    def ycsb_operation_props(self) -> str:
        if self.operation is not None:
            return self.to_parameter(self.operation, 1)
        else:
            return ""

    def output_props(self) -> str:
        hdrhistogram_filename = self.get_absolute_path_of_output_dir(self.experiment_name)
        raw_output_filename = self.get_absolute_path_of_output_dir(f"{self.experiment_name}_raw.csv")
        return f"{self.to_parameter(HDR_HISTOGRAM_OUTPUT_PATH, hdrhistogram_filename)}" \
               f"{self.to_parameter(RAW_OUTPUT_FILE, raw_output_filename)}"

    def get_log_output_filename(self) -> str:
        return f"{self.experiment_name}_{self.command}_output.log"

    @staticmethod
    def get_absolute_path_of_output_dir(filename) -> str:
        return os.path.join(YCSBCommandBuilder.get_current_working_dir(), filename)

    @staticmethod
    def to_parameter(k, v) -> str:
        return f"{PROPERTY_FLAG}{k}={v} "

    @staticmethod
    def get_current_working_dir() -> str:
        return os.getcwd()
