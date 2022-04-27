# Created by Johannes A. Ebster
from enum import Enum, auto

from .config_parameters import *
from .indexes_mappings import get_indexes_for_workload, get_all_indexes, get_best_indexes_for_operation, \
    is_point_query_operation, contains_point_query_operation
from .indexes_scripts import drop_all_indexes, create_index, post_indexing


class IndexesStrategies(Enum):
    best = auto()  # just the best index for operation
    all = auto()  # all indexes
    none = auto()  # no indexes (except primary)
    workload = auto()  # best indexes for each operation contained in workload


class IndexesManager:

    def __init__(self, experiment_spec: dict):
        if INDEXES_STRATEGY not in experiment_spec:
            raise ValueError(f"{INDEXES_STRATEGY} is a required spec property")
        indexes_strategy = IndexesStrategies[experiment_spec[INDEXES_STRATEGY]]
        if indexes_strategy not in IndexesStrategies:
            raise ValueError(
                f"spec property {INDEXES_STRATEGY} must be one of {IndexesStrategies}, but is {indexes_strategy}")
        if indexes_strategy is IndexesStrategies.best:
            if OPERATION not in experiment_spec:
                raise ValueError(f"if {INDEXES_STRATEGY} is {IndexesStrategies.best}, {OPERATION} must be specified")
            self.operation = experiment_spec[OPERATION]
        if USE_HASH_INDEXES in experiment_spec and experiment_spec[USE_HASH_INDEXES]:
            if experiment_spec[DB] != POSTGRESQL:
                raise ValueError(f"{USE_HASH_INDEXES} is only supported for {DB}: {POSTGRESQL}")
            if OPERATION in experiment_spec and not is_point_query_operation(experiment_spec[OPERATION]):
                raise ValueError(f"{experiment_spec[OPERATION]} is not a point-query, "
                                 f"so it cannot be used with {USE_HASH_INDEXES}")
            if WORKLOAD in experiment_spec and not contains_point_query_operation(experiment_spec[WORKLOAD]):
                raise ValueError(f"{experiment_spec[WORKLOAD]} does not contain point-queries, "
                                 f"so it cannot be used with {USE_HASH_INDEXES}")
            self.use_hash_indexes = True
        self.indexes_strategy = indexes_strategy
        self.db = experiment_spec[DB]
        if WORKLOAD in experiment_spec:
            self.workload = experiment_spec[WORKLOAD]

    def setup_indexes(self):
        self.drop_all_indexes()
        if self.indexes_strategy is IndexesStrategies.best:
            self.setup_best_index()
        elif self.indexes_strategy is IndexesStrategies.all:
            self.setup_all_indexes()
        elif self.indexes_strategy is IndexesStrategies.workload:
            self.setup_indexes_for_workload()
        elif self.indexes_strategy is IndexesStrategies.none:
            # nothing to do here, all indexes already dropped
            pass
        self.post_indexing()

    def drop_all_indexes(self):
        print(f"dropping all indexes on {self.db}...")
        drop_all_indexes(self.db)
        print(f"finished dropping indexes on {self.db}.")

    def setup_indexes_for_workload(self):
        print(f"setting up indexes for {self.workload} on {self.db}...")
        indexes = get_indexes_for_workload(self.workload)
        self.create_indexes(indexes)
        print(f"finished setting up index for {self.workload} on {self.db}.")

    def setup_best_index(self):
        print(f"setting up best indexes on {self.db}...")
        best_indexes = get_best_indexes_for_operation(self.operation)
        self.create_indexes(best_indexes)
        print(f"finished setting up best indexes on {self.db}.")

    def setup_all_indexes(self):
        print(f"setting up all indexes on {self.db}...")
        all_indexes = get_all_indexes()
        self.create_indexes(all_indexes)
        print(f"finished setting up all indexes on {self.db}.")

    def create_indexes(self, indexes):
        for index in indexes:
            self.create_index(index)

    def create_index(self, index):
        if index == "primary":
            pass
        else:
            print(f"creating index {index} on {self.db}....")
            create_index(index, self.db, self.use_hash_indexes)
            print(f"created index {index} on {self.db}")

    def post_indexing(self):
        print(f"executing post-indexing step on {self.db}")
        post_indexing(self.db)
        print(f"executed post-indexing step on {self.db}")
