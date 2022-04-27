# Created by Johannes A. Ebster
from enum import Enum, unique, auto
from typing import Dict, List, Set


@unique
class Operation(Enum):
    Insert = "soe_insert"
    Update = "soe_update"
    Read = "soe_read"
    Scan = "soe_scan"
    Search = "soe_search"
    Page = "soe_page"
    NestScan = "soe_nestscan"
    ArrayScan = "soe_arrayscan"
    ArrayDeepScan = "soe_arraydeepscan"
    Report = "soe_report"
    Report2 = "soe_report2"
    CompoundMultipleArray = "soe_compound_multiple_array"
    LiteralArray = "soe_literal_array"


@unique
class IndexInstance(Enum):
    primary = auto()
    ix1 = auto()
    ix2 = auto()
    ix3 = auto()
    ix4 = auto()
    ix5 = auto()
    ix6 = auto()
    ix7 = auto()
    ix8 = auto()
    ix9 = auto()
    ix10a = auto()
    ix10b = auto()
    ix11 = auto()


OPERATION_MAP: Dict[Operation, Set[IndexInstance]] = {
    Operation.Insert: {IndexInstance.primary},
    Operation.Update: {IndexInstance.primary},
    Operation.Scan: {IndexInstance.primary},
    Operation.Read: {IndexInstance.primary},
    Operation.Page: {IndexInstance.ix1},
    Operation.Search: {IndexInstance.ix2},
    Operation.NestScan: {IndexInstance.ix3},
    Operation.ArrayScan: {IndexInstance.ix4},
    Operation.ArrayDeepScan: {IndexInstance.ix5},
    Operation.Report: {IndexInstance.ix6},
    Operation.Report2: {IndexInstance.ix7},
    # TODO should have only one ix10 and have the dbs create two indexes in place of it
    Operation.CompoundMultipleArray: {IndexInstance.ix10a, IndexInstance.ix10b},
    Operation.LiteralArray: {IndexInstance.ix11}
}

OPERATION_POINT_QUERY: Set[Operation] = {
    Operation.Read,
    Operation.Search,
    Operation.Page,
    Operation.NestScan,
    Operation.Report,
    Operation.Report2,
    Operation.LiteralArray
}

WORKLOAD_OP_MAP = {
    "soe/workloadsa": {Operation.Update, Operation.Read},
    "soe/workloadsb": {Operation.Update, Operation.Read},
    "soe/workloadsc": {Operation.Read},
    "soe/workloadsd": {Operation.Insert, Operation.Read},
    "soe/workloadse": {Operation.Insert, Operation.Scan},
    "soe/workloadsf": {Operation.Insert, Operation.Update, Operation.Read, Operation.Scan, Operation.Page,
                       Operation.Search, Operation.NestScan, Operation.ArrayScan, Operation.ArrayDeepScan,
                       Operation.Report, Operation.Report2},
    "soe/workloadsg": {Operation.Insert, Operation.Update, Operation.Page},
    "soe/workloadsh": {Operation.Insert, Operation.Update, Operation.Search},
    "soe/workloadsi": {Operation.Insert, Operation.Update, Operation.NestScan},
    "soe/workloadsj": {Operation.Insert, Operation.Update, Operation.ArrayScan},
    "soe/workloadsk": {Operation.Insert, Operation.Update, Operation.ArrayDeepScan},
}
""" these are not used
    "soe/workloadsl1": {IndexInstance.ix6},
    "soe/workloadsl2": {IndexInstance.ix7}
"soe/workloadsmix": {
        IndexInstance.primary, IndexInstance.ix1, IndexInstance.ix2, IndexInstance.ix3, IndexInstance.ix4,
        IndexInstance.ix5,
        IndexInstance.ix6, IndexInstance.ix7},
    "soe/workloadsg2": {IndexInstance.ix8},
    "soe/workloadsh2": {IndexInstance.ix9},
    "soe/workloadsmix2": {
        IndexInstance.ix8, IndexInstance.ix9, IndexInstance.primary, IndexInstance.ix3, IndexInstance.ix4,
        IndexInstance.ix5,
        IndexInstance.ix6,
        IndexInstance.ix7},
    }
"""


def get_indexes_for_workload(workload):
    indexes = set()
    operations = WORKLOAD_OP_MAP[workload]
    for operation in operations:
        for index in OPERATION_MAP[operation]:
            indexes.add(index)
    return indexes


WORKLOAD_MAP = {workload: get_indexes_for_workload(workload) for workload in WORKLOAD_OP_MAP}


def is_point_query_operation(operation: str):
    operation = Operation(operation)
    return operation in OPERATION_POINT_QUERY


def contains_point_query_operation(workload: str):
    return WORKLOAD_OP_MAP[workload].intersection(OPERATION_POINT_QUERY)


def get_best_indexes_for_operation(operation: str, db: str = None) -> List[str]:
    operation = Operation(operation)
    return [ix.name for ix in OPERATION_MAP[operation]]


def get_indexes_for_workload(workload, db=None):
    if workload not in WORKLOAD_MAP:
        print(f"WARN: no indexes found for {workload}. not setting up indexes")
        return []
    return [ix.name for ix in WORKLOAD_MAP[workload]]


def get_all_indexes() -> List[str]:
    return [ix.name for ix in IndexInstance]
