# Created by Johannes A. Ebster
from .subprocess_utils import *


def drop_all_indexes(db):
    indexes_for_db_path = get_indexes_for_db_path(db)
    execute_script_on_remote_machine(f"{indexes_for_db_path}/drop_all_indexes")


def create_index(index, db):
    indexes_for_db_path = get_indexes_for_db_path(db)
    execute_script_on_remote_machine(f"{indexes_for_db_path}/create_index_{index}")


def post_indexing(db):
    indexes_for_db_path = get_indexes_for_db_path(db)
    execute_script_on_remote_machine(f"{indexes_for_db_path}/post_indexing")


def get_indexes_for_db_path(db):
    return f"indexes/{db}"
