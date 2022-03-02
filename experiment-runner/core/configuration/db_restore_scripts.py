# Created by Johannes A. Ebster
from .subprocess_utils import *


def restore_db(db):
    restore_script_for_db = get_restore_for_db_path(db)
    execute_script_on_remote_machine(f"{restore_script_for_db}")


def get_restore_for_db_path(db):
    return f"restore_db/{db}"
