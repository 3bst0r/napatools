import subprocess
from threading import Thread
import sys
import requests
import time

USER = "Administrator"
PWD = "password"
BUCKET = "bucket-1"

INDEX_1 = "CREATE PRIMARY INDEX ON `" + BUCKET + "`"
INDEX_2 = "CREATE INDEX ix1 ON `" + BUCKET + "`(address.zip)"
INDEX_3 = "CREATE index ix2 ON `" + BUCKET + "`(address.country, age_group, DATE_PART_STR(dob,'year'))"
INDEX_4 = "CREATE INDEX ix3 ON `" + BUCKET + "`(address.prev_address.zip)"
INDEX_5 = "CREATE INDEX ix4 ON `" + BUCKET + "`(DISTINCT devices)"
INDEX_6 = "CREATE INDEX ix5 ON `" + BUCKET + "`(DISTINCT ARRAY (DISTINCT ARRAY " \
                                             "(v.country || \".\" || c) FOR c IN v.cities END) FOR v IN visited_places END);"
INDEX_7 = "CREATE INDEX ix6 ON `" + BUCKET + "`(address.zip, order_list)"
INDEX_8 = "CREATE INDEX ix7 ON `" + BUCKET + "`(address.zip, month, order_list, sale_price)"
INDEX_9 = "CREATE INDEX ix8 ON `" + BUCKET + "`(address.geo_region)"
INDEX_10 = "CREATE index ix9 ON `" + BUCKET + "`(address.geo_region, age_group, DATE_PART_STR(dob,'year'))"

INDEXING_MAP = {
    "workloadsa": (INDEX_1,),
    "workloadsb": (INDEX_1,),
    "workloadsc": (INDEX_1,),
    "workloadsd": (INDEX_1,),
    "workloadse": (INDEX_1,),
    "workloadsf": (INDEX_1,),
    "workloadsg": (INDEX_2,),
    "workloadsh": (INDEX_3,),
    "workloadsi": (INDEX_4,),
    "workloadsj": (INDEX_5,),
    "workloadsk": (INDEX_6,),
    "workloadsl1": (INDEX_7,),
    "workloadsl2": (INDEX_8,),
    "workloadsmix": (INDEX_1, INDEX_2, INDEX_3, INDEX_4, INDEX_5, INDEX_6, INDEX_7, INDEX_8),
    "workloadsg2": (INDEX_9,),
    "workloadsh2": (INDEX_10,),
    "workloadsmix2": (INDEX_9, INDEX_10, INDEX_1, INDEX_4, INDEX_5, INDEX_6, INDEX_7, INDEX_8),
}


def action_createindex():
    workload = ""
    host = ""
    for i, item in enumerate(sys.argv):
        if item == "-workload":
            workload = sys.argv[i + 1]
        elif item == "-master_host":
            host = sys.argv[i + 1]

    if INDEXING_MAP[workload] != None:
        for indexdef in INDEXING_MAP[workload]:
            api = 'http://{}:8093/query/service'.format(host)
            data = {'statement': indexdef}
            response = requests.post(url=api, data=data, auth=(USER, PWD))
            print(response)


def action_load():
    total = ""
    cached = ""
    host = ""
    workload = ""
    insertstart = ""
    for i, item in enumerate(sys.argv):
        if item == "-total_items":
            total = sys.argv[i + 1]
        elif item == "-cache_items":
            cached = sys.argv[i + 1]
        elif item == "-master_host":
            host = sys.argv[i + 1]
        elif item == "-workload":
            workload = sys.argv[i + 1]
        elif item == "-insertstart":
            insertstart = sys.argv[i + 1]

    cmd = "./bin/ycsb load couchbase2 -P workloads/soe/{} " \
          "-p couchbase.host={} -p couchbase.bucket={} -p couchbase.password={} " \
          "-p operationcount=1 -p recordcount={} -p totalrecordcount={} -threads 20 -p insertstart={}". \
        format(workload, host, BUCKET, PWD, cached, total, insertstart)
    print("Executing command: {}".format(cmd))
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd="../YCSB")
    for line in p.stdout.readlines():
        print(line, end=' ')
    retval = p.wait()


def run_thread(workload, host, t, kv, log, i, insertstart):
    cmd = get_ycsb_run_cmd(workload, host, t, kv, "{}_i{}".format(log, i), insertstart)
    print("Executing command: {}".format(cmd))
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         cwd="../clones/YCSB_{}".format(i))
    for line in p.stdout.readlines():
        print(line, end=' ')
    retval = p.wait()


def action_run():
    workload = ""
    host = ""
    threads = 0
    kv = ""
    log = ""
    insertstart = ""
    for i, item in enumerate(sys.argv):
        if item == "-workload":
            workload = sys.argv[i + 1]
        elif item == "-master_host":
            host = sys.argv[i + 1]
        elif item == "-threads":
            threads = int(sys.argv[i + 1])
        elif item == "-kv":
            kv = sys.argv[i + 1]
        elif item == "-log":
            log = sys.argv[i + 1]
        elif item == "-insertstart":
            insertstart = sys.argv[i + 1]

    if threads > 35:
        for i in (1, 2, 3, 4):
            time.sleep(10)
            if i != 4:
                t = int(threads / 4)
            else:
                t = threads - int(threads / 4) * 3
            new_thread = Thread(target=run_thread, args=(workload, host, t, kv, log, i, insertstart))
            new_thread.start()

    else:
        cmd = get_ycsb_run_cmd(workload, host, threads, kv, log, insertstart)
        print
        "Executing command: {}".format(cmd)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd="../YCSB")
        for line in p.stdout.readlines():
            print
            line,
        retval = p.wait()


def get_ycsb_run_cmd(workload, host, threads, kv, log, insertstart):
    return "./bin/ycsb run couchbase2 -P workloads/soe/{} " \
           "-p couchbase.host={} -p couchbase.bucket={} -p couchbase.password={} " \
           "-p operationcount=900000000 -p maxexecutiontime=600 -threads {} " \
           "-p couchbase.kv={} -p exportfile=../{}.log -p insertstart={}".format(workload,
                                                                                 host,
                                                                                 BUCKET,
                                                                                 PWD,
                                                                                 threads,
                                                                                 kv,
                                                                                 log,
                                                                                 insertstart)


for i, item in enumerate(sys.argv):
    if item == "-action":
        if sys.argv[i + 1] == "createindex":
            action_createindex()
        elif sys.argv[i + 1] == "load":
            action_load()
        elif sys.argv[i + 1] == "run":
            action_run()
