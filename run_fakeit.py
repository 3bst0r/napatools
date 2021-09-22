import subprocess
from threading import Thread
import sys

_server = None
_bucket = None
_password = None
_items = None
_threads = None
_global_offset = 0
workdir = "/Users/johannesprivat/WebstormProjects/fakeit-agyryk-3bst0r"

p = subprocess.Popen('make build', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                     cwd=workdir)
retval = p.wait()


def run(batch_size, total_batches, group, s, b, p, g):
    conn_str = "-s {} -b {}".format(s, b)
    if p is not None:
        conn_str = "{} -p {}".format(conn_str, p)

    for i in range(0, total_batches):
        p = subprocess.Popen("./bin/fakeit -d couchbase "
                             "{} --offset {}".format(conn_str,
                                                     batch_size * group * total_batches + i * batch_size + g),
                             shell=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             cwd=workdir)
        for line in p.stdout.readlines():
            print(line, end=' ')
        retval = p.wait()


def run_fakeit(s, b, p, i, t, g):
    batch_size = 1000
    total_batches = i // (batch_size * t)
    for i in range(0, t):
        new_thread = Thread(target=run, args=(batch_size, total_batches, i, s, b, p, g))
        new_thread.start()


for i, item in enumerate(sys.argv):
    if item == "-s":
        _server = sys.argv[i + 1]
    elif item == "-b":
        _bucket = sys.argv[i + 1]
    elif item == "-p":
        _password = sys.argv[i + 1]
    elif item == "-i":
        _items = int(sys.argv[i + 1])
    elif item == "-t":
        _threads = int(sys.argv[i + 1])
    elif item == "-g":
        _global_offset = int(sys.argv[i + 1])

if _server and _bucket and _items and _threads:
    run_fakeit(s=_server, b=_bucket, p=_password, i=_items, t=_threads, g=_global_offset)
else:
    print("Usage run_fakeit.py -s <server> -b <bucket_name> -p <bucket_password if any> "
          "-i <items>  -t <thereads>")
