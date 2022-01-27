import datetime
import sys
from threading import Thread

import pyArango.connection as arango
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster, ClusterOptions, PasswordAuthenticator, ClusterTimeoutOptions
from pyArango.collection import Collection
from pyArango.database import Database

SERVER = 'dbis-nosql-comparison.uibk.ac.at'


def transfer(threads, customers, cbuser, cbpassword, cbbucket, mongodb, mongotable, offset):
    batch_size = int(customers) // int(threads)

    for i in range(0, int(threads)):
        print("process {} started".format(i))
        new_thread = Thread(target=run_batch,
                            args=(batch_size, i, cbuser, cbpassword, cbbucket, mongodb, mongotable, offset))
        new_thread.start()


def insert_to_arango(id, doc, db: Database):
    aql = "INSERT @doc INTO usertable LET newDoc = NEW RETURN newDoc"
    post_doc = db.AQLQuery(aql, bindVars={"doc": doc})
    print("document {} has been inserted to arango".format(post_doc[0]["_key"]))


def run_batch(size, id, cbuser, cbpassword, cbbucket, mongodb, mongotable, offset):
    if cbpassword != "" and cbuser != "":
        cluster = Cluster("couchbase://{}".format(SERVER),
                          ClusterOptions(authenticator=PasswordAuthenticator(cbuser, cbpassword),
                                         timeout_options=ClusterTimeoutOptions(
                                             config_total_timeout=datetime.timedelta(seconds=10))))
        cb = cluster.bucket(cbbucket)
    else:
        cb = Bucket("couchbase://{}/?operation_timeout=10".format(SERVER))

    client = arango.Connection(arangoURL='http://{}:8529'.format(SERVER))
    db = client['ycsb']

    min = id * size + 1 + int(offset)
    max = min + size

    orders_inserted = set()

    for i in range(min, max):
        doc_id = "customer:::{}".format(i)
        doc = cb.get(doc_id).value

        doc["_key"] = doc["_id"]

        orders = doc["order_list"]

        insert_to_arango(id, doc, db)

        for order in orders:
            if order not in orders_inserted:
                orderdoc = cb.get(order).value
                orderdoc["_key"] = orderdoc["_id"]
                insert_to_arango(id, orderdoc, db)
                orders_inserted.add(order)


threads = 20
customers = 10000000
cbuser = ""
cbpassword = ""
cbbucket = "bucket-1"
mongodb = "soe"
mongotable = "bucket"
offset = 0

for i, item in enumerate(sys.argv):
    if item == "-threads":
        threads = sys.argv[i + 1]
    elif item == "-customers":
        customers = sys.argv[i + 1]
    elif item == "-cbpassword":
        cbpassword = sys.argv[i + 1]
    elif item == "-cbuser":
        cbuser = sys.argv[i + 1]
    elif item == "-cbbucket":
        cbbucket = sys.argv[i + 1]
    elif item == "-offset":
        offset = sys.argv[i + 1]

transfer(threads, customers, cbuser, cbpassword, cbbucket, mongodb, mongotable, offset)
