# Created by Johannes A. Ebster
import datetime
import sys
import copy
from threading import Thread
import pymongo
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster, ClusterOptions, PasswordAuthenticator, ClusterTimeoutOptions
from pyArango.database import Database
import pyArango.connection as arango
import psycopg2 as postgres
from psycopg2.extras import DictCursor
import json

SERVER = "dbis-nosql-comparison.uibk.ac.at"


def transfer(threads, customers, cbuser, cbpassword, cbbucket, mongodb, mongotable, offset):
    batch_size = int(customers) // int(threads)

    for i in range(0, int(threads)):
        print("process {} started".format(i))
        new_thread = Thread(target=run_batch,
                            args=(batch_size, i, cbuser, cbpassword, cbbucket, mongodb, mongotable, offset))
        new_thread.start()


def insert_to_mongo(mongodoc, mcoll):
    if skip_mongo:
        print("skipping insert to mongo\n")
        return
    post_id = mcoll.insert_one(mongodoc).inserted_id
    print("document {} has been inserted to mongo".format(post_id))


def insert_customer_doc_to_mongo(cb_doc, mcoll):
    mongodoc = copy.copy(cb_doc)
    y, m, d = cb_doc["dob"].split("-")
    mongodoc["dob"] = datetime.datetime(int(y), int(m), int(d))
    insert_to_mongo(mongodoc, mcoll)


def insert_doc_to_postgres(cb_doc, pg_db):
    pg_doc = copy.copy(cb_doc)
    pg_db.execute("INSERT INTO usertable (YCSB_KEY, YCSB_VALUE) VALUES (%s, %s) ",
                  (pg_doc["_id"], json.dumps(pg_doc)))


def insert_order_doc_to_mongo(cb_doc, mcoll):
    insert_to_mongo(cb_doc, mcoll)


def insert_doc_to_arango(cb_doc, db: Database):
    if skip_arango:
        print("skipping insert to arango\n")
        return
    arangodoc = copy.copy(cb_doc)
    arangodoc["_key"] = arangodoc["_id"]
    aql = "INSERT @doc INTO usertable LET newDoc = NEW RETURN newDoc"
    post_doc = db.AQLQuery(aql, bindVars={"doc": arangodoc})
    print("document {} has been inserted to arango\n".format(post_doc[0]["_key"]))


def run_batch(size, id, cbuser, cbpassword, cbbucket, mongodb, mongotable, offset, pguser='postgres',
              pgpasswd='postgres', pgdbname='test'):
    if cbpassword != "" and cbuser != "":
        cluster = Cluster("couchbase://{}".format(SERVER),
                          ClusterOptions(authenticator=PasswordAuthenticator(cbuser, cbpassword),
                                         timeout_options=ClusterTimeoutOptions(
                                             config_total_timeout=datetime.timedelta(seconds=10))))
        cb = cluster.bucket(cbbucket)
    else:
        cb = Bucket("couchbase://{}/{}?operation_timeout=10".format(SERVER))

    mongo_client = pymongo.MongoClient("mongodb://{}:27017/".format(SERVER))
    mdb = mongo_client[mongodb]
    mcollection = mdb[mongotable]

    arango_client = arango.Connection(arangoURL='http://{}:8529'.format(SERVER))
    arango_db = arango_client['ycsb']

    postgres_connection = postgres.connect(f"dbname={pgdbname} user={pguser} password={pgpasswd} host={SERVER}")
    postgres_cursor = postgres_connection \
        .cursor(cursor_factory=DictCursor)

    min = id * size + 1 + int(offset)
    max = min + size

    orders_inserted = set()

    for i in range(min, max):
        doc_id = "customer:::{}".format(i)
        cb_doc = cb.get(doc_id).value

        orders = cb_doc["order_list"]

        insert_customer_doc(arango_db, cb_doc, mcollection, postgres_cursor)
        for order in orders:
            if order not in orders_inserted:
                cb_orderdoc = cb.get(order).value
                insert_order_doc(cb_orderdoc, arango_db, mcollection, postgres_cursor)
                orders_inserted.add(order)

    postgres_connection.commit()
    postgres_cursor.close()
    postgres_connection.close()


def insert_order_doc(cb_orderdoc, arango_db, mcollection, pg_db):
    insert_order_doc_to_mongo(cb_orderdoc, mcollection)
    insert_doc_to_arango(cb_orderdoc, arango_db)
    insert_doc_to_postgres(cb_orderdoc, pg_db)


def insert_customer_doc(arango_db, cb_doc, mcollection, pg_db):
    insert_customer_doc_to_mongo(cb_doc, mcollection)
    insert_doc_to_arango(cb_doc, arango_db)
    insert_doc_to_postgres(cb_doc, pg_db)


threads = 20
customers = 10000000
cbuser = ""
cbpassword = ""
cbbucket = "bucket-1"
mongodb = "soe"
mongotable = "bucket"
offset = 0
skip_arango = False
skip_mongo = False

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
    elif item == "-mongodb":
        mongodb = sys.argv[i + 1]
    elif item == "-mongotable":
        mongotable = sys.argv[i + 1]
    elif item == "-offset":
        offset = sys.argv[i + 1]
    elif item == "-skip_mongo":
        skip_mongo = sys.argv[i + 1] == 'True'
    elif item == "-skip_arango":
        skip_arango = sys.argv[i + 1] == 'True'

transfer(threads, customers, cbuser, cbpassword, cbbucket, mongodb, mongotable, offset)
