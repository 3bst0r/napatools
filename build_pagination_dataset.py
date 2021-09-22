from couchbase.bucket import Bucket
import json
from pprint import pprint
import sys
import random
import numpy

defaultmetapath = "../YCSB/workloads/custom/data/meta"
defaultdatapath =  "../YCSB/workloads/custom/data"


def run(s, b, p, i, f):
    filename = "{}/{}".format(defaultmetapath, f)
    with open(filename) as data_file:
        dataset = json.load(data_file)

    if p is not None:
        cb = Bucket("couchbase://{}/{}?operation_timeout=10".format(s, b), password=p)
    else:
        cb = Bucket("couchbase://{}/{}?operation_timeout=10".format(s, b))

    ids = []
    for int_id in range(1, _items - 1):
        ids.append("{}_ycsb".format(int_id))

    random.shuffle(ids)
    counter = 0
    print("Bulding read dataset:")
    for docid in ids:
        counter += 1
        if counter % 100 == 0:
            p, fs = filling_progress(dataset)
            # print "parsed {} values. Dataset is full on {}%".format(counter, p)
            progress_par = "["
            for ip in range(0, 200):
                if ip > p * 2:
                    progress_par += " "
                else:
                    progress_par += "#"
            progress_par += "]"
            print(progress_par)

        document = cb.get(docid).value

        for dataset_field in dataset.keys():
            for document_field in document.keys():
                if "[]{}" in dataset_field:
                    arr_name, sub_field_name = dataset_field.split('[]{}')
                    if document_field == arr_name:
                        arr = document[arr_name]
                        arr_size = len(arr)
                        if arr_size > 0:
                            if arr_size == 1:
                                value = arr[0][sub_field_name]
                            else:
                                random_item = random.randint(0, arr_size - 1)
                                value = arr[random_item][sub_field_name]
                            post_to_dataset(dataset, dataset_field, value)

                elif "[]" in dataset_field:
                    arr_name = dataset_field.split('[]')[0]
                    if document_field == arr_name:
                        arr = document[arr_name]
                        arr_size = len(arr)
                        if arr_size > 0:
                            if arr_size == 1:
                                value = arr[0]
                            else:
                                random_item = random.randint(0, arr_size - 1)
                                value = arr[random_item]
                                post_to_dataset(dataset, dataset_field, value)

                elif "{}" in dataset_field:
                    obj_name, obj_field_name = dataset_field.split('{}')
                    if obj_name == document_field:
                        value = document[document_field][obj_field_name]
                        post_to_dataset(dataset, dataset_field, value)

                else:
                    if document_field == dataset_field:
                        value = document[document_field]
                        post_to_dataset(dataset, dataset_field, value)

        full, _ = is_full(dataset)
        totally_full = False
        if full:
            print "Read dataset is completed, building update dataset"
            random.shuffle(ids)
            max_keys = dataset["_id"]["max_docs_to_update"]
            counter = 0
            for id in ids:
                dataset["_id"]["docs_to_update"].append(id)
                counter += 1
                if counter > max_keys:
                    totally_full = True
                    break

            if totally_full:
                print("Updated dataset is complerted, dumping to a file")
                outf = f.replace("meta.", "dataset.")
                filenameout = "{}/{}".format(defaultdatapath, outf)
                with open(filenameout, 'w') as fp:
                    fp.write(json.dumps(dataset, sort_keys=True, indent=4, separators=(',', ': ')))
                fp.close()
                sys.exit()
            else:
                print("Not enough unique values to satisfy the update docs requirement")

    full, fields = is_full(dataset, with_fields=True)
    if not full:
        print("Not enough unique values to satisfy the dataset requirements. Fields not full are: \n {}".format(fields))


def post_to_dataset(_dataset, _field, _value):
    if _value is not None:
        all_values = set(_dataset[_field]["values"])
        if len(all_values) < int(_dataset[_field]["max_values"]):
            all_values.add(_value)
            _dataset[_field]["values"] = list(all_values)


def is_full(_dataset, with_fields=False):
    full = True
    f = []
    for item in _dataset.keys():
        if len(_dataset[item]["values"]) < int(_dataset[item]["max_values"]):
            full = False
            if with_fields:
                f.append(item)
    return full, f


def filling_progress(_dataset):
    ps = []
    fs = []
    for item in _dataset.keys():
        actual = len(_dataset[item]["values"])
        total = float(_dataset[item]["max_values"])
        p = actual / total * 100
        ps.append(p)
        if p < 100:
            st = set(fs)
            st.add(item)
            fs = list(st)
    sum = 0
    for p in ps:
        sum = sum + p
    if len(ps):
        return sum / len(ps), fs
    else:
        return sum, fs

_server = None
_bucket = None
_items = None
_password = None
_metafile = None

for i,item in enumerate(sys.argv):
    if item == "-s":
        _server = sys.argv[i+1]
    elif item == "-b":
        _bucket = sys.argv[i+1]
    elif item == "-i":
        _items = int(sys.argv[i+1])
    elif item == "-p":
        _password = sys.argv[i+1]
    elif item == "-f":
        _metafile = sys.argv[i+1]

if _server and _bucket and _items and _metafile:
    run(s = _server, b = _bucket, p = _password, i = _items, f = _metafile)
else:
    print("Usage build_pagination_dataset.py -s <server> -b <bucket_name> -p <bucket_password if any> "
          "-i <items>  -f <meta file name w/o path>")