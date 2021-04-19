#!/usr/bin/python3

from random import randint
import random
import requests
from time import time
import concurrent.futures
from vars import *

myheaders = {"X-Auth-Token": token }

def ds_create(s, ds_name, parent_id, nr_of_hardlinks):
    r = s.post("https://"+provider+"/api/v3/oneprovider/data/"+parent_id+"/children?name="+ds_name+"&type=DIR", headers=myheaders)
    if r.status_code != 201:
        print("REST call failed")
    ds_id=r.json()['fileId']
    # establish dataset for the dir
    r = s.post("https://"+provider+"/api/v3/oneprovider/datasets/", json={"rootFileId": ds_id},  headers=myheaders)
    randomized = []
    for k in range(nr_of_hardlinks):
        randomized.append(random.choice(result))
    for a,b in randomized:
        target_id = b[:-1]
        a = a[1:]
        path = a.replace("/", "-")
        r = s.post("https://"+provider+"/api/v3/oneprovider/data/"+ds_id+"/children?name="+path+"&type=LNK&target_file_id="+target_id, headers=myheaders)

st = time()
pt = time()
ct = time()

result = []
with open(list_file_name) as f:
    for line in f.readlines():
        result.append(line.split(","))

ds_count = 0
with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
    s = requests.Session()
    future_to_ds = {executor.submit(ds_create, s, "dataset-"+str(ds_i), parent_id, nr_hardlinks): ds_i for ds_i in range(nr_datasets)}
    for future in concurrent.futures.as_completed(future_to_ds):
        if future.exception() != None:
            raise future.exception("Error: Dataset already exists")
        ds = future_to_ds[future]
        ds_count += 1
        ct = time()
        if ct-pt > 1:
            pt=ct
            print("\r"+str(ds_count)+" datasets in "+str(ct-st)+" seconds. Rate: "+str(ds_count/(ct-st))+" ds/s", end='', flush=True)
print("\r"+str(ds_count)+" datasets in "+str(ct-st)+" seconds. Rate: "+str(ds_count/(ct-st))+" ds/s")
