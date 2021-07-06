#!/usr/bin/python3

from random import randint
import random
import requests
from time import time, sleep
import concurrent.futures
from vars import *

def post_rec(s, url, **kwargs):
    failed = True
    while failed:
        try:
            failed = False
            r = s.post(url, **kwargs)
        except requests.exceptions.RequestException as e:
            failed = True
            sleep(1)
    return r
    
def ds_create(s, ds_name, parent_id, nr_of_hardlinks):
    myheaders = {"X-Auth-Token": token }
    r = post_rec(s, "https://"+provider+"/api/v3/oneprovider/data/"+parent_id+"/children?name="+ds_name+"&type=DIR", headers=myheaders, timeout=30)
    if r.status_code != 201:
        print("REST call failed: Create DIR", r.request.body)
    ds_id=r.json()['fileId']
    # establish dataset for the dir
    r = post_rec(s, "https://"+provider+"/api/v3/oneprovider/datasets/", json={"rootFileId": ds_id},  headers=myheaders, timeout=30)
    if r.status_code != 201:
        print("REST call failed: Establish dataset")
    randomized = []
    for k in range(nr_of_hardlinks):
        randomized.append(random.choice(result))
    n=1
    for a,b in randomized:
        target_id = b[:-1]
        a = a[1:]
        path = a.replace("/", "-")
        url = "https://"+provider+"/api/v3/oneprovider/data/"+ds_id+"/children?name="+str(n)+"_"+path+"&type=LNK&target_file_id="+target_id
        r = post_rec(s, url, headers=myheaders, timeout=30)
        n+=1
        if r.status_code != 201:
            print("REST call failed: Create hardlink")

st = time()
pt = time()
ct = time()

result = []
with open(list_file_name) as f:
    for line in f.readlines():
        result.append(line.split(","))

ds_count = 0
with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
    s = requests.Session()
    future_to_ds = {executor.submit(ds_create, s, "dataset-"+str(ds_i), parent_id, nr_hardlinks): ds_i for ds_i in range(nr_datasets)}
    for future in concurrent.futures.as_completed(future_to_ds):
        tmp = future.exception()
        if tmp != None:
            print("future exception", tmp, flush=True)
            # raise future.exception()
        ds = future_to_ds[future]
        ds_count += 1
        ct = time()
        if ct-pt > 1:
            pt=ct
            print("\r"+str(ds_count)+" datasets in "+str(round(ct-st,2))+" seconds. Rate: "+str(round((ds_count/(ct-st)),2))+" ds/s ", end='', flush=True)
print("\r"+str(ds_count)+" datasets in "+str(round(ct-st,2))+" seconds. Rate: "+str(round((ds_count/(ct-st)),2))+" ds/s ")
