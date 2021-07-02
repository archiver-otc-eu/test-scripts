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
    ds_ids[ds_name] = ds_id
    # establish dataset for the dir
    r = s.post("https://"+provider+"/api/v3/oneprovider/datasets/", json={"rootFileId": ds_id},  headers=myheaders)

def ds_hardlinks(s, ds_name, nr_of_hardlinks):
    ds_id = ds_ids[ds_name]
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
pc = 0 # previous ds_count
ph = 0 # previos hl_count
bn = "dataset-"
hl_count = 0

ds_ids = {}
result = []
with open(list_file_name) as f:
    for line in f.readlines():
        result.append(line.split(","))

ds_count = 0
with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
    s = requests.Session()
    future_to_ds = {executor.submit(ds_create, s, bn+str(ds_i), parent_id, nr_hardlinks): ds_i for ds_i in range(nr_datasets)}
    for future in concurrent.futures.as_completed(future_to_ds):
        if future.exception() != None:
            raise future.exception("Error: Dataset already exists")
        ds = future_to_ds[future]
        ds_count += 1
        ct = time()
        if ct-pt > 1:
            print("\r"+str(ds_count)+" empty datasets in "+str(round(ct-st,2))+" seconds.  Rate (cur/avg): "+str(round((ds_count-pc)/(ct-pt),2))+"/"+str(round((ds_count)/(ct-st),2))+" ds/s", end='', flush=True)
            pt=ct
            pc = ds_count
print("\n"+str(ds_count)+" empty datasets in "+str(round(ct-st,2))+" seconds. Average rate: "+str(round(ds_count/(ct-st),2))+" ds/s  ")

st2 = time()
with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
    s = requests.Session()
    future_to_hl = {executor.submit(ds_hardlinks, s, bn+str(ds_i), nr_hardlinks): ds_i for ds_i in range(nr_datasets)}
    for future in concurrent.futures.as_completed(future_to_hl):
        if future.exception() != None:
            raise future.exception("Error: Adding hardlinks failed")
        ds = future_to_hl[future]
        hl_count += nr_hardlinks
        ct = time()
        if ct-pt > 1:
            print("\r"+str(hl_count)+" hardlinks in "+str(round(ct-st2,2))+" seconds. Rate (cur/avg): "+str(round((hl_count-ph)/(ct-pt),2))+"/"+str(round((hl_count)/(ct-st2),2))+" hl/s  ", end='', flush=True)
            pt=ct
            ph = hl_count
print("\n"+str(ds_count)+" datasets in "+str(round(ct-st,2))+" seconds. Average rate: "+str(round(ds_count/(ct-st),2))+" ds/s")
