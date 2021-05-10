#!/usr/bin/python3

# Run with:

from fs.walk import Walker
from fs.onedatafs import OnedataFS
from time import time, sleep
import requests
import os
from vars import nr_dirs, nr_subdirs, nr_subdir_files, provider, token, space_name, nr_warm_subdirs, qos_request_interval

def set_qos(s, file_id, json):
    while True:
        r = s.get("https://"+provider+"/api/v3/oneprovider/data/"+file_id+"/qos/summary", headers=myheaders)
        if r.json()['status'] == 'fulfilled':
            break
        print('.', end='', flush=True)
        sleep(3)
    if r.status_code != 200:
        print("REST call failed")
    if r.json()['requirements'] != {}:
        reqs = list(r.json()['requirements'].keys())
        for x in reqs:
            r = s.delete("https://"+provider+"/api/v3/oneprovider/qos_requirements/"+x, headers=myheaders)
            if r.status_code != 204:
                print("REST call failed")
    myheaders["Content-Type"] = "application/json"
    r = s.post("https://"+provider+"/api/v3/oneprovider/qos_requirements",
               headers=myheaders,
               data=json)
    del myheaders["Content-Type"]
    if r.status_code != 201:
        print("REST call failed")


print('Initiating connection with '+provider+'...')
odfs = OnedataFS(provider, token, force_direct_io=True, cli_args="--communicator-pool-size=20 --communicator-thread-count=8")
space = odfs.opendir(space_name)

print('Creating list of subdirs...')
subdirs = []
for j in range(nr_dirs):
    for k in range(nr_subdirs):
        path='dir-'+str(j)+'/subdir-'+str(k)
        file_id = space.getxattr(path, "org.onedata.file_id").decode("utf-8").strip('"')
        subdirs.append({'name': path, 'file_id': file_id})

print('Closing OnedatFS connection...')
odfs.close()
del odfs
del space

count=0
i_warm=nr_warm_subdirs
i_cold=0

s = requests.Session()
myheaders = {"X-Auth-Token": token }

while True:
    print('\nSetting warm storage for '+subdirs[i_warm]['name']+'...', end='', flush=True)
    set_qos(s, subdirs[i_warm]['file_id'],
            '{"expression": "class=warm", "replicasNum": 1, "fileId":"'+subdirs[i_warm]['file_id']+'"}')
    print('\nSetting cold storage for '+subdirs[i_cold]['name']+'...', end='', flush=True)
    set_qos(s, subdirs[i_cold]['file_id'],
            '{"expression": "class=cold", "replicasNum": 1, "fileId":"'+subdirs[i_cold]['file_id']+'"}')
    i_cold += 1
    if i_cold >= len(subdirs):
        i_cold = 0    
    i_warm += 1
    if i_warm >= len(subdirs):
        i_warm = 0
    count += 1
    sleep(qos_request_interval)
    if count > 1000:
        break
