#!/usr/bin/python3

from random import randint
import random
import requests
from time import time
from datetime import datetime
import concurrent.futures
import json
from vars import *

octet_headers = {"X-Auth-Token": token,
                 "Content-Type": "application/octet-stream"
}
json_headers = {"X-Auth-Token": token,
                 "Content-Type": "application/json"
}

def load_words_list():
    word_source_url = 'https://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain'
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/77.0.3865.90 Chrome/77.0.3865.90 Safari/537.36'
    }
    response = requests.get(word_source_url, headers=headers)
    words = [word.decode('utf-8') for word in response.content.splitlines()]
    return {
        'name_words': [w for w in words if w[0].isupper() and not w.isupper()],
        'words': [w for w in words if not w[0].isupper() and not w.isupper()],
    }

def generate_metadata(file_no, names, words):
    statuses = ('open', 'pending', 'closing', 'closed', 'rejected')
    keyword_count = random.randint(1, 3)
    edition_time = datetime.fromtimestamp(int(time()) - file_no * 7200)
    return json.dumps({
        'id': file_no,
        'creator': random.choice(names),
        'status': {
            'now': random.choice(statuses),
            'prev': random.choice(statuses),
        },
        'keywords': random.sample(words, keyword_count),
        'enabled': random.random() > 0.5,
        'editionTime': edition_time.strftime('%Y/%m/%d %H:%M:%S'),
    });

def create_dir_with_metadata(s, file_name, parent_id, json_meta):
    r = s.post("https://"+provider+"/api/v3/oneprovider/data/"+parent_id+"/children?name="+file_name+"&type=DIR", headers=octet_headers)
    if r.status_code != 201:
        print("REST call failed")
    dir_id=r.json()['fileId']
    r = s.put("https://"+provider+"/api/v3/oneprovider/data/"+dir_id+"/metadata/json",  headers=json_headers, data=json_meta)
    if r.status_code != 204:
        print("REST call failed")
    return dir_id
        

def ds_create_meta(s, ds_name, parent_id, nr_of_hardlinks, ds_nr, names, words):
    ds_id = create_dir_with_metadata(s, ds_name, parent_id,
                             generate_metadata(ds_nr, names, words)) 
    # establish dataset for the dir
    r = s.post("https://"+provider+"/api/v3/oneprovider/datasets/", json={"rootFileId": ds_id},  headers=json_headers)
    if r.status_code != 201:
        print("Dataset establish REST call failed with status:", r.status_code)
        # print("URL:", r.request.url)
        # print("HEADERS:",r.request.headers)
        # print("BODY:",r.request.body)
    randomized = []
    for k in range(nr_of_hardlinks):
        randomized.append(random.choice(result))
    k=0
    for a,b in randomized:
        target_id = b[:-1]
        a = a[1:]
        path = a.replace("/", "-")
        k += 1
        r = s.post("https://"+provider+"/api/v3/oneprovider/data/"+ds_id+"/children?name="+str(k)+"_"+path+"&type=LNK&target_file_id="+target_id, headers=json_headers)
        if r.status_code != 201:
            print("Dataset establish REST call failed with status:", r.status_code)

st = time()
pt = time()
ct = time()

result = []
with open(list_file_name) as f:
    for line in f.readlines():
        result.append(line.split(","))

sample_data = load_words_list()
random.seed(1)
names = [random.choice(sample_data['name_words']) for _ in range(nr_names)]
words = [random.choice(sample_data['words']) for _ in range(nr_words)]

ds_count = 0
with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
    s = requests.Session()
    future_to_ds = {executor.submit(ds_create_meta, s, "dataset-"+str(ds_i), parent_id, nr_hardlinks, ds_i, names, words): ds_i for ds_i in range(nr_datasets)}
    for future in concurrent.futures.as_completed(future_to_ds):
        if future.exception() != None:
            raise future.exception("Error: Dataset already exists")
        ds = future_to_ds[future]
        ds_count += 1
        ct = time()
        if ct-pt > 1:
            pt=ct
            print("\r"+str(ds_count)+" datasets in "+str(round(ct-st, 2))+" seconds. Rate: "+str(round(ds_count/(ct-st), 2))+" ds/s  ", end='', flush=True)
print("\r"+str(ds_count)+" datasets in "+str(round(ct-st, 2))+" seconds. Rate: "+str(round(ds_count/(ct-st), 2))+" ds/s  ")
