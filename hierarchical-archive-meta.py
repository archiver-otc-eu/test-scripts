#!/usr/bin/python3

from random import randint
import random
import requests
from time import time
import concurrent.futures
from vars import *

myheaders = {"X-Auth-Token": token }

def create_file_with_metadata(s, file_name, parent_id, file_content, json_meta):
    myheaders["Content-Type"] = "application/octet-stream"
    r = s.post("https://"+provider+"/api/v3/oneprovider/data/"+parent_id+"/children?name="+file_name, headers=myheaders, data=file_content)
    if r.status_code != 201:
        print("REST call failed")
    file_id=r.json()['fileId']
    myheaders["Content-Type"] = "application/json"
    r = s.put("https://"+provider+"/api/v3/oneprovider/data/"+file_id+"/metadata/json",  headers=myheaders, data=json_meta)
    if r.status_code != 204:
        print("REST call failed")
    return file_id

def create_dir_with_metadata(s, file_name, parent_id, json_meta):
    myheaders["Content-Type"] = "application/octet-stream"
    r = s.post("https://"+provider+"/api/v3/oneprovider/data/"+parent_id+"/children?name="+file_name+"&type=DIR", headers=myheaders)
    if r.status_code != 201:
        print("REST call failed")
    dir_id=r.json()['fileId']
    myheaders["Content-Type"] = "application/json"
    r = s.put("https://"+provider+"/api/v3/oneprovider/data/"+dir_id+"/metadata/json",  headers=myheaders, data=json_meta)
    if r.status_code != 204:
        print("REST call failed")
    return dir_id
        
    
s = requests.Session()

datasets_id = create_dir_with_metadata(s, "Datasets", parent_id, '{"type": { "primary": "Dataset", "secondary": []}}')
collisions_id = create_dir_with_metadata(s, "Collisions", datasets_id, '{"type": { "primary": "Dataset", "secondary": ["Collision"]}}')
create_file_with_metadata(s, "col_data_1", collisions_id, "collisions data 1", '{"type": { "primary": "Dataset", "secondary": ["Collision"]}}')
create_file_with_metadata(s, "col_data_2", collisions_id, "collisions data 2", '{"type": { "primary": "Dataset", "secondary": ["Collision"]}}')
simulations_id = create_dir_with_metadata(s, "Simulations", datasets_id, '{"type": { "primary": "Dataset", "secondary": ["Simulation"]}}')
create_file_with_metadata(s, "sim_data_1", simulations_id, "simulation data 1", '{"type": { "primary": "Dataset", "secondary": ["Simulation"]}}')
create_file_with_metadata(s, "sim_data_2", simulations_id, "simulation data 2", '{"type": { "primary": "Dataset", "secondary": ["Simulation"]}}')
derivations_id = create_dir_with_metadata(s, "Derivations", datasets_id, '{"type": { "primary": "Dataset", "secondary": ["Derived"]}}')
create_file_with_metadata(s, "derived_data_1", derivations_id, "derived data 1", '{"type": { "primary": "Dataset", "secondary": ["Derived"]}}')
create_file_with_metadata(s, "derived_data_2", derivations_id, "derived data 2", '{"type": { "primary": "Dataset", "secondary": ["Derived"]}}')
softwares_id = create_dir_with_metadata(s, "Softwares", parent_id, '{"type": { "primary": "Software", "secondary": []}}')
analysis_id = create_dir_with_metadata(s, "Analysis", softwares_id, '{"type": { "primary": "Software", "secondary": ["Analysis"]}}')
create_file_with_metadata(s, "analysis_1", analysis_id, "Analysis 1", '{"type": { "primary": "Software", "secondary": ["Analysis"]}}')
create_file_with_metadata(s, "analysis_2", analysis_id, "Analysis 2", '{"type": { "primary": "Software", "secondary": ["Analysis"]}}')
frameworks_id = create_dir_with_metadata(s, "Frameworks", softwares_id, '{"type": { "primary": "Software", "secondary": ["Framework"]}}')
create_file_with_metadata(s, "framework_1", frameworks_id, "Framework 1", '{"type": { "primary": "Software", "secondary": ["Framework"]}}')
create_file_with_metadata(s, "framework_2", frameworks_id, "Framework 2", '{"type": { "primary": "Software", "secondary": ["Framework"]}}')
computing_environments_id = create_dir_with_metadata(s, "Computing Environments", parent_id, '{"type": { "primary": "Compting Environments", "secondary": []}}')
virtual_machine_images_id = create_dir_with_metadata(s, "Virtual Machine images", computing_environments_id, '{"type": { "primary": "Compting Environments", "secondary": ["Virtual Machine images"]}}')
create_file_with_metadata(s, "image_1", virtual_machine_images_id, "Image 1", '{"type": { "primary": "Computing Environments", "secondary": ["Virtual Machine images"]}}')
create_file_with_metadata(s, "image_2", virtual_machine_images_id, "Image 2", '{"type": { "primary": "Computing Environments", "secondary": ["Virtual Machine images"]}}')
condition_databases_id = create_dir_with_metadata(s, "Condition databases", computing_environments_id, '{"type": { "primary": "Compting Environments", "secondary": ["Condition databases"]}}')
create_file_with_metadata(s, "database_1", condition_databases_id, "Database 1", '{"type": { "primary": "Computing Environments", "secondary": ["Condition databases"]}}')
create_file_with_metadata(s, "database_2", condition_databases_id, "Database 2", '{"type": { "primary": "Computing Environments", "secondary": ["Condition databases"]}}')


