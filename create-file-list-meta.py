#!/usr/bin/python3

# Run with:

from fs.walk import Walker
from fs.onedatafs import OnedataFS
from time import time
import os
from vars import *

#os.chdir("/root") # Comment this line if runing outside of docker
odfs = OnedataFS(provider, token, force_proxy_io=True, cli_args="--communicator-pool-size=20 --communicator-thread-count=8")
space = odfs.opendir(space_name)
st = ct = pt = time()
file_count = 0 
f = open(list_file_name, "w")
print("Writing file list to "+list_file_name, flush=True) 
for i in range(files_limit):
    path = '/dir-flat/file-'+str(i)
    file_id = space.getxattr(path, "org.onedata.file_id").decode("utf-8").strip('"')
    f.write(("{},{}\n".format(path, file_id)))
    file_count += 1
    ct = time()
    if ct-pt > 1:
        pt=ct
        print('\r'+str(file_count)+" files in "+str(round(ct-st,2))+" seconds, avg rate: "+str(round(file_count/(ct-st),2))+" files/s     ", end='', flush=True)
    if file_count >= files_limit:
        break
ct = time()
print('\r'+str(file_count)+" files in "+str(round(ct-st,2))+" seconds, avg rate: "+str(round(file_count/(ct-st),2))+" files/s     ", flush=True)
f.close()
