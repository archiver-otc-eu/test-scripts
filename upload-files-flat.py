#!/usr/bin/python3

from fs.walk import Walker
from fs.onedatafs import OnedataFS
from time import time
import os
import subprocess
from vars import *

odfs = OnedataFS(provider, token, force_direct_io=True, cli_args="--communicator-pool-size=20 --communicator-thread-count=8")
space = odfs.opendir(space_name)
st = ct = pt = time()
file_count = 0
if 'block_size' not in globals():
    block_size = 1048576
if 'block_count' not in globals():
    block_count = 100
space.makedir('dir-flat')
print('Uploading', files_limit, 'files of size', 
      block_size*block_count/1048576, 'MB...', flush=True)
for i in range(files_limit):
    f = subprocess.run(['./gbs', str(block_size), str(i), str(block_count), 'w'], stdout=subprocess.PIPE)
    space.writebytes('dir-flat'+'/file-'+str(i), f.stdout)
    file_count += 1
    ct = time()
    if ct-pt > 1:
        pt=ct
        print('\r'+str(file_count)+" files in "+str(round(ct-st,2))+" seconds, avg rate: "+str(round(file_count*block_count*block_size/1048576/(ct-st),2))+" MB/s     ", end='', flush=True)
ct = time()
print('\r'+str(file_count)+" files in "+str(round(ct-st,2))+" seconds, avg rate: "+str(round(file_count*block_count*block_size/1048576/(ct-st),2))+" MB/s     ", flush=True)

