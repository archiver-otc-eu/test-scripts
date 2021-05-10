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
for j in range(nr_dirs):
    space.makedir('dir-'+str(j))
    for k in range(nr_subdirs):
        space.makedir('dir-'+str(j)+'/subdir-'+str(k))
        for i in range(nr_subdir_files):
            f = subprocess.run(['./gbs', '1048576', '1', '100', 'w'], stdout=subprocess.PIPE)
            space.writebytes('dir-'+str(j)+'/subdir-'+str(k)+'/file-'+str(i), f.stdout)
            file_count += 1
            ct = time()
            if ct-pt > 1:
                pt=ct
                print('\r'+str(file_count)+" files in "+str(round(ct-st,2))+" seconds, avg rate: "+str(round(file_count*100/(ct-st),2))+" MB/s     ", end='', flush=True)
ct = time()
print('\r'+str(file_count)+" files in "+str(round(ct-st,2))+" seconds, avg rate: "+str(round(file_count*100/(ct-st),2))+" MB/s     ", flush=True)

