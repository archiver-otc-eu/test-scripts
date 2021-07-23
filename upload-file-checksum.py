#!/usr/bin/python3

# Run with:
# ./upload-file-checksum.py [--algorithm <hashing algorithm>] <local-file> <onedata-path>
# The hashing alorithm can be md5, adler32, sha512 

from fs.onedatafs import OnedataFS
from time import time
import argparse
import os
import sys
from vars import *
import hashlib
import zlib

def define_arguments_parser():
    parser = argparse.ArgumentParser(
        description='Uploads file with checksum'
    )

    parser.add_argument(
        '-a', '--algorithm',
        default='md5',
        help='Hasing algorithm',
        dest='algorithm'
    )

    parser.add_argument(
        'local_file'
    )

    parser.add_argument(
        'onedata_path'
    )

    return parser


def read_arguments():
    parser = define_arguments_parser()
    args = parser.parse_args()
    return args


BLOCKSIZE = 1024*1024

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(BLOCKSIZE), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def sha512(fname):
    hash_sha512 = hashlib.sha512()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(BLOCKSIZE), b""):
            hash_sha512.update(chunk)
    return hash_sha512.hexdigest()

def adler32sum(fname):
    asum = 1
    with open(fname, "rb") as f:
        while True:
            data = f.read(BLOCKSIZE)
            if not data:
                break
            #print(type(data))
            asum = zlib.adler32(data, asum)
            if asum < 0:
                asum += 2**32
    return format(asum,'x')

args=read_arguments()
algorithm=args.algorithm
file_to_upload=args.local_file
upload_path=args.onedata_path

odfs = OnedataFS(provider, token, force_proxy_io=True, cli_args="--communicator-pool-size=20 --communicator-thread-count=8")
if algorithm == 'md5':
    hash_value = md5(file_to_upload)
    hash_key = 'md5_orig'
elif algorithm == 'adler32':
    hash_value = adler32sum(file_to_upload)
    hash_key = 'adler32_orig'
elif algorithm == 'sha512':
    hash_value = sha512(file_to_upload)
    hash_key = 'sha512_orig'
else:
    print(algorithm+" not supported")
    quit()
print(hash_key, hash_value)
with open(file_to_upload,"rb") as read_file:
    odfs.writefile(upload_path, read_file)
odfs.setxattr(upload_path, hash_key, "\""+hash_value+"\"")
del odfs
    

