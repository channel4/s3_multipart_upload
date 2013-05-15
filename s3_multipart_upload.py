#!/usr/bin/env python
"""Fill some shit in here"""

import argparse
import sys
import os
import glob
import subprocess
import multiprocessing
import functools
import contextlib
import multiprocessing
from multiprocessing.pool import IMapIterator

try:
    import boto
except ImportError, e:
    raise e

def upload_file_to_s3(filename, dest):
    """
    Uploads large file to S3
    arg:filename:string
    arg:dest:string
     """
    conn = boto.connect_s3()
    bucket = conn.lookup(dest)
    if bucket == None:
        print 'fuck off'
        sys.exit(-1)

    s3_key_name = os.path.basename(filename)
    try:
        mb_size = os.path.getsize(filename) / 1e6
    except OSError, e:
        print e
        sys.exit(-1)
    if mb_size < 60:
        _standard_transfer(bucket, s3_key_name, filename)
    else:
        _multipart_upload(bucket, s3_key_name, filename, mb_size)
        

def _standard_transfer(bucket, s3_key_name, filename):
    """docstring for _standard_transfer"""
    print " Upload with standard transfer, not multipart",
    new_s3_item = bucket.new_key(s3_key_name)
    new_s3_item.set_contents_from_filename(filename, reduced_redundancy=False,
                                           cb=upload_cb, num_cb=10)
    print

def map_wrap(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return apply(f, *args, **kwargs)
    return wrapper

def mp_from_ids(mp_id, mp_keyname, mp_bucketname):
    """Get the multipart upload from the bucket and multipart IDs.

    This allows us to reconstitute a connection to the upload
    from within multiprocessing functions.
    """
    conn = boto.connect_s3()
    bucket = conn.lookup(mp_bucketname)
    mp = boto.s3.multipart.MultiPartUpload(bucket)
    mp.key_name = mp_keyname
    mp.id = mp_id
    return mp

@map_wrap
def transfer_part(mp_id, mp_keyname, mp_bucketname, i, part):
    """Transfer a part of a multipart upload. Designed to be run in parallel.
    """
    mp = mp_from_ids(mp_id, mp_keyname, mp_bucketname)
    print " Transferring", i, part
    with open(part) as t_handle:
        mp.upload_part_from_file(t_handle, i+1)
    os.remove(part)

def _multipart_upload(bucket, s3_key_name, tarball, mb_size, use_rr=False):
    """Upload large files using Amazon's multipart upload functionality.
    """
    cores = multiprocessing.cpu_count()
    def split_file(in_file, mb_size, split_num=5):
        prefix = os.path.join(os.path.dirname(in_file),
                              "%sS3PART" % (os.path.basename(s3_key_name)))
        split_size = int(min(mb_size / (split_num * 2.0), 250))
        if not os.path.exists("%saa" % prefix):
            cl = ["split", "-b%sm" % split_size, in_file, prefix]
            subprocess.check_call(cl)
        return sorted(glob.glob("%s*" % prefix))

    mp = bucket.initiate_multipart_upload(s3_key_name, reduced_redundancy=False)
    with multimap(cores) as pmap:
        for _ in pmap(transfer_part, ((mp.id, mp.key_name, mp.bucket_name, i, part)
                                      for (i, part) in
                                      enumerate(split_file(tarball, mb_size, cores)))):
            pass
    mp.complete_upload()

@contextlib.contextmanager
def multimap(cores=None):
    """Provide multiprocessing imap like function.

    The context manager handles setting up the pool, worked around interrupt issues
    and terminating the pool on completion.
    """
    if cores is None:
        cores = max(multiprocessing.cpu_count() - 1, 1)
    def wrapper(func):
        def wrap(self, timeout=None):
            return func(self, timeout=timeout if timeout is not None else 1e100)
        return wrap
    IMapIterator.next = wrapper(IMapIterator.next)
    pool = multiprocessing.Pool(cores)
    yield pool.imap
    pool.terminate()

def upload_cb(complete, total):
    sys.stdout.write(".")
    sys.stdout.flush()

if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--file",
            help="Filename of file to upload relative/absolute path", 
            type=str)
    argparser.add_argument("--dest",
             help="Destination bucket in the form of s3://my-bucket/", 
            type=str)

    args = argparser.parse_args()
    
    if (args.file or args.dest) == None:
        print __doc__
        sys.exit(-1)
    else:
        upload_file_to_s3(args.file, args.dest)