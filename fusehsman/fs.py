#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging
import tarfile

from errno import EACCES
import os.path as p
from sys import argv, exit
from threading import Lock

import os

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class Hierarchical(LoggingMixIn, Operations):
    def __init__(self, data):
        self.data = p.realpath(data)
        self.archive = tarfile.open(p.join(p.dirname(p.realpath(data)), 'archive.tar.gz'), mode='w:gz')
        self.archfhs = {}
        self.rwlock = Lock()

    def archivate(self):


    # def __call__(self, op, path, *args):
    #     return super(Hierarchical, self).__call__(op, self.data + path, *args)

    chmod = os.chmod
    chown = os.chown

    def create(self, path, mode):
        fh = os.open(self.data + path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
        self.archfhs[fh] = False
        return fh

    def flush(self, path, fh):
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        if datasync != 0:
          return os.fdatasync(fh)
        else:
          return os.fsync(fh)

    # def getattr(self, path, fh=None):
    #     st = os.lstat(self.data + path)
    #     return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
    #         'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    getxattr = None

    def link(self, target, source):
        return os.link(source, target)

    listxattr = None
    mkdir = os.mkdir
    mknod = os.mknod
    open = os.open

    def read(self, path, size, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        return ['.', '..'] + os.listdir(self.data + path) +

    readlink = os.readlink

    def release(self, path, fh):
        return os.close(fh)

    def rename(self, old, new):
        return os.rename(old, self.data + new)

    rmdir = os.rmdir

    def statfs(self, path):
        stv = os.statvfs(self.data + path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def symlink(self, target, source):
        return os.symlink(source, target)

    def truncate(self, path, length, fh=None):
        with open(self.data + path, 'r+') as f:
            f.truncate(length)

    unlink = os.unlink
    utimens = os.utime

    def write(self, path, data, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


if __name__ == '__main__':
    if len(argv) != 4:
        print('usage: %s <data> <archive> <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)

fuse = FUSE(Hierarchical(argv[1]), argv[3], foreground=True)
