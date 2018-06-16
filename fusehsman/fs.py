#!/usr/bin/env python
from __future__ import print_function, absolute_import, division
from shutil import copyfile

import logging
import tarfile
from threading import Timer

import os.path as p
from sys import argv, exit
from threading import Lock

import os

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class Hierarchical(LoggingMixIn, Operations):
    def __init__(self, data):
        self.data = p.realpath(data)
        self.archive_name = p.join(p.dirname(p.realpath(data)), 'archive.tar.gz')
        self.archive = tarfile.open(self.archive_name, mode='w:gz')
        self.archive.close()
        self.archfhs = {}
        self.timer = {}
        self.rwlock = Lock()

    def compress(self, path, fh):
        if fh in self.archfhs.keys() and self.archfhs[fh] == False:
            with tarfile.open(self.archive_name, mode='w:gz') as tar:
                tar.add(path)
            try:
                os.remove(path)
            except OSError:
                pass
            self.archfhs[fh] = True

    def extract(self, path, fh):
        if fh in self.archfhs.keys() and self.archfhs[fh] == True:
            old_archive_name = p.join(p.dirname(self.archive_name), 'old_archive.tar.gz')
            os.rename(self.archive_name, old_archive_name)
            original = tarfile.open(old_archive_name)
            modified = tarfile.open(self.archive_name, 'w:gz')
            for info in original.getmembers():
                if info.name == path:
                    original.extract(info, path=self.data)
                    continue
                extracted = original.extractfile(info)
                if not extracted:
                    continue
                modified.addfile(info, extracted)
            original.close()
            modified.close()
            self.timer[fh] = Timer(600.0, self.compress, [p.join(self.data, path), fh])
            self.archfhs[fh] = False
        else:
            pass

    # def __call__(self, op, path, *args):
    #     return super(Hierarchical, self).__call__(op, p.join(self.data, path), *args)

    chmod = os.chmod
    chown = os.chown

    def create(self, path, mode):
        fh = os.open(p.join(self.data, path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
        self.archfhs[fh] = False
        self.timer[fh] = Timer(600.0, self.compress, [p.join(self.data, path), fh])
        return fh

    def flush(self, path, fh):
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        if datasync != 0:
          return os.fdatasync(fh)
        else:
          return os.fsync(fh)

    # def getattr(self, path, fh=None):
    #     st = os.lstat(p.join(self.data, path))
    #     return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
    #         'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    getxattr = None

    def link(self, target, source):
        return os.link(source, target)

    listxattr = None
    mkdir = os.mkdir
    mknod = os.mknod

    def open(self, path, flags):
        self.create(p.join(self.data, path + '.snapshot'), mode=None)
        copyfile(p.join(self.data, path), p.join(self.data, path + '.snapshot'))
        fh = os.open(p.join(self.data, path), flags)
        return fh

    def read(self, path, size, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        with tarfile.open(self.archive_name) as tar:
            tarlist = tar.list(verbose=False)
        return ['.', '..'] + os.listdir(p.join(self.data, path)) + tarlist

    readlink = os.readlink

    def release(self, path, fh):
        return os.close(fh)

    def rename(self, old, new):
        return os.rename(old, self.data + new)

    rmdir = os.rmdir

    def statfs(self, path):
        stv = os.statvfs(p.join(self.data, path))
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def symlink(self, target, source):
        return os.symlink(source, target)

    def truncate(self, path, length, fh=None):
        with open(p.join(self.data, path), 'r+') as f:
            f.truncate(length)

    unlink = os.unlink
    utimens = os.utime

    def write(self, path, data, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <data> <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)

fuse = FUSE(Hierarchical(argv[1]), argv[3], foreground=True)
