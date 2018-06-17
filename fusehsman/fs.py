#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

from errno import EACCES
from shutil import copyfile

import logging
import tarfile
from threading import Timer

import os.path as os_path
from sys import argv, exit
from threading import Lock

import os

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class ArchiveManager():
    def __init__(self, path):
        self.archive_name = os_path.join(path, 'archive.tar.gz')
        try:
            os.remove(self.archive_name)
        except OSError:
            pass
        self.archive = tarfile.open(self.archive_name, mode='w:gz')
        self.archive.close()
        self.archived_files = {}
        self.timer = {}

    OFFSET = 600.0

    def compress(self, path, fh):
        if fh not in self.archived_files.keys() or self.archived_files[fh] is False:
            with tarfile.open(self.archive_name, mode='w:gz') as tar:
                tar.add(path)
            self.archived_files[fh] = True
            try:
                os.remove(path)
            except OSError:
                pass

    def compress_with_timer(self, path, fh):
        if fh in self.timer.keys():
            self.timer[fh].cancel()
        self.timer[fh] = Timer(self.OFFSET, self.compress, [path, fh])
        self.timer[fh].start()

    def extract(self, path, fh):
        if fh in self.archived_files.keys() and self.archived_files[fh] is True:
            old_archive_name = os_path.join(os_path.dirname(self.archive_name), 'old_archive.tar.gz')
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
            self.timer[fh] = Timer(600.0, self.compress, [path, fh])
            self.archived_files[fh] = False
        else:
            pass


class Hierarchical(LoggingMixIn, Operations):
    def __init__(self, data):
        self.data = os_path.abspath(data)
        self.archive = ArchiveManager(os_path.dirname(self.data))
        self.rwlock = Lock()

    def __call__(self, op, path, *args):
        new_path = os_path.join(self.data, os.path.abspath(path)[1:])
        return LoggingMixIn.__call__(self, op, new_path, *args)

    def access(self, path, mode):
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    chmod = os.chmod
    chown = os.chown

    def create(self, path, mode):
        fh = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
        self.archive.compress_with_timer(path, fh)
        return fh

    def flush(self, path, fh):
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        if datasync != 0:
          return os.fdatasync(fh)
        else:
          return os.fsync(fh)

    def getattr(self, path, fh=None):
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    getxattr = None

    def link(self, target, source):
        return os.link(source, target)

    listxattr = None
    mkdir = os.mkdir
    mknod = os.mknod

    def open(self, path, flags):
        # self.create(p.join(self.data, path + '.snapshot'), mode=None)
        # copyfile(path, path + '.snapshot')
        fh = os.open(path, flags)
        return fh

    def read(self, path, size, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        # with tarfile.open(self.archive_name) as tar:
            # tarlist = tar.list(verbose=False)
        logging.info(['.', '..'] + os.listdir(path))
        return ['.', '..'] + os.listdir(path)

    readlink = os.readlink

    def release(self, path, fh):
        return os.close(fh)

    def rename(self, old, new):
        return os.rename(old, os_path.join(self.data, new))

    rmdir = os.rmdir

    def statfs(self, path):
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def symlink(self, target, source):
        return os.symlink(source, target)

    def truncate(self, path, length, fh=None):
        with open(path, 'r+') as f:
            f.truncate(length)

    unlink = os.unlink
    utimens = os.utime

    def write(self, path, data, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


def main():
    if len(argv) != 3:
        print('usage: %s <data> <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    FUSE(Hierarchical(argv[1]), argv[2], foreground=True)


if __name__ == '__main__':
    main()
