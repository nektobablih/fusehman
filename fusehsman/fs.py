from __future__ import print_function, absolute_import, division

from errno import EACCES

import logging
import tarfile
from threading import Timer

import os.path as os_path
from sys import argv, exit
from threading import Lock

import os

import shutil
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class ArchiveManager(object):

    def __init__(self, path, offset):
        self.archive_name = os_path.join(path, 'archive.tar.gz')
        try:
            os.remove(self.archive_name)
        except OSError:
            pass
        self.archive = tarfile.open(self.archive_name, mode='w:gz')
        self.archive.close()
        self.archived_files = {}
        self.timer = {}
        self.tarlock = Lock()
        self.OFFSET = offset

    def compress(self, path, fh):
        if fh not in self.archived_files.keys() or self.archived_files[fh] is False:
            with self.tarlock:
                tar = tarfile.open(self.archive_name, mode='w:gz')
                tar.add(path)
                tar.close()
                self.archived_files[fh] = True
            try:
                os.unlink(path)
            except OSError:
                pass

    def compress_with_timer(self, path, fh):
        self.refresh_timer(fh)
        self.archived_files[fh] = False
        self.timer[fh] = Timer(self.OFFSET, self.compress, [path, fh])
        self.timer[fh].start()

    def refresh_timer(self, fh):
        if fh in self.timer.keys():
            self.timer[fh].cancel()

    def extract(self, path):
        old_archive_name = os_path.join(os_path.dirname(self.archive_name), 'old_archive.tar.gz')
        try:
            os.unlink(old_archive_name)
        except OSError:
            pass

        fh = None

        with self.tarlock:
            os.rename(self.archive_name, old_archive_name)

            original = tarfile.open(old_archive_name, 'r:gz')
            modified = tarfile.open(self.archive_name, 'w:gz')

            for info in original.getmembers():
                if ('/' + info.name) == path:
                    original.extract(info, path='/')
                    fh = os.open(path)
                    logging.info('EEEEEEEEXXXXXXXXTRACT!')
                    continue
                extracted = original.extractfile(info)
                if not extracted:
                    continue
                modified.addfile(info, extracted)
            original.close()
            modified.close()

        os.unlink(old_archive_name)

        return fh

    def list_files(self, path):
        # logging.info(self.archive_name)

        with self.tarlock:
            try:
                tar = tarfile.open(self.archive_name, mode='r:gz')
                relative_names = [os.path.relpath('/' + item, path) for item in tar.getnames()]
                tar.close()
            except tarfile.ReadError:
                relative_names = []
        return relative_names


class Hierarchical(LoggingMixIn, Operations):
    def __init__(self, data, offset):
        self.data = os_path.abspath(data)
        self.archive = ArchiveManager(os_path.dirname(self.data), offset)
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
        # logging.info('CREATE {}'.format(path))
        # fh = self.archive.extract(path)
        # if fh is None:
        fh = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)

        return fh

    def flush(self, path, fh):
        # logging.info('FLUSH {}'.format(path))
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        logging.info('FSYNC {}'.format(path))
        if datasync != 0:
          return os.fdatasync(fh)
        else:
          return os.fsync(fh)

    def getattr(self, path, fh=None):
        # logging.info('GETTTTTATTTTTTRRRRRRRR {}'.format(path))
        if not os.path.exists(path):
            self.archive.extract(path)
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
        # logging.info('OPEN {}'.format(path))
        # self.create(p.join(self.data, path + '.snapshot'), mode=None)
        # copyfile(path, path + '.snapshot')
        fh = os.open(path, flags)
        self.archive.refresh_timer(fh)
        return fh

    def read(self, path, size, offset, fh):
        # logging.info('READ {}'.format(path))
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        # logging.info('READDIR {}'.format(path))
        return_list = ['.', '..']
        return_list.extend(os.listdir(path))
        # logging.info(self.archive.list_files(path))
        from_archive = self.archive.list_files(path)

        return_list.extend(from_archive)
        return return_list

    readlink = os.readlink

    def release(self, path, fh):
        # logging.info('RELEASE {}'.format(path))
        self.archive.compress_with_timer(path, fh)
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
        # logging.info('WRITE {}'.format(path))
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


def main():
    if len(argv) < 3:
        print('usage: %s <data> <mountpoint> <offset>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    FUSE(Hierarchical(argv[1], argv[3] if len(argv) == 4 else 10.0), argv[2], foreground=True)


if __name__ == '__main__':
    main()
