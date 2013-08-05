#!/usr/bin/env python
# coding: utf-8

"""
Contains file related operations, especially `isolatedmove`, which
will copy files or directories as atomic as possible.

It is not atomic, because:

* there's a tempfile left over if `isolatedmove` is interrupted, although this
  will be cleaned up via `cleanup_temporary_files` exit handler
* some versions of Mac OS X comes with a broken `rename`, 
  see: http://www.weirdnet.nl/apple/rename.html
"""

import atexit
import glob
import random
import os
import shutil
import string
import tempfile
import time

TMP_PREFIX = 'tdo-7481de0fc5c9f4539786c09b0d8e4f37316c0133'
TMP_PREFIX_PERSISTENT = 'tdo-7481de0fc5c9f4539786c09b0d8e4f37316c013F'

def random_string(length=16):
    return ''.join(random.sample(string.letters * length, length))


def random_tmp_path(delete=True):
    """ When `delete` is True, the runtime will try to 
    remove all remains of this file from the temporary directory.
    """
    if delete:
        prefix = TMP_PREFIX
    else:
        prefix = TMP_PREFIX_PERSISTENT
    return os.path.join(tempfile.gettempdir(), '{}-{}'.format(
        prefix, random_string()))


def cleanup_temporary_files():
    """ Temporary files are only leftover, when something interrupts the
    program. Try to remove those before the runtime exits. Since everything
    goes into `/tmp`, the OS might take care of anything leftover some time.
    """
    pattern = os.path.join(tempfile.gettempdir(), '%s*' % TMP_PREFIX)
    for path in glob.glob(pattern):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)


atexit.register(cleanup_temporary_files)


def touch(path):
    """ Touch file. """
    with file(path, 'a'):
        os.utime(path, None)    


def which(program):
    """ Return `None` if no executable can be found. """
    def is_executable(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_executable(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = os.path.join(path.strip('"'), program)
            if is_executable(path):
                return path

    return None


def file_age_in_seconds(path):
    """ Raise OSError on nonexistent files. """
    return time.time() - os.stat(path).st_mtime


def isolatedmove(src, dst):
    """ 
    Move src file or directory to dst in isolation. 
    Poor man's atomicity. 
    Will overwrite any existing at `dst`.
    """
    stopover = random_tmp_path()
    if os.path.isdir(src):
        # copy a tree
        shutil.copytree(src, stopover)
        parent = os.path.dirname(os.path.abspath(dst))
        if not os.path.exists(parent):
            os.makedirs(parent)
        os.rename(stopover, dst)
    else:
        # copy a file
        shutil.copyfile(src, stopover)
        os.rename(stopover, dst)
