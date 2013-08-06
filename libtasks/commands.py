#!/usr/bin/env python
# coding: utf-8

""" 
This module contains luigi tasks, that can/should be used without subclassing.
Overview:

    Directory
    Executable
    MarcXMLtoBinary
    XSLT
    DosToUnix
    DownloadFile
    FTPMirror

"""

from libtasks.fs import random_tmp_path, which
from subprocess import Popen, PIPE
import datetime
import hashlib
import luigi
import re
import os
import urllib
import subprocess


class Directory(luigi.Task):
    """ Create directory or fail. """
    name = luigi.Parameter(description='directory to create')

    def run(self):
        os.makedirs(self.name)

    def complete(self):
        return os.path.exists(self.name)


class Executable(luigi.Task):
    """ 
    Checks, whether an external executable is available.
    This task returns `None` as output, so if this task is
    used make sure you check your input.
    """
    name = luigi.Parameter()

    def run(self):
        """ Just complain explicitly about missing program.
        """
        if not which(self.name):
            raise Exception('external program %s required' % self.name)

    def complete(self):
        return which(self.name) is not None

    def output(self):
        return None


class MarcXMLtoBinary(luigi.Task):
    """ 
    Convert MARC XML to MARC binary.
    """
    infile = luigi.Parameter(description='input file in MARC XML format')
    outfile = luigi.Parameter(description='output file, will be a MARC binary')

    def requires(self):
        return Executable(name='yaz-marcdump')

    def run(self):
        stopover = random_tmp_path()
        command = "yaz-marcdump -i marcxml -o marc {infile} > {outfile}".format(
            infile=self.infile, outfile=stopover)
        p = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            os.rename(stopover, self.outfile)
        else:
            msg = {'stdout': stdout, 'stderr': stderr, 'stopover': stopover,
                   'command': command }
            raise Exception('Could not convert MarcXML to Marc: %s' % msg)

    def output(self):
        return luigi.LocalTarget(self.outfile)


class XSLT(luigi.Task):
    """ Apply an XSL transformation.
    """
    infile = luigi.Parameter(description='input XML file')
    stylesheet = luigi.Parameter(description='path to the XSL file')
    outfile = luigi.Parameter(description='output filename')

    def requires(self):
        return Executable(name='xsltproc')

    def run(self):
        stopover = random_tmp_path()
        command = "xsltproc -o {output} {xsl} {file}".format(
            output=stopover, xsl=self.stylesheet, file=self.infile)
        # code = subprocess.call(command, shell=True)
        p = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            os.rename(stopover, self.outfile)
        else:
            msg = {'stdout': stdout, 'stderr': stderr, 'stopover': stopover,
                   'command': command }
            raise Exception('Could not apply XSLT: %s' % msg)

    def output(self):
        return luigi.LocalTarget(self.outfile)


class DosToUnix(luigi.Task):
    """ Convert line endings. """

    infile = luigi.Parameter(description='input file')
    outfile = luigi.Parameter(description='output file')


    def requires(self):
        return Executable(name='dos2unix')

    def run(self):
        stopover = random_tmp_path()
        command = "dos2unix -n {infile} {output}".format(
            infile=self.infile, output=stopover)
        p = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            os.rename(stopover, self.outfile)
        else:
            msg = {'stdout': stdout, 'stderr': stderr, 'stopover': stopover,
                   'command': command }
            raise Exception('Could not convert DOS to UNIX: %s' % msg)

    def output(self):
        return luigi.LocalTarget(self.outfile)


class DownloadFile(luigi.Task):
    """ Download a file from a URL. Command version.
    """

    url = luigi.Parameter(description='URL to download')
    dst = luigi.Parameter(default=random_tmp_path())

    def run(self):
        target = os.path.dirname(self.output().fn)
        if not os.path.exists(target):
            os.makedirs(target)
        tmpfn, _ = urllib.urlretrieve(self.url)
        os.rename(tmpfn, self.output().fn)

    def output(self):
        return luigi.LocalTarget(self.dst)


class FTPMirror(luigi.Task):
    """ A generic FTP directory sync.
    """
    target = luigi.Parameter(description='the target directory')
    host = luigi.Parameter(description='the FTP host')
    username = luigi.Parameter(description='the FTP username')
    password = luigi.Parameter(description='the FTP password')
    pattern = luigi.Parameter(
        description="e.g. '*leip_*.zip' or 'data/*upd.zip'")

    def requires(self):
        return Executable(name='lftp')

    def run(self):
        if not os.path.exists(self.target):
            os.makedirs(self.target)

        command = """ lftp -u %s,%s 
        -e "set net:max-retries 1; set net:timeout 2; 
        mirror --verbose --only-newer -I '%s' / %s; exit" %s """ % (
            self.username, self.password, self.pattern, 
            self.target, self.host)

        command = re.sub('[ \t\n]+', ' ', command)
        code = subprocess.call(command, shell=True)
        if not code == 0:
            raise Exception('lftp returned nonzero exit code: %s' % code)

        now = datetime.datetime.now()
        with self.output().open('w') as output:
            output.write(now.strftime('%Y-%m-%d-%H%M'))

    def output(self):
        """ Per parameters and hours write a single indicator file.
        """
        now = datetime.datetime.now()
        indicator = hashlib.sha1('%s%s%s%s%s%s' % (
            self.target, self.host, self.username, self.password, self.pattern,
            now.strftime('%Y-%m-%d-%H'))).hexdigest()
        return luigi.LocalTarget(
            os.path.join(random_tmp_path(delete=False), 'FTPMirror', indicator))
