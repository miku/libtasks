#!/usr/bin/env python
# coding: utf-8

""" 

This module contains luigi task templates, that can be sublassed and 
which provide usual operations like file conversions, file 
and string normalizations, XSLT, etc.

Overview:

    DefinedFields
    MarcToJSON
    UnicodeNormalizeFile
    DownloadFile
    ShelveMarcRecords
    NormalizeISBNs
    NormalizeValues
    NormalizeAndTokenizeValues
    MARCIDAttributeList
    MARCISBNList
    MARCAuthorList
    MARCAdditionalAuthorList
    MARCCombinedAuthorList
    MARCTitleList
    MARCDDCList
    ConcatenateFiles
    InvertedIndex

"""

from nltk.tokenize import RegexpTokenizer
from libtasks import xisbn
from libtasks.fs import random_tmp_path, isolatedcopy
from libtasks.commands import Executable
from itertools import izip
from subprocess import Popen, PIPE
import collections
import datetime
import luigi
import operator
import pymarc
import os
import tempfile



class DefinedFields(luigi.Task):
    """ 
    Report which MARC fields and subfields are defined on the input
    (a marc file) and to which cardinality.

    Example:

    # recs  8390    1.0
    040.d   50740   6.04767580453
    650.a   42378   5.05101311085
    020.a   25406   3.02812872467
    650.x   19513   2.32574493445
    029.a   17159   2.04517282479
    029.b   17159   2.04517282479
    019.a   15745   1.87663885578
    938.n   15098   1.79952324195
    938.b   15098   1.79952324195
    938.a   15098   1.79952324195
    776.z   14739   1.75673420739
    260.a   13995   1.66805721097
    776.w   13762   1.64028605483
    650.2   11614   1.38426698451
    260.b   9307    1.10929678188
    700.a   9255    1.10309892729

    """
    def requires(self):
        raise NotImplementedError

    def run(self):
        counter = collections.Counter()
        with self.input().open('r') as handle:
            reader = pymarc.MARCReader(handle)
            total = 0
            for record in reader:
                for field in record.get_fields():
                    if field.is_control_field():
                        counter[field.tag] += 1
                    else:
                        data = field.subfields
                        for _, value in izip(data[::2], data[1::2]):
                            counter['%s.%s' % (field.tag, _)] += 1
                total += 1

        with self.output().open('w') as output:
            output.write('# recs\t%s\t1.0\n' % total)
            for tag, count in counter.most_common():
                output.write('%s\t%s\t%s\n' % (
                    tag, count, count / float(total)))

    def output(self):
        raise NotImplementedError


class MarcToJSON(luigi.Task):
    """
    Convert a single input MARC file to Elasticsearch flavored JSON,
    without any metadata, one JSON document per line.

    Requires a `marctojson` binary in `PATH`.

    Example:

    {"content":{"019":[{"a":"428101204","ind2":" ","ind1":" "}],"260":[{" ...
    """

    def _requires(self):
        return (super(MarcToJSON, self)._requires(), 
                Executable(name='marctojson'))

    def requires(self):
        raise NotImplementedError

    def run(self):
        infile = self.input().fn
        stopover = random_tmp_path()
        command = "marctojson -i {infile} -o {outfile}".format(
            infile=infile, outqfile=stopover)
        p = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            os.rename(stopover, self.output().fn)
        else:
            msg = {'stdout': stdout, 'stderr': stderr, 'stopover': stopover,
                   'command': command }
            raise Exception('Could not convert Marc to JSON: %s' % msg)

    def output(self):
        raise NotImplementedError


class UnicodeNormalizeFile(luigi.Task):
    """ 
    Normalize a single file. 
    """
    input_encoding = luigi.Parameter(default='utf-8')
    output_encoding = luigi.Parameter(default='utf-8')    
    normalization = luigi.Parameter(default='nfc', 
        description='nfc, nfd, nfkc or nfkd')

    def _requires(self):
        return (super(UnicodeNormalizeFile, self)._requires(), 
                Executable(name='uconv'))

    def requires(self):
        raise NotImplementedError

    def run(self):
        stopover = random_tmp_path()
        command = """ uconv -f %s -t %s -x %s -o %s %s""" % (
            self.input_encoding, 
            self.output_encoding,
            self.normalization,                    
            stopover,
            self.input().fn)
        p = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            os.rename(stopover, self.output().fn)
        else:
            msg = {'stdout': stdout, 'stderr': stderr, 'stopover': stopover,
                   'command': command }
            raise Exception('uconv failed: %s' % msg)

    def output(self):
        raise NotImplementedError


class DownloadFile(luigi.ExternalTask):
    """ 
    Subclass this task to implement a file download and override
    `url` and `output(self)`. Does not handle redirects.
    """
    url = luigi.Parameter(description='the url to download')

    def run(self):
        r = requests.get(self.url)
        if r.status_code == 200:
            with self.output().open('w') as output:
                output.write(r.text)
        else:
            raise Exception('Non 200 HTTP status code: %s: %s' % 
                            (r.status_code, self.url))

    def output(self):
        raise NotImplementedError


class URLFileDownloads(luigi.Task):
    """ 
    Take a single file with URLs (one per line) 
    and download them all. 
    """

    directory = luigi.Parameter()

    def filename(self, url):
        """ override this in subclass, if you need special filenames """
        splt = urlparse.urlsplit(url)
        filename = splt.path.split('/')[-1] or random_string()
        return os.path.join(self.directory, filename)

    def run(self):
        files = []
        with self.input().open('r') as handle:
            for url in [ line.strip() for line in handle ]:
                stopover = random_tmp_path()
                command = "wget -O %s %s" % (stopover, url)
                code = subprocess.call(command, shell=True)
                if code == 0:
                    dst = self.filename(url)
                    if dst == None:
                        fn = os.path.basename(stopover)
                        dst = os.path.join(self.directory, fn)
                    if not os.path.exists(os.path.dirname(dst)):
                        os.makedirs(os.path.dirname(dst))
                    os.rename(stopover, dst)
                    files.append(dst)
                else:
                    raise Exception('Non-zero return code from wget')
        stopover = random_tmp_path()
        with open(stopover, 'w') as handle:
            for path in files:
                handle.write('%s\n' % path)
        os.rename(stopover, os.path.join(self.directory, '.filelist'))

    def complete(self):
        """ # TODO make this date aware, if necessary """
        return os.path.exists(os.path.join(self.directory, '.filelist'))

    def output(self):
        """ # TODO make this date aware, if necessary """
        outputs = []
        with open(os.path.join(self.directory, '.filelist')) as handle:
            for line in handle:
                path = line.strip()
                output.append(path)
        return [ luigi.LocalTarget(p) for p in outputs ]


class ShelveMarcRecords(luigi.Task):
    """ 
    Create a `shelve` version of the data for faster loads or easier
    ad-hoc lookups in an interactive python session.

    See: http://docs.python.org/2/library/shelve.html
    """

    idfield = luigi.Parameter(default='001', 
                              description='MARC field to use as ID')

    def requires(self):
        """ A single MARC file.
        """
        raise NotImplementedError

    def run(self):
        stopover = random_tmp_path()
        records = shelve.open(stopover)

        with self.input().open('r') as handle:
            reader = FatMARCReader(handle)
            for record in reader:
                id = record.firstvalue(self.idfield, default=None)
                if id is None:
                    raise ValueError('Undefined ID value for record: %s' % record)
                if id in records:
                    if not cmp(record.as_dict(), records[id].as_dict()) == 0:
                        raise ValueError(
                            'Different records with same id in the same file')
                    else:
                        print('Identical record found for ID: %s' % id)
                records[id] = record

        records.close()
        atomic_copyfile(stopover, self.output().fn)

    def output(self):
        """ A single file that contains the `shelve` version of the data.
        Basically a persistent dictionary with `self.idfield` as key.
        """
        raise NotImplemented


class NormalizeISBNs(luigi.Task):
    """ 
    Take a twoo-column TSV of IDs and ISBNs and normalize it 
    to the 13 digit variant. Write back a two-column TSV list of normalized 
    ID-ISBN pairs.
    """
    def requires(self):
        """ Expects a two-column TSV with ID,ISBN columns
        """
        raise NotImplemented

    def run(self):
        with self.input().open('r') as handle:
            with self.output().open('w') as output:
                for line in handle:
                    id, freeform = line.strip().split('\t', 1)
                    normalized = xisbn.normalize(freeform)
                    output.write('%s\t%s\n' % (id, normalized))

    def output(self):
        """ Will write a two-columbn TSV with ID,ISBN - where the ISBN is
        normalized to the 13 digit variant, without any dashes. 
        """
        raise NotImplemented


class NormalizeValues(luigi.Task):
    """ 
    Take a TSV file with of the form 

    ID     value
    ID     value
    ...

    and normalize the values.
    Normalization can be whitespace, lowercase, alphanumeric and alpha.
    """
    ws = luigi.BooleanParameter(default=True, description='1 whitespace '
                                'separators only')
    lower = luigi.BooleanParameter(default=True, description='lowercase')
    alphanumeric = luigi.BooleanParameter(default=False, 
                                          description='del [^a-zA-Z0-9 ]')
    alpha = luigi.BooleanParameter(default=False, 
                                   description='del [^a-zA-Z ]')

    @property
    def signature(self):
        """ optionally use this in your output 
        to distinguish between normalization levels
        """
        return '%s%s%s%s' % (
            int(self.ws),
            int(self.lower),
            int(self.alphanumeric),
            int(self.alpha))

    def run(self):
        with self.input().open('r') as handle:
            with self.output().open('w') as output:
                for line in handle:
                    try:
                        id, val = line.strip().split('\t', 1)
                        if self.lower:
                            val = val.lower()
                        if self.alphanumeric:
                            val = re.sub('[^a-zA-Z0-9 ]', '', val)
                        if self.alpha:
                            val = re.sub('[^a-zA-Z ]', '', val)
                        if self.ws:
                            val = " ".join(val.split())

                        output.write('%s\t%s\n' % (id, val))    
                    except ValueError as exc:
                        print('line failed: %s: %s' % (line.strip(), exc))


class NormalizeAndTokenizeValues(luigi.Task):
    """ 
    Take a single two column TSV file and normalize the values as specified
    by the parameters. To distinguish between normalizations, `self.signature`
    can be used in `self.output`.

    To change the parameters in subclasses, override the parameters with your
    default values.

    By default do only lowercase normalization 
    and only emit tokens of length 2 or longer.

    * lower
    * alphanumeric < alpha
    * length
    """
    lower = luigi.BooleanParameter(default=True, description='lowercase')
    alphanumeric = luigi.BooleanParameter(default=False, 
                                          description='del [^a-zA-Z0-9 ]')
    alpha = luigi.BooleanParameter(default=False, 
                                   description='del [^a-zA-Z ]')
    mintokenlen = luigi.IntParameter(default=2, description='filter out tokens,'
                                     ' that are less than mintokenlen long')

    @property
    def signature(self):
        """ optionally use this in your output 
        to distinguish between normalization types
        """
        return '%s%s%s%s' % (
            int(self.lower),
            int(self.alphanumeric),
            int(self.alpha),
            self.mintokenlen)

    def requires(self):
        raise NotImplemented

    def run(self):
        tokenizer = RegexpTokenizer('\w+')
        with self.input().open('r') as handle:
            with self.output().open('w') as output:
                for line in handle:
                    try:
                        id, val = line.strip().split('\t', 1)
                        if self.lower:
                            val = val.lower()
                        if self.alphanumeric:
                            val = re.sub('[^a-zA-Z0-9 ]', '', val)
                        if self.alpha:
                            val = re.sub('[^a-zA-Z ]', '', val)
                        tokens = tokenizer.tokenize(val)
                        for token in tokens:
                            if len(token) > self.mintokenlen:
                                output.write('%s\t%s\n' % (id, token))
                    except ValueError as exc:
                        print('line could not be splitted: %s: %s' % (line.strip(), exc))

    def output(self):
        raise NotImplemented


class MARCIDAttributeList(luigi.Task):
    """ 
    Create a TSV with 2 columns: ID and the attribute values. 

    This will raise an Exception, when:

    * the ID field is not defined, or 
    * the ID field is not unique
    """
    idfield = luigi.Parameter(default='001', 
        description='marc field to use as ID field; '
                    'this must be uniq across the file')
    field = luigi.Parameter(description='MARC fieldspec to use as attribute; '
                            'multiple specs can be given, e.g. "100.a 700.a" '
                            'to create a list of joint values')

    directory = luigi.Parameter(default=random_tmp_path(), 
        description='directory, where the output will be stored')

    halt_on_duplicate_ids = luigi.BooleanParameter(default=False, 
        description='whether to completely stop, '
                    'when an ID is seen a second time')

    halt_on_undefined_ids = luigi.BooleanParameter(default=False, 
        description='whether to completely stop, '
                    'when the ID value is not found')


    def requires(self):
        """ should return a task that yields a single MARC file """
        raise NotImplemented

    @property
    def filename(self):
        """ You can use this prepared filename in your custom output. """
        fields = self.fields.split()
        today = datetime.datetime.today()
        return 'id-attr-list-%s-%s-%s.tsv' % (self.idfield, '-'.join(fields), 
                today.strftime('%Y-%m-%d'))        

    def run(self):
        """ Right now this will hold all entries in memory to
        get rid of possible duplicates. Maybe just dump the dups and 
        dedup later?
        """
        entries = set()
        fields = self.field.split()
        with self.input().open('r') as handle:
            reader = FatMARCReader(handle)
            for record in reader:
                id = record.firstvalue(self.idfield, default=None)                
                if id is None and self.halt_on_undefined_ids:
                    raise ValueError('ID field is not defined')
                if self.halt_on_duplicate_ids and id in entries:
                    raise ValueError('ID field is not uniq')

                for value in record.itervalues(*fields):
                    entries.add( (id, value) )

        with self.output().open('w') as output:
            for id, value in sorted(entries):
                output.write('%s\t%s\n' % (id, value))

    def output(self):
        """ override this, if you want """
        return luigi.LocalTarget(os.path.join(self.directory, self.filename))


class MARCISBNList(MARCIDAttributeList):
    field = luigi.Parameter(default='020.a')


class MARCAuthorList(MARCIDAttributeList):
    field = luigi.Parameter(default='100.a')


class MARCAdditionalAuthorList(MARCIDAttributeList):
    field = luigi.Parameter(default='700.a')


class MARCCombinedAuthorList(MARCIDAttributeList):
    field = luigi.Parameter(default='100.a 700.a')    


class MARCTitleList(MARCIDAttributeList):
    field = luigi.Parameter(default='245.a')


class MARCDDCList(MARCIDAttributeList):
    field = luigi.Parameter(default='082.a')


class ConcatenateFiles(luigi.Task):
    """ Concatenate all inputs into one. 
    """
    
    def requires(self):
        """ An iterable of target files """
        raise NotImplemented

    def run(self):
        stopover = random_tmp_path()
        for input in self.input():
            command = "cat %s >> %s" % (input.fn, stopover)
            code = subprocess.call(command, shell=True)
            if code == 0:
                os.rename(stopover, self.output().fn)
            else:
                raise Exception('Could not concatenate files: %s' % code)

    def output(self):
        """ Output is a single file, which is a cat'd 
        version of the inputs.
        """
        raise NotImplemented


class InvertedIndex(luigi.Task):
    """ 
    Create a poor mans index from a two column TSV.

    If the file contains lines like:

    1   berlin
    2   leipzig
    3   hamburg
    4   leipzig
    ...

    the inverted index will take the form:

    >>> index.get('berlin')
    set(['1'])
    >>> index.get('leipzig')
    set([2, 4])
    >>> index.get('hamburg')
    set(['3'])
    """
    def requires(self):
        raise NotImplemented

    def run(self):
        """ Use a defaultdict as cache and copy it to a shelve; 
        Beware: all in memory!
        """
        cache = collections.defaultdict(set)

        with self.input().open('r') as handle:
            for line in handle:
                id, token = line.strip().split('\t', 1)
                cache[token].add(id)

        # persist the dict
        stopover = random_tmp_path()
        sh = shelve.open(stopover)

        for key, value in cache.items():
            sh[key] = value

        sh.close()
        isolatedmove(stopover, self.output().fn)

    def output(self):
        raise NotImplemented
