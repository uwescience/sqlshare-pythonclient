"""
Python interface to SQLShare

G. Cole and D. Halperin and B. Howe
University of Washington
https://github.com/uwescience/sqlshare-pythonclient/
"""

import os.path, tempfile
import glob
import stat, mimetypes
import httplib
import json
import urllib
import sys, os, time
from ConfigParser import SafeConfigParser
import getpass

# For HTTP debug mode, uncomment:
#httplib.HTTPConnection.debuglevel = 1

def debug(msg):
    print msg

DEFAULT_CHUNKSIZE = 100 * 2**20 # 100 MB
DEFAULT_DL_CHUNKSIZE = 50 * 2**10 # 50 KB
DEFAULT_REST_HOST = 'rest.sqlshare.escience.washington.edu'

_DEFAULT_CONFIG = {
    'host': DEFAULT_REST_HOST,
    'chunkSize': str(DEFAULT_CHUNKSIZE),
    'dlChunkSize': str(DEFAULT_DL_CHUNKSIZE)
}

def file_chunks(file_, size):
    """Reads a file in chunks of about the specified size (in bytes).

    The return value is a list of lines found in the read chunk. Incomplete
    lines will be saved for the next chunk returned."""
    pos = file_.tell()
    lines = file_.readlines(size)
    while lines:
        yield (pos, ("".join(lines)))
        lines = file_.readlines(size)


def _load_conf():
    "load information from default configuration file ($HOME/.sqlshare/config)"
    config = SafeConfigParser() #_DEFAULT_CONFIG)
    config.add_section('sqlshare')
    config.set('sqlshare', 'host', DEFAULT_REST_HOST)
    config.set('sqlshare', 'chunkSize', str(DEFAULT_CHUNKSIZE))
    config.set('sqlshare', 'dlChunkSize', str(DEFAULT_DL_CHUNKSIZE))
    conf_file = os.path.expanduser('~/.sqlshare/config')
    if os.path.exists(conf_file):
        # load the configuration
        config.read(conf_file)
    return config

class SQLShareError(ValueError):
    pass

class SQLShareUploadError(SQLShareError):
    pass

class SQLShare(object):
    REST = "/REST.svc"
    RESTFILE = REST + "/v2/file"
    RESTDB = REST + "/v1/db"
    RESTDB2 = REST + "/v2/db"
    chunksize = DEFAULT_CHUNKSIZE
    dl_chunksize = DEFAULT_DL_CHUNKSIZE
    ERROR_NUM = 0
    SQLSHARE_SECTION = 'sqlshare'

    def __init__(self, username=None, password=None):
        """
      @param host: the DNS URL
      @param username: sql share username
      @param password: sql share username's password
      """
        self.config = _load_conf()
        self.username = username
        self.password = password
        if self.username is None:
            self.username = self.config.get('sqlshare', 'user')
        if self.password is None:
            self.password = self.config.get('sqlshare', 'password')
        # if password is still none, get it from command line
        if self.password is None:
            self.password = getpass.getpass()

        self.rest_host = self.config.get('sqlshare', 'host')
        self.chunksize = self.config.getint('sqlshare', 'chunkSize')
        self.dl_chunksize = self.config.getint('sqlshare', 'dlChunkSize')
        self.schema = self.get_userinfo()["schema"]

    def set_auth_header(self, header=None):
        if not header:
            header = {}
        header['Authorization'] = 'ss_apikey ' + self.username + ' : ' + self.password
        return header


    def post_file_chunk(self, filepath, dataset_name, chunk, force_append, force_column_headers):
        filename = os.path.basename(filepath)
        content_type, body = encode_multipart_formdata_via_chunks(filename, chunk)

        h = httplib.HTTPSConnection(self.rest_host)
        headers = {
          'User-Agent': 'python_multipart_caller',
          'Content-Type': content_type,
        }
        self.set_auth_header(headers)

        selector = self.RESTFILE + '?dataset=' + urllib.quote(dataset_name)
        if force_append != None:
            selector += '&force_append=%s' % force_append
        if force_column_headers != None:
            selector += '&force_column_headers=%s' % force_column_headers

        h.request('POST', selector, body, headers)

        res = h.getresponse()
        if res.status == 200:
            return res.read()
        else:
            raise SQLShareError("%s: %s" % (res.status, res.read()))

    def upload(self, filepath, tablenames=None):
        """
      Upload multiple files to sqlshare.  Assumes all files have the same format.
      @param tablename: the tablename that should be created on this datafile
      @param filepath: location to the file to be uploaded
      @param hasHeader: (optional default = true) STRING 'true' or 'false' if the document has a header
      @param delimiter: (optional default = tab) the character to be a delimiter, or 'tab' to refer to '\t'
        """
        fnames = [fn for fn in glob.glob(filepath)]
        if not tablenames:
            tablenames = [os.path.basename(fn) for fn in fnames]
        print "uploading %s into %s" % (filepath, tablenames)
        pairs = zip(fnames, tablenames)
        # get user info; we need the schema name

        for fn, tn in pairs:
            yield self.uploadone(fn, tn)

    def uploadone(self, fn, dataset_name, force_append=None, force_column_headers=None):
        f = open(fn)
        first_chunk = True
        start = time.time()
        rfn = restartfile(fn)

        if os.path.exists(rfn):
            try:
                rf = open(rfn)
                pos = int(rf.read())
                rf.close()
                f.seek(pos)
            except:
                print "Bad restart file %s; ignoring." % rfn

        lines = 0
        for pos, chunk in file_chunks(f, self.chunksize):
            chunk_lines = chunk.count('\n')
            print 'processing chunk line %s to %s (%s s elapsed)' % (lines, lines + chunk_lines, time.time() - start)
            try:
                if first_chunk:
                    self.upload_chunk(fn, dataset_name, chunk, force_append, force_column_headers)
                else:
                    self.upload_chunk(fn, dataset_name, chunk, True, False)
            except SQLShareError as e:
                # record the stopping point in a file
                f = open(rfn, "w")
                f.write(str(pos))
                f.close()
                print >> sys.stderr, "Error uploading data in the chunk starting at pos %d (lines %d to %d): %s" % (pos, lines, lines+chunk_lines, e)
                break
            lines += chunk_lines
            first_chunk = False

        if lines == 0:
            print >> sys.stderr, "Found no data to upload in %s" % f
            return None

        print "finished %s" % dataset_name
        return dataset_name

    def upload_chunk(self, fn, dataset_name, chunk, force_append=None, force_column_headers=None):
        print "pushing %s..." % fn
        # step 1: push file
        jsonuploadid = self.post_file_chunk(fn, dataset_name, chunk, force_append, force_column_headers)
        uploadid = json.loads(jsonuploadid)

        print "parsing %s..." % uploadid
        # step 2: get parse information
        self.poll_selector('%s/v2/file/%s' % (self.REST, uploadid))

    def get_tags(self, query_name, schema=None):
        "Get the tags for a given dataset"
        if not schema:
            schema = self.schema
        params = (self.REST, urllib.quote(schema), urllib.quote(query_name))
        selector = "%s/v2/db/dataset/%s/%s/tags" % params
        return json.loads(self.poll_selector(selector))

    def set_tags(self, name, tags):
        "Set the tags for a given dataset."
        schema = self.schema
        h = httplib.HTTPSConnection(self.rest_host)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.set_auth_header(headers)

        # type is [(name, [tag])]
        tagsobj = [{"name" : self.username, "tags" : tags}]
        params = (self.REST, urllib.quote(schema), urllib.quote(name))
        selector = "%s/v2/db/dataset/%s/%s/tags" % params
        h.request('PUT', selector, json.dumps(tagsobj), headers)
        res = h.getresponse()
        if res.status == 200:
            return json.loads(res.read())
        else:
            raise SQLShareError("%s: %s" % (res.status, res.read()))


    # TODO: Add a generic PUT, or generalize this method
    def poll_selector(self, selector, verb='GET', returnresponse=False, headers=None):
        "Generic GET method to poll for a response"
        if not headers:
            headers = {}
        while True:
            h = httplib.HTTPSConnection(self.rest_host)
            headers.update(self.set_auth_header())
            h.request(verb, selector, '', headers)
            res = h.getresponse()
            if res.status == 200:
                if returnresponse:
                    return res
                else:
                    return res.read()
            if res.status == 202:
                time.sleep(0.5)
                continue
            else:
                raise SQLShareError("code: %s : %s" % (res.status, res.read()))

    def write_error_out(self, chunk):
        f = open("error_set_%s" % self.ERROR_NUM, 'w')
        f.write(chunk)
        f.close()

    # (Why are the tags in a separate API call??)
    def get_userinfo(self):
        "Get metadata for a query"
        selector = '%s/v1/user/%s' % (self.REST, self.username)
        return json.loads(self.poll_selector(selector))

    # attempt to generalize table operations--use poll selector instead
    def tableop(self, tableid, operation):
        h = httplib.HTTPSConnection(self.rest_host)
        headers = self.set_auth_header()
        selector = '%s/%s/%s' % (self.RESTFILE, urllib.quote(tableid), operation)
        h.request('GET', selector, '', headers)
        res = h.getresponse()
        return res

    def get_all_queries(self):
        "Get a list of all queries that are available to user"
        selector = "%s/query" % (self.RESTDB)
        return json.loads(self.poll_selector(selector))

    def get_query(self, schema, query_name):
        "Get meta data about target query, this also includes a cached sampleset of first 200 rows of data"
        selector = "%s/query/%s/%s" % (self.RESTDB, urllib.quote(schema), urllib.quote(query_name))
        return json.loads(self.poll_selector(selector))

    def save_query(self, sql, name, description, is_public=False):
        "Save a query"
        h = httplib.HTTPSConnection(self.rest_host)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        self.set_auth_header(headers)

        queryobj = {
                "is_public": is_public,
          "description":description,
          "sql_code":sql
        }

        selector = "%s/query/%s/%s" % (self.RESTDB, urllib.quote(self.schema), urllib.quote(name))
        h.request('PUT', selector, json.dumps(queryobj), headers)
        res = h.getresponse()
        if res.status == 200:
            return 'modified'
        elif res.status == 201:
            return 'created'
        else: raise SQLShareError("%s: %s" % (res.status, res.read()))

    def delete_query(self, query_name):
        h = httplib.HTTPSConnection(self.rest_host)
        headers = self.set_auth_header()
        selector = "%s/query/%s/%s" % (self.RESTDB, urllib.quote(self.schema), urllib.quote(query_name))
        h.request('DELETE', selector, '', headers)

    def download_sql_result(self, sql, format_='csv', output=None):
        """Return the result of a SQL query as delimited text."""
        selector = "%s/file?sql=%s&format=%s" % (self.RESTDB, urllib.quote(sql), format_)
        response = self.poll_selector(selector, returnresponse=True)
        if output is None:
            return response.read()
        data = response.read(self.dl_chunksize)
        output.write(data)
        while len(data) == self.dl_chunksize:
            data = response.read(self.dl_chunksize)
            output.write(data)
        return

    def materialize_table(self, query_name, new_table_name=None, new_query_name=None):
        h = httplib.HTTPSConnection(self.rest_host)
        headers = self.set_auth_header()
        selector = "/REST.svc/v1/materialize?query_name=%s" % urllib.quote(query_name)

        if new_table_name != None:
            selector += '&table_name=%s' % urllib.quote(new_table_name)

        if new_query_name != None:
            selector += '&new_query_name=%s' % urllib.quote(new_query_name)

        h.request('GET', selector, '', headers)
        res = h.getresponse()
        if res.status >= 400:
            raise SQLShareError("code: %s : %s" % (res.status, res.read()))

        return json.loads(res.read())

    def execute_sql(self, sql, maxrows=700):
        """Execute a sql query"""
        h = httplib.HTTPSConnection(self.rest_host)
        headers = self.set_auth_header()
        selector = "%s?sql=%s&maxrows=%s" % (self.RESTDB, urllib.quote(sql), maxrows)
        h.request('GET', selector, '', headers)
        res = h.getresponse()
        if res.status == 202: #accepted
            location = res.getheader('Location')
            return json.loads(self.poll_selector(location))
        else:
            raise SQLShareError("code: %s : %s" % (res.status, res.read()))

    def get_parser(self, tableid):
        resp = SQLShareUploadResponse(self.tableop(tableid, 'parser'))
        while not resp.done():
            if resp.failed():
                raise ValueError("%s: %s" % resp.error)
            resp = SQLShareUploadResponse(self.tableop(tableid, 'parser'))
        return resp.parser


# Garret says:
# for the most bizzare reason, having the check table function inside of the put_table1 method
# would freeze upon SSL handshake every single time. But it works when its not being called from
# within that method
# Bill says: sounds like a race condition on the server....
    def put_table(self, filename, parser):
        self.put_table1(filename, parser)
        success = self.table_exists(filename)
        while not success:
            time.sleep(0.5)
            success = self.table_exists(filename)
        return True

    def put_table1(self, filename, parser):
     # httplib.HTTPSConnection.debuglevel = 5
        h = httplib.HTTPSConnection(self.rest_host)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.set_auth_header(headers)
        selector = "%s/%s/table" % (self.RESTFILE, urllib.quote(filename))
        h.request('PUT', selector, json.dumps(parser), headers)
        res = h.getresponse()
        time.sleep(0.3)
        if res.status != 200:
            raise SQLShareUploadError("%s: %s" % (res.status, res.read()))

    # why is this method here twice?
    def table_exists(self, filename):
        """ Return true if a table exists """
        #httplib.HTTPSConnection.debuglevel = 5
        h = httplib.HTTPSConnection(self.rest_host)
        headers = self.set_auth_header()
        selector = "%s/%s/table" % (self.RESTFILE, urllib.quote(filename))
        h.request('GET', selector, '', headers)
        res = h.getresponse()
        if res.status == 200:
            return True
        elif res.status == 404:
            return False
        raise SQLShareUploadError("%s: %s" % (res.status, res.read()))

    def get_permissions(self, name, schema=None):
        """ Get the permissions for a given dataset """
        if not schema:
            schema = self.schema
        selector = "%s/dataset/%s/%s/permissions" % (self.RESTDB2, urllib.quote(schema), urllib.quote(name))
        return json.loads(self.poll_selector(selector))

    def set_permissions(self, name, is_public=False, is_shared=False, authorized_viewers=None):
        """ Share table with given users """
        if not authorized_viewers:
            authorized_viewers = []
        h = httplib.HTTPSConnection(self.rest_host)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.set_auth_header(headers)

        queryobj = {
          "is_public": is_public,
          "is_shared": is_shared,
          "authorized_viewers" : authorized_viewers
        }

        # permission API is defined in v2
        selector = "%s/dataset/%s/%s/permissions" % (self.RESTDB2, urllib.quote(self.schema), urllib.quote(name))
        h.request('PUT', selector, json.dumps(queryobj), headers)
        res = h.getresponse()
        if res.status == 200:
            return 'set'
        else:
            raise SQLShareError("%s: %s" % (res.status, res.read()))


class SQLShareUploadResponse(object):
    SUCCESS = 200
    ERROR = 400
    ACCEPTED = 202

    def __init__(self, httpresponse):
        self.error = (None, None)
        self.code = httpresponse.status
        if self.code == self.SUCCESS:
            objs = json.loads(httpresponse.read())
            self.parser = objs['parser']
            self.sample = objs['sample_parsed']
        if self.code >= self.ERROR:
            self.error = (self.code, httpresponse.read())
            raise SQLShareUploadError("%s: %s" % self.error)
        if self.code == self.ACCEPTED:
            pass

    def done(self):
        return self.code != self.ACCEPTED

    def success(self):
        return self.code == self.SUCCESS

    def failed(self):
        return self.code >= self.ERROR


def encode_multipart_formdata_via_chunks(filename, chunk):

    BOUNDARY = '----------ThIs_Is_tHe_bouNdaRY_$'
    CRLF = '\r\n'
    L = []
    contenttype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
    L.append('--%s' % BOUNDARY)
    # L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename))
    L.append('Content-Disposition: form-data; name="file1"; filename="%s"' % filename)
    L.append('Content-Type: %s' % contenttype)
    L.append('\r\n' + chunk)
    L.append('--' + BOUNDARY + '--')
    L.append('')
    body = CRLF.join(L)
    content_type = 'multipart/form-data; boundary=%s' % BOUNDARY
    return content_type, body


def _encode_multipart_formdata(fields, files):
    """
Utility for formatting form data
@return: (content_type, body) ready for httplib.HTTP instance

DO NOT ERASE, USED AS MULTIPART REFERENCE
    """

    BOUNDARY = '----------ThIs_Is_tHe_bouNdaRY_$'
    CRLF = '\r\n'
    L = []
    for (key, value) in fields:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"' % key)
        L.append('')
        L.append(value)
    for (tablename, fd) in files:
        filename = os.path.basename(fd.name) #fd.name.split('/')[-1]
        #filename = filename.split('\\')[-1]
        contenttype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        L.append('--%s' % BOUNDARY)
        # Can't save the table with a different name
        # L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename))
        L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (tablename, tablename))
        L.append('Content-Type: %s' % contenttype)
        fd.seek(0)
        L.append('\r\n' + fd.read())
    L.append('--' + BOUNDARY + '--')
    L.append('')
    body = CRLF.join(L)
    content_type = 'multipart/form-data; boundary=%s' % BOUNDARY
    return content_type, body


# construct the name of the restart file for long uploads
def restartfile(fn):
    return fn + ".sqlshare.restart"
