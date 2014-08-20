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

    def __set_auth_header(self, header=None):
        """Given the specified HTTP header dict, set the Authorization header
        using the user's API key."""
        if not header:
            header = {}
        header['Authorization'] = 'ss_apikey ' + self.username + ' : ' + self.password
        return header


    def __post_file_chunk(self, filepath, dataset_name, chunk, force_append, force_column_headers):
        filename = os.path.basename(filepath)
        content_type, body = encode_multipart_formdata_chunk(filename, chunk)

        conn = httplib.HTTPSConnection(self.rest_host)
        headers = {
          'User-Agent': 'python_multipart_caller',
          'Content-Type': content_type,
        }
        self.__set_auth_header(headers)

        selector = self.RESTFILE + '?dataset=' + urllib.quote(dataset_name)
        if force_append != None:
            selector += '&force_append=%s' % force_append
        if force_column_headers != None:
            selector += '&force_column_headers=%s' % force_column_headers

        conn.request('POST', selector, body, headers)

        res = conn.getresponse()
        if res.status == 200:
            return res.read()
        else:
            raise SQLShareError("%s: %s" % (res.status, res.read()))

    def __post_file_chunk_v3(self, conn, dataset_name, chunk, upload_id=None):
        """Using the v3 upload API, posts a chunk of the file to the specified
        dataset. If upload_id is None, a new file upload process is initiated.
        Otherwise, the existing process is used and the data is appended. In
        either case, the upload_id is returned."""

        content_type, body = encode_multipart_formdata_chunk(dataset_name, chunk)
        headers = {
          'User-Agent': 'python_multipart_caller',
          'Content-Type': content_type,
        }
        self.__set_auth_header(headers)

        # See http://escience.washington.edu/get-help-now/sql-share-rest-api
        # Upload (A) is do a POST of the first chunk to the right url
        if not upload_id:
            selector = '{}/v3/file'.format(self.REST)
        else:
            selector = '{}/v3/file/{}'.format(self.REST, upload_id)

        conn.request('POST', selector, body, headers)

        res = conn.getresponse()
        if res.status == 200:
            return json.loads(res.read())
        else:
            raise SQLShareError("%s: %s" % (res.status, res.read()))

    def upload_file(self, filename, tablename=None):
        """Uploads the specified file to the specified table. Does this using
        the V3 API, not the V2 API. Faster and less error-prone, but does not
        use the append-based semantics."""

        # Figure out table name
        if not isinstance(tablename, basestring):
            tablename = os.path.basename(filename)
        # Open file and debug (throwing error if not exist or not readable)
        file_ = open(filename, 'r')
        size = os.stat(filename).st_size
        if not size:
            raise SQLShareError("Cannot upload empty file {}".format(filename, size))
        print >> sys.stderr, \
            "Uploading file {0} ({2} bytes) into a table named {1}" \
            .format(filename, tablename, size)

        # Connect to the REST server
        conn = httplib.HTTPSConnection(self.rest_host)

        start_time = time.time()
        cur_bytes = 0
        # (A) First chunk, get upload_id
        bytes_ = file_.read(self.chunksize)
        cur_bytes += len(bytes_)
        upload_id = self.__post_file_chunk_v3(conn, tablename, bytes_)
        elapsed = time.time() - start_time
        print >> sys.stderr, "Uploaded {:d}/{:d} bytes, {:.1f}s elapsed/{:.1f}s expected" \
            .format(cur_bytes, size, elapsed,
                    _expected_time(cur_bytes, size, elapsed))

        # (B) Rest of the chunks to upload_id-specific URL
        bytes_ = file_.read(self.chunksize)
        while bytes_:
            cur_bytes += len(bytes_)
            self.__post_file_chunk_v3(conn, tablename, bytes_, upload_id)
            elapsed = time.time() - start_time
            print >> sys.stderr, "Uploaded {:d}/{:d} bytes, {:.1f}s elapsed/{:.1f}s expected" \
                .format(cur_bytes, size, elapsed,
                        _expected_time(cur_bytes, size, elapsed))
            bytes_ = file_.read(self.chunksize)

        # (C) Get the parser
        print >> sys.stderr, "Upload complete, parsing the file to determine"
        selector = '{}/v3/file/{}/parser'.format(self.REST, upload_id)
        conn.request('GET', selector, headers=self.__set_auth_header())
        res = conn.getresponse()
        if res.status != 200:
            raise SQLShareError('Unable to get parser for file {} (upload_id {}): {}' \
                    .format(filename, upload_id, res.read()))
        else:
            parser = res.read()

        # (E) [skip D which changes the parser]
        print >> sys.stderr, "Parsing complete, beginning transfer to the database. (This can take a while...)"
        selector = '{}/v3/file/{}/database'.format(self.REST, upload_id)
        headers = self.__set_auth_header()
        headers['Content-type'] = 'application/json'
        conn.request('PUT', selector, body=parser, headers=headers)
        res = conn.getresponse()
        if res.status != 202:
            raise SQLShareError('Transfer of file {} (upload_id {}) to database failed: {}' \
                    .format(filename, upload_id, res.read()))

        # (F) poll the selector until 200 is received.
        res = json.loads(self.__poll_selector(selector))
        if 'Detail' in res:
            conn.close()
            raise SQLShareUploadError(res['Detail'])
        else:
            msg = ("Successfully uploaded {} rows to dataset {}"
                   .format(res['records_total'], tablename))

        conn.close()
        return msg

    def upload(self, filepath, tablenames=None):
        """
      Upload multiple files to sqlshare.  Assumes all files have the same format.
      @param tablename: the tablename that should be created on this datafile
      @param filepath: location to the file to be uploaded
      @param hasHeader: (optional default = true) STRING 'true' or 'false' if the document has a header
      @param delimiter: (optional default = tab) the character to be a delimiter, or 'tab' to refer to '\t'
        """
        fnames = [filename for filename in glob.glob(filepath)]
        if not tablenames:
            tablenames = [os.path.basename(filename) for filename in fnames]
        print "uploading %s into %s" % (filepath, tablenames)
        pairs = zip(fnames, tablenames)
        # get user info; we need the schema name

        for filename, tablename in pairs:
            yield self.upload_file(filename, tablename)

    def uploadone(self, filename, dataset_name, force_append=None, force_column_headers=None):
        file_ = open(filename, 'r')
        first_chunk = True
        start = time.time()
        restart_filename = _restartfile(filename)

        if os.path.exists(restart_filename):
            try:
                restart_file = open(restart_filename)
                pos = int(restart_file.read())
                restart_file.close()
                file_.seek(pos)
            except:
                print "Bad restart file %s; ignoring." % restart_filename

        lines = 0
        for pos, chunk in file_chunks(file_, self.chunksize):
            chunk_lines = chunk.count('\n')
            print 'processing chunk line %s to %s (%s s elapsed)' % (lines, lines + chunk_lines, time.time() - start)
            try:
                if first_chunk:
                    self.__upload_chunk(filename, dataset_name, chunk, force_append, force_column_headers)
                else:
                    self.__upload_chunk(filename, dataset_name, chunk, True, False)
            except SQLShareError as err:
                # record the stopping point in a file
                restart_file = open(restart_filename, "w")
                restart_file.write(str(pos))
                restart_file.close()
                print >> sys.stderr, "Error uploading data in the chunk starting at pos %d (lines %d to %d): %s" % (pos, lines, lines+chunk_lines, err)
                break
            lines += chunk_lines
            first_chunk = False

        if lines == 0:
            print >> sys.stderr, "Found no data to upload in %s" % filename
            return None

        print "finished %s" % dataset_name
        return dataset_name

    def __upload_chunk(self, filename, dataset_name, chunk, force_append=None, force_column_headers=None):
        print "pushing %s..." % filename
        # step 1: push file
        jsonuploadid = self.__post_file_chunk(filename, dataset_name, chunk, force_append, force_column_headers)
        uploadid = json.loads(jsonuploadid)

        print "parsing %s..." % uploadid
        # step 2: get parse information
        self.__poll_selector('%s/v2/file/%s' % (self.REST, uploadid))

    def get_tags(self, query_name, schema=None):
        "Get the tags for a given dataset"
        if not schema:
            schema = self.schema
        params = (self.REST, urllib.quote(schema), urllib.quote(query_name))
        selector = "%s/v2/db/dataset/%s/%s/tags" % params
        return json.loads(self.__poll_selector(selector))

    def set_tags(self, name, tags):
        "Set the tags for a given dataset."
        schema = self.schema
        conn = httplib.HTTPSConnection(self.rest_host)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.__set_auth_header(headers)

        # type is [(name, [tag])]
        tagsobj = [{"name" : self.username, "tags" : tags}]
        params = (self.REST, urllib.quote(schema), urllib.quote(name))
        selector = "%s/v2/db/dataset/%s/%s/tags" % params
        conn.request('PUT', selector, json.dumps(tagsobj), headers)
        res = conn.getresponse()
        if res.status == 200:
            return json.loads(res.read())
        else:
            raise SQLShareError("%s: %s" % (res.status, res.read()))


    # TODO: Add a generic PUT, or generalize this method
    def __poll_selector(self, selector, verb='GET', returnresponse=False, headers=None):
        "Generic GET method to poll for a response"
        if not headers:
            headers = {}
        while True:
            conn = httplib.HTTPSConnection(self.rest_host)
            headers.update(self.__set_auth_header())
            conn.request(verb, selector, '', headers)
            res = conn.getresponse()
            if res.status in [200, 201]:
                if returnresponse:
                    return res
                else:
                    return res.read()
            if res.status == 202:
                time.sleep(0.5)
                continue
            else:
                raise SQLShareError("code: %s : %s" % (res.status, res.read()))

    # (Why are the tags in a separate API call??)
    def get_userinfo(self):
        "Get metadata for a query"
        selector = '%s/v1/user/%s' % (self.REST, self.username)
        return json.loads(self.__poll_selector(selector))

    # attempt to generalize table operations--use poll selector instead
    def __tableop(self, tableid, operation):
        conn = httplib.HTTPSConnection(self.rest_host)
        headers = self.__set_auth_header()
        selector = '%s/%s/%s' % (self.RESTFILE, urllib.quote(tableid), operation)
        conn.request('GET', selector, '', headers)
        res = conn.getresponse()
        return res

    def get_all_queries(self):
        "Get a list of all queries that are available to user"
        selector = "%s/query" % (self.RESTDB)
        return json.loads(self.__poll_selector(selector))

    def get_query(self, schema, query_name):
        "Get meta data about target query, this also includes a cached sampleset of first 200 rows of data"
        selector = "%s/query/%s/%s" % (self.RESTDB, urllib.quote(schema), urllib.quote(query_name))
        return json.loads(self.__poll_selector(selector))

    def save_query(self, sql, name, description, is_public=False):
        "Save a query"
        conn = httplib.HTTPSConnection(self.rest_host)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        self.__set_auth_header(headers)

        queryobj = {
                "is_public": is_public,
          "description":description,
          "sql_code":sql
        }

        selector = "%s/query/%s/%s" % (self.RESTDB, urllib.quote(self.schema), urllib.quote(name))
        conn.request('PUT', selector, json.dumps(queryobj), headers)
        res = conn.getresponse()
        if res.status == 200:
            return 'modified'
        elif res.status == 201:
            return 'created'
        else: raise SQLShareError("%s: %s" % (res.status, res.read()))

    def delete_query(self, query_name):
        conn = httplib.HTTPSConnection(self.rest_host)
        headers = self.__set_auth_header()
        selector = "%s/query/%s/%s" % (self.RESTDB, urllib.quote(self.schema), urllib.quote(query_name))
        conn.request('DELETE', selector, '', headers)

    def download_sql_result(self, sql, format_='csv', output=None):
        """Return the result of a SQL query as delimited text."""
        selector = "%s/file?sql=%s&format=%s" % (self.RESTDB, urllib.quote(sql), format_)
        response = self.__poll_selector(selector, returnresponse=True)
        if output is None:
            return response.read()
        data = response.read(self.dl_chunksize)
        output.write(data)
        while len(data) == self.dl_chunksize:
            data = response.read(self.dl_chunksize)
            output.write(data)

    def materialize_table(self, query_name, new_table_name=None, new_query_name=None):
        conn = httplib.HTTPSConnection(self.rest_host)
        headers = self.__set_auth_header()
        selector = "/REST.svc/v1/materialize?query_name=%s" % urllib.quote(query_name)

        if new_table_name != None:
            selector += '&table_name=%s' % urllib.quote(new_table_name)

        if new_query_name != None:
            selector += '&new_query_name=%s' % urllib.quote(new_query_name)

        conn.request('GET', selector, '', headers)
        res = conn.getresponse()
        if res.status >= 400:
            raise SQLShareError("code: %s : %s" % (res.status, res.read()))

        return json.loads(res.read())

    def execute_sql(self, sql, maxrows=700):
        """Execute a sql query"""
        conn = httplib.HTTPSConnection(self.rest_host)
        headers = self.__set_auth_header()
        selector = "%s?sql=%s&maxrows=%s" % (self.RESTDB, urllib.quote(sql), maxrows)
        conn.request('GET', selector, '', headers)
        res = conn.getresponse()
        if res.status == 202: #accepted
            location = res.getheader('Location')
            return json.loads(self.__poll_selector(location))
        else:
            raise SQLShareError("code: %s : %s" % (res.status, res.read()))

    def get_parser(self, tableid):
        resp = SQLShareUploadResponse(self.__tableop(tableid, 'parser'))
        while not resp.done():
            if resp.failed():
                raise ValueError("%s: %s" % resp.error)
            resp = SQLShareUploadResponse(self.__tableop(tableid, 'parser'))
        return resp.parser


    # why is this method here twice?
    def table_exists(self, filename):
        """ Return true if a table exists """
        #httplib.HTTPSConnection.debuglevel = 5
        conn = httplib.HTTPSConnection(self.rest_host)
        headers = self.__set_auth_header()
        selector = "%s/%s/table" % (self.RESTFILE, urllib.quote(filename))
        conn.request('GET', selector, '', headers)
        res = conn.getresponse()
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
        return json.loads(self.__poll_selector(selector))

    def set_permissions(self, name, is_public=False, is_shared=False, authorized_viewers=None):
        """ Share table with given users """
        if not authorized_viewers:
            authorized_viewers = []
        conn = httplib.HTTPSConnection(self.rest_host)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.__set_auth_header(headers)

        queryobj = {
          "is_public": is_public,
          "is_shared": is_shared,
          "authorized_viewers" : authorized_viewers
        }

        # permission API is defined in v2
        selector = "%s/dataset/%s/%s/permissions" % (self.RESTDB2, urllib.quote(self.schema), urllib.quote(name))
        conn.request('PUT', selector, json.dumps(queryobj), headers)
        res = conn.getresponse()
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


def encode_multipart_formdata_chunk(filename, chunk):

    boundary = '----------ThIs_Is_tHe_bouNdaRY_$'
    crlf = '\r\n'
    lines = []
    contenttype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
    lines.append('--%s' % boundary)
    lines.append('Content-Disposition: form-data; name="file1"; filename="%s"' % filename)
    lines.append('Content-Type: %s' % contenttype)
    lines.append(crlf + chunk)
    lines.append('--' + boundary + '--')
    lines.append('')
    body = crlf.join(lines)
    content_type = 'multipart/form-data; boundary=%s' % boundary
    return content_type, body


# construct the name of the restart file for long uploads
def _restartfile(filename):
    return filename + ".sqlshare.restart"

def _expected_time(cur_size, total_size, cur_time):
    return float(cur_time * total_size) / cur_size
