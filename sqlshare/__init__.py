"""
Python interface to SQLShare

G. Cole and B. Howe
"""
import os.path, tempfile
import glob
import stat, mimetypes
import httplib
#httplib.HTTPConnection.debuglevel = 1
import json
import urllib
import sys, os, time
from ConfigParser import SafeConfigParser
import getpass

def debug(m):
  print m

DEFAULT_CHUNKSIZE = 100*2**20 # 100 MB
DEFAULT_DL_CHUNKSIZE = 50**10 # 50 KB

_DEFAULT_CONFIG = {
    'host': 'rest.sqlshare.escience.washington.edu',
    'chunkSize': str(DEFAULT_CHUNKSIZE),
    'dlChunkSize': str(DEFAULT_DL_CHUNKSIZE)
}

"""
load information from default configuration file ($HOME/.sqlshare/config)
"""
def _load_conf():
    config = SafeConfigParser() #_DEFAULT_CONFIG)
    config.add_section('sqlshare')
    config.set('sqlshare', 'host', 'rest.sqlshare.escience.washington.edu')
    config.set('sqlshare', 'chunkSize', str(DEFAULT_CHUNKSIZE))
    config.set('sqlshare', 'dlChunkSize', str(DEFAULT_DL_CHUNKSIZE))
    confFile = os.path.expanduser('~/.sqlshare/config')
    if os.path.exists(confFile):
       # load the configuration
       config.read(confFile)
    return config

class SQLShareError(ValueError):
  pass

class SQLShareUploadError(SQLShareError):
  pass

class SQLShare:
  REST = "/REST.svc"
  RESTFILE = REST + "/v2/file"
  RESTDB = REST + "/v1/db"
  RESTDB2 = REST + "/v2/db"
  CHUNKSIZE = DEFAULT_CHUNKSIZE 
  DL_CHUNKSIZE = DEFAULT_DL_CHUNKSIZE 
  ERROR_NUM = 0
  SQLSHARE_SECTION = 'sqlshare'

  """
@param host: the DNS URL
@param username: sql share username
@param password: sql share username's password
"""
  def __init__(self,  username = None, password = None):
    self.config = _load_conf()
    self.username = username
    self.password = password
    if self.username is None:
        self.username = self.config.get('sqlshare','user')
    if self.password is None:
        self.password = self.config.get('sqlshare','password')
    # if password is still none, get it from command line
    if self.password is None:
        self.password = getpass.getpass()

    self.HOST = self.config.get('sqlshare','host')
    self.CHUNKSIZE = self.config.getint('sqlshare','chunkSize')
    self.DL_CHUNKSIZE = self.config.getint('sqlshare','dlChunkSize')
    self.schema = self.get_userinfo()["schema"]

  def set_auth_header(self, header = {}):
    header['Authorization'] = 'ss_apikey ' + self.username + ' : ' + self.password
    return header
    
  def chunksoff(self, f, size):
    pos = f.tell()
    lines = f.readlines(size)    
    while lines:
        yield (pos, ("".join(lines)))
        lines = f.readlines(size)	  
  
	  
  """
Upload a datasheet into Sql 
@param tablename: the tablename that should be created on this datafile.  Defaults to filename.
@param fileobj: file-like object to upload
@param hasHeader: (optional default = true) STRING 'true' or 'false' if the document has a header
@param delimiter: (optional default = tab) the character to be a delimiter, or 'tab' to refer to '\t'
  """
  def post_file(self, filepath, tablename=None, hasHeader='true', delimiter='tab'):
    filename = os.path.basename(filepath)
    fileobj = open(filepath)
    fields = []
    content_type, body = _encode_multipart_formdata_via_chunks(filename, chunk)

    h = httplib.HTTPSConnection(self.HOST)
    headers = {
      'User-Agent': 'python_multipart_caller',
      'Content-Type': content_type,
    }
    self.set_auth_header(headers)

    selector = self.RESTFILE
    h.request('POST', selector, body, headers)

    res = h.getresponse()
    resp_msg = res.read()
    return resp_msg
    
    
  def post_file_chunk(self, filepath, dataset_name, chunk, force_append, force_column_headers):
    filename = os.path.basename(filepath)
    fileobj = open(filepath)
    fields = []
    content_type, body = _encode_multipart_formdata_via_chunks(filename, chunk)

    h = httplib.HTTPSConnection(self.HOST)
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
    if res.status == 200: return res.read()
    else: raise SQLShareError("%s: %s" % (res.status, res.read()))

  """
Upload multiple files to sqlshare.  Assumes all files have the same format.
@param tablename: the tablename that should be created on this datafile
@param filepath: location to the file to be uploaded
@param hasHeader: (optional default = true) STRING 'true' or 'false' if the document has a header
@param delimiter: (optional default = tab) the character to be a delimiter, or 'tab' to refer to '\t'
  """
  def upload(self, filepath, tablenames=None, hasHeader='true', delimiter='tab'):
    fnames = [fn for fn in glob.glob(filepath)]
    if not tablenames: 
      tablenames = [os.path.basename(fn) for fn in fnames]
    print "uploading %s into %s" % (filepath, tablenames)
    pairs = zip(fnames,tablenames)
    # get user info; we need the schema name

    for fn,tn in pairs:
      yield self.uploadone(fn,tn)

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
    for pos, chunk in self.chunksoff(f, self.CHUNKSIZE):        
        print 'processing chunk line %s to %s (%s s elapsed)' % (lines, lines + chunk.count('\n'), time.time() - start)
        lines += chunk.count('\n')
        try:
          if first_chunk:
            self.upload_chunk(fn, dataset_name, chunk, force_append, force_column_headers)
          else:
            self.upload_chunk(fn, dataset_name, chunk, True, False)
        except SQLShareError:
          # record the stopping point in a file
          f = open(rfn,"w")
          f.write(str(pos))
          f.close()
        first_chunk = False           

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
    
  """
Get the tags for a given dataset
  """
  def get_tags(self, query_name, schema=None):
    if not schema: schema = self.schema
    params = (self.REST, urllib.quote(schema), urllib.quote(query_name))
    selector = "%s/v2/db/dataset/%s/%s/tags" % params
    return json.loads(self.poll_selector(selector))

  """
Set the tags for a given dataset.
  """
  def set_tags(self, name, tags):
    schema = self.schema
    h = httplib.HTTPSConnection(self.HOST)
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
    if res.status == 200: return json.loads(res.read())
    else: raise SQLShareError("%s: %s" % (res.status, res.read()))


  """
Generic GET method to poll for a response
  """
  # TODO: Add a generic PUT, or generalize this method
  def poll_selector(self, selector, verb = 'GET', returnresponse = False, headers={}):    
    while True:        
        h = httplib.HTTPSConnection(self.HOST)
        headers.update(self.set_auth_header())
        h.request(verb, selector, '', headers)
        res = h.getresponse()
        if res.status == 200:
            if returnresponse: return res
            else: return res.read()
        if res.status == 202:
            time.sleep(0.5)
            continue
        else: 
            raise SQLShareError("code: %s : %s" % (res.status, res.read()))

  def write_error_out(self, chunk):
    f = open("error_set_%s" % self.ERROR_NUM, 'w')
    f.write(chunk)
    f.close()
  
  """
 Get metadata for a query
  """
  # (Why are the tags in a separate API call??)
  def get_userinfo(self):
    selector = '%s/v1/user/%s' % (self.REST, self.username)
    return json.loads(self.poll_selector(selector))

  # attempt to generalize table operations--use poll selector instead
  def tableop(self, tableid, operation):
    h = httplib.HTTPSConnection(self.HOST)
    headers = self.set_auth_header()
    selector = '%s/%s/%s' % (self.RESTFILE, urllib.quote(tableid), operation)
    h.request('GET', selector, '', headers)
    res = h.getresponse()
    return res

  """
 Get a list of all queries that are available to user 
  """
  def get_all_queries(self):    
    selector = "%s/query" % (self.RESTDB) 
    return json.loads(self.poll_selector(selector))
    
  """
Get meta data about target query, this also includes a cached sampleset of first 200 rows of data
  """
  def get_query(self, schema, query_name):
    selector = "%s/query/%s/%s" % (self.RESTDB, urllib.quote(schema), urllib.quote(query_name))
    return json.loads(self.poll_selector(selector))
    
  """
Save a query
  """    
  def save_query(self, sql, name, description, is_public=False):
    h = httplib.HTTPSConnection(self.HOST)
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
    h = httplib.HTTPSConnection(self.HOST)    
    headers = self.set_auth_header()
    selector = "%s/query/%s/%s" % (self.RESTDB, urllib.quote(self.schema), urllib.quote(query_name))    
    h.request('DELETE', selector, '', headers)       

  """
Return the result of a SQL query as delimited text.
  """
  # TODO: Need to be able to control the format
  def download_sql_result(self, sql, format='csv'):
    selector = "%s/file?sql=%s&format=%s" % (self.RESTDB, urllib.quote(sql), format)    
    return self.poll_selector(selector)

  def materialize_table(self, query_name, new_table_name=None, new_query_name=None):
    h = httplib.HTTPSConnection(self.HOST)        
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
 
  """
Execute a sql query
  """
  # What's the difference between this and download_sql_result?   
  def execute_sql(self, sql, maxrows=700):
    h = httplib.HTTPSConnection(self.HOST)        
    headers = self.set_auth_header()
    selector = "%s?sql=%s&maxrows=%s" % (self.RESTDB, urllib.quote(sql),maxrows)    
    h.request('GET', selector, '', headers)
    res = h.getresponse()
    if res.status == 202: #accepted
      location = res.getheader('Location')
      return json.loads(self.poll_selector(location))
    else:
      raise SQLShareError("code: %s : %s" % (res.status, res.read()))
        
  def get_parser(self, tableid):
    resp = SQLShareUploadResponse(self.tableop(tableid, 'parser'))
    while (not resp.done()):
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
    h = httplib.HTTPSConnection(self.HOST)
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

  """
Return true if a table exists
  """
  # why is this method here twice?
  def table_exists(self, filename):
    #httplib.HTTPSConnection.debuglevel = 5
    h = httplib.HTTPSConnection(self.HOST)
    headers = self.set_auth_header()
    selector = "%s/%s/table" % (self.RESTFILE, urllib.quote(filename))
    h.request('GET', selector, '', headers)
    res = h.getresponse()
    if res.status == 200:
      return True
    elif res.status == 404:
      return False
    raise SQLShareUploadError("%s: %s" % (res.status, res.read()))

  """
Get the permissions for a given dataset
  """
  def get_permissions(self,name,schema=None):
    if not schema: schema = self.schema
    selector = "%s/dataset/%s/%s/permissions" % (self.RESTDB2, urllib.quote(schema), urllib.quote(name)) 
    return json.loads(self.poll_selector(selector))

    """
Share table with given users
    """    
  def set_permissions(self, name, is_public=False, is_shared=False, authorized_viewers=[]):
    h = httplib.HTTPSConnection(self.HOST)
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
    if res.status == 200: return 'set'
    else: raise SQLShareError("%s: %s" % (res.status, res.read()))


class SQLShareUploadResponse:
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


def _encode_multipart_formdata_via_chunks(filename, chunk):

    BOUNDARY = '----------ThIs_Is_tHe_bouNdaRY_$'
    CRLF = '\r\n'
    L = []            
    file_size = len(chunk)    
    contenttype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
    L.append('--%s' % BOUNDARY)    
    # L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key,filename))
    L.append('Content-Disposition: form-data; name="file1"; filename="%s"' % filename)
    L.append('Content-Type: %s' % contenttype)    
    L.append('\r\n' + chunk)
    L.append('--' + BOUNDARY + '--')
    L.append('')
    body = CRLF.join(L)
    content_type = 'multipart/form-data; boundary=%s' % BOUNDARY
    return content_type, body

    
    """
Utility for formatting form data
@return: (content_type, body) ready for httplib.HTTP instance

DO NOT ERASE, USED AS MULTIPART REFERENCE
    """
def _encode_multipart_formdata(fields, files):

    BOUNDARY = '----------ThIs_Is_tHe_bouNdaRY_$'
    CRLF = '\r\n'
    L = []
    for (key, value) in fields:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"' % key)
        L.append('')
        L.append(value)
    for (tablename, fd) in files:
        file_size = os.fstat(fd.fileno())[stat.ST_SIZE]
        filename = os.path.basename(fd.name) #fd.name.split('/')[-1]
        #filename = filename.split('\\')[-1]
        contenttype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        L.append('--%s' % BOUNDARY)
        # Can't save the table with a different name
        # L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key,filename))
        L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (tablename,tablename))
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
