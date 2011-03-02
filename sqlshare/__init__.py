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
import sys
import urllib
import time

def debug(m):
  print m



class SQLShareError(ValueError):
  pass

class SQLShareUploadError(SQLShareError):
  pass

class SQLShare:
  HOST = "sqlshare-rest-test.cloudapp.net" #"192.168.3.109"# 
  REST = "/REST.svc"
  RESTFILE = REST + "/v2/file"
  RESTDB = REST + "/v1/db"
  CHUNKSIZE = 10*2**20 # 1MB (test)
  ERROR_NUM = 0

  """
@param host: the DNS URL
@param username: sql share username
@param password: sql share username's password
"""
  def __init__(self,  username, password):
    self.username = username
    self.password = password
    self.schema = self.get_userinfo()["schema"]
 

  def set_auth_header(self, header):
    header['Authorization'] = 'ss_user ' + self.username
    
  def chunksoff(self, f, size):
    print os.path.getsize(f.name)
    lines = f.readlines(size)    
    while lines:
        yield "".join(lines)	 	  
        lines = f.readlines(size)	  
  
	  
  """
Upload a datasheet into Sql 
@param tablename: the tablename that should be created on this datafile.  Defaults to filename.
@param fileobj: file-like object to upload
@param hasHeader: (optional default = true) STRING 'true' or 'false' if the document has a header
@param delimiter: (optional default = tab) the character to be a delimiter, or 'tab' to refer to '\t'
  """
  def post_file(self, filepath, tablename=None, hasHeader='true', delimiter='tab'):
    tableid = tablename or os.path.basename(filename)
    fileobj = file(filepath)
    fields = []
    content_type, body = _encode_multipart_formdata(fields, [(tableid,fileobj)])

    h = httplib.HTTPSConnection(self.HOST)
    headers = {
      'User-Agent': 'python_multipart_caller',
      'Content-Type': content_type,
      'Authorization': 'ss_user ' + self.username
    }

    selector = self.RESTFILE
    h.request('POST', selector, body, headers)

    res = h.getresponse()
    resp_msg = res.read()
    return resp_msg
    
    
  def post_file_chunk(self, filepath, dataset_name, chunk, force_append, force_column_headers):
    tableid = dataset_name or os.path.basename(filename)
    fileobj = file(filepath)
    fields = []
    content_type, body = _encode_multipart_formdata_via_chunks(tableid, chunk)

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
    resp_msg = res.read()
    return resp_msg

  """
Upload multiple files to sqlshare.  Assumes all files have the same format.
@param tablename: the tablename that should be created on this datafile
@param filepath: location to the file to be uploaded
@param hasHeader: (optional default = true) STRING 'true' or 'false' if the document has a header
@param delimiter: (optional default = tab) the character to be a delimiter, or 'tab' to refer to '\t'
  """
  def upload(self, filepath, tablenames=None, hasHeader='true', delimiter='tab'):
    print "uploading %s into %s" % (filepath, tablenames)
    fnames = [fn for fn in glob.glob(filepath)]
    if not tablenames: 
      tablenames = fnames
    pairs = [(fn, t or os.path.basename(fn)) for (fn, t) in zip(fnames,tablenames)]
    # get user info; we need the schema name

    for fn,tn in pairs:
      yield self.uploadone(fn,tn)

  def uploadone(self, fn, dataset_name):
    f = file(fn)
    first_chunk = True
    for chunk in self.chunksoff(f, self.CHUNKSIZE):        
        print 'processing chunk.. %s' % chunk.count('\n')
        if first_chunk:
          self.upload_chunk(fn, dataset_name, chunk)
        else:
          self.upload_chunk(fn, dataset_name, chunk, True, False)
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
      
        

  def poll_selector(self, selector):    
    while True:        
        h = httplib.HTTPSConnection(self.HOST)
        headers = {}
        self.set_auth_header(headers)    
        h.request('GET', selector, '', headers)
        res = h.getresponse()
        if res.status == 200:
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
  
  def get_userinfo(self):
    h = httplib.HTTPSConnection(self.HOST)
    headers = {
        'Authorization': 'ss_user ' + self.username
    }
    selector = '%s/v1/user/%s' % (self.REST, self.username)
    h.request('GET', selector, '', headers)
    res = h.getresponse()
    if res.status > 400:
      raise SQLShareError("%s: %s" % (res.status, res.read()))
    return json.loads(res.read())


  def tableop(self, tableid, operation):
    h = httplib.HTTPSConnection(self.HOST)
    headers = {
        #'Content-Type': content_type,
        'Authorization': 'ss_user ' + self.username
    }
    selector = '%s/%s/%s' % (self.RESTFILE, urllib.quote(tableid), operation)
    h.request('GET', selector, '', headers)
    res = h.getresponse()
    return res

  """
 Get a list of all queries that are available to user 
  """
  def get_all_queries(self):    
    h = httplib.HTTPSConnection(self.HOST)    
    headers = {}
    self.set_auth_header(headers)
    selector = "%s/query" % (self.RESTDB)    
    h.request('GET', selector, '', headers)
    res = h.getresponse()
    if res.status == 200: return json.loads(res.read())
    else: raise SQLShareError("%s: %s" % (res.status, res.read()))	
    
  """
Get meta data about target query, this also includes a cached sampleset of first 200 rows of data
  """
  def get_query(self, schema, query_name):
    h = httplib.HTTPSConnection(self.HOST)    
    headers = {}
    self.set_auth_header(headers)
    selector = "%s/query/%s/%s" % (self.RESTDB, urllib.quote(schema), urllib.quote(query_name))    
    h.request('GET', selector, '', headers)
    res = h.getresponse()
    if res.status == 200: return json.loads(res.read())
    else: raise SQLShareError("%s: %s" % (res.status, res.read()))	       

  """
Save a query
  """    
  def save_query(self, sql, name, description, is_public=False):
    h = httplib.HTTPSConnection(self.HOST)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'ss_user ' + self.username
    }

    queryobj = {
      "description":description,
      "sql_code":sql,
	    "is_public": is_public
    }

    selector = "%s/query/%s/%s" % (self.RESTDB, urllib.quote(self.schema), urllib.quote(name)) 
    h.request('PUT', selector, json.dumps(queryobj), headers)
    res = h.getresponse()
    print res
    if res.status == 200: return 'modified'
    elif res.status == 201: return 'created' 
    else: raise SQLShareError("%s: %s" % (res.status, res.read()))
    
  def delete_query(self, query_name):
    h = httplib.HTTPSConnection(self.HOST)    
    headers = {}
    self.set_auth_header(headers)
    selector = "%s/query/%s/%s" % (self.RESTDB, urllib.quote(self.schema), urllib.quote(query_name))    
    h.request('DELETE', selector, '', headers)       

  def download_sql_result(self, sql):
    h = httplib.HTTPSConnection(self.HOST)       
    headers = {}
    self.set_auth_header(headers)
    selector = "%s/file?sql=%s" % (self.RESTDB, urllib.quote(sql))    
    h.request('GET', selector, '', headers)
    res = h.getresponse()
    if res.status == 200: return res.read()
    else: raise SQLShareError("%s: %s" % (res.status, res.read()))	    
    

  def materialize_table(self, query_name, new_table_name=None, new_query_name=None):
    h = httplib.HTTPSConnection(self.HOST)        
    headers = {}
    self.set_auth_header(headers)
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
    
  def execute_sql(self, sql):
    h = httplib.HTTPSConnection(self.HOST)        
    headers = {}
    self.set_auth_header(headers)
    selector = "%s?sql=%s" % (self.RESTDB, urllib.quote(sql))    
    h.request('GET', selector, '', headers)
    res = h.getresponse()
    if res.status == 202: #accepted
        location = res.getheader('Location')
        return json.loads(self.poll_execute_sql(location))
    else:
        raise SQLShareError("code: %s : %s" % (res.status, res.read()))
    
        
  def get_parser(self, tableid):
    resp = SQLShareUploadResponse(self.tableop(tableid, 'parser'))
    while (not resp.done()):
      if resp.failed():
        raise ValueError("%s: %s" % resp.error)
      resp = SQLShareUploadResponse(self.tableop(tableid, 'parser'))
    return resp.parser    
    
# for the most bizzare reason, having the check table function inside of the put_table1 method
# would freeze upon SSL handshake every single time. But it works when its not being called from
# within that method
  def put_table(self, filename, parser):
    self.put_table1(filename, parser)
    success = self.check_table(filename)
    while not success:
      time.sleep(0.5)
      success = self.check_table(filename)
    return True
      
  def put_table1(self, filename, parser):
   # httplib.HTTPSConnection.debuglevel = 5
    h = httplib.HTTPSConnection(self.HOST)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    #print parser
    self.set_auth_header(headers)
    selector = "%s/%s/table" % (self.RESTFILE, urllib.quote(filename))
    h.request('PUT', selector, json.dumps(parser), headers)    
    res = h.getresponse()
    time.sleep(0.3)    
    if res.status != 200:
      raise SQLShareUploadError("%s: %s" % (res.status, res.read()))     

  def check_table(self, filename):
    #httplib.HTTPSConnection.debuglevel = 5
    h = httplib.HTTPSConnection(self.HOST)
    headers = {}
    self.set_auth_header(headers)
    selector = "%s/%s/table" % (self.RESTFILE, urllib.quote(filename))
    h.request('GET', selector, '', headers)
    res = h.getresponse()
    if res.status == 200:
      return True
    else:
      if res.status >= 400:
        raise SQLShareUploadError("%s: %s" % (res.status, res.read()))
      #debug("%s: %s" % (res.status, res.read()))
      return False

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


