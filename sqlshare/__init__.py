"""
Python interface to SQLShare

G. Cole and B. Howe
"""
import os.path, tempfile
import glob
import stat, mimetypes
import httplib
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
  HOST = "sqlshare-rest-test.cloudapp.net" #"sqlshare-test.cloudapp.net"
  REST = "/REST.svc/v1"
  RESTFILE = REST + "/file"
  RESTDB = REST + "/db"

  """
@param host: the DNS URL
@param username: sql share username
@param password: sql share username's password
"""
  def __init__(self,  username, password):
    self.username = username
    self.password = password
    self.schema = self.get_userinfo()["schema"]
 
    
  
 
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
    info = self.get_userinfo()
    schema = info["schema"]

    for fn,tn in pairs:
      yield self.uploadone(fn,tn,schema)


  def uploadone(self, fn, tn, schema):
      
      print "pushing %s..." % fn
      # step 1: push file
      quotedtableid = self.post_file(fn, tn) 
      datasetname = quotedtableid[1:-1]
      #datasetname = 'xauthors.csv'
      tablename = "table_%s" % datasetname
   
      print "parsing %s..." % datasetname
      # step 2: get parse information
      parser = self.get_parser(datasetname)
       
      # step 2.5: optionally change parameters
      parser['table']['name'] = tablename

      print "putting %s..." % tablename
      # step 3: parse and insert to SQL
      # polls to verify success
      self.put_table(datasetname, parser)
  
      print "saving query %s..." % datasetname
      # step 4: create view
      q = "SELECT * FROM [%s]" % tablename
      self.save_query(q, schema, datasetname, "Uploaded using %s from '%s'" % (__file__,fn))

      print "finished %s" % datasetname
      return datasetname

  def get_userinfo(self):
    h = httplib.HTTPSConnection(self.HOST)
    headers = {
        'Authorization': 'ss_user ' + self.username
    }
    selector = '%s/user/%s' % (self.REST, self.username)
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

  def save_query(self, sql, schema, name, description):
    h = httplib.HTTPSConnection(self.HOST)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'ss_user ' + self.username
    }

    queryobj = {
      "description":"",
      "sql_code":sql,
	    "is_public": False
    }

    selector = "%s/query/%s/%s" % (self.RESTDB, schema, name) 
    h.request('PUT', selector, json.dumps(queryobj), headers)
    res = h.getresponse()
    if res.status == 200: return 'modified'
    elif res.status == 201: return 'created' 
    else: raise SQLShareError("%s: %s" % (res.status, res.read()))

  def get_parser(self, tableid):
    resp = SQLShareUploadResponse(self.tableop(tableid, 'parser'))
    while (not resp.done()):
      if resp.failed():
        raise ValueError("%s: %s" % resp.error)
      resp = SQLShareUploadResponse(self.tableop(tableid, 'parser'))
    return resp.parser

  def put_table(self, filename, parser):
    h = httplib.HTTPSConnection(self.HOST)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'ss_user ' + self.username,
        'Accept': 'application/json'
    }
    selector = "%s/%s/table" % (self.RESTFILE, urllib.quote(filename))
    h.request('PUT', selector, json.dumps(parser), headers)
    res = h.getresponse()
    if res.status != 200:
      raise SQLShareUploadError("%s: %s" % (res.status, res.read()))

    success = self.check_table(filename)
    while not success:
      success = self.check_table(filename)
    return True  

  def check_table(self, filename):
    h = httplib.HTTPSConnection(self.HOST)
    headers = {
        'Authorization': 'ss_user ' + self.username,
    }
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


    """
Utility for formatting form data
@return: (content_type, body) ready for httplib.HTTP instance
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
    for (key, fd) in files:
        file_size = os.fstat(fd.fileno())[stat.ST_SIZE]
        filename = os.path.basename(fd.name) #fd.name.split('/')[-1]
        #filename = filename.split('\\')[-1]
        contenttype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        L.append('--%s' % BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key,key))
        L.append('Content-Type: %s' % contenttype)
        fd.seek(0)
        L.append('\r\n' + fd.read())
    L.append('--' + BOUNDARY + '--')
    L.append('')
    body = CRLF.join(L)
    content_type = 'multipart/form-data; boundary=%s' % BOUNDARY
    return content_type, body


