import httplib
import urllib

# This script issues an SQL query directly against the 
# REST API, bypassing the python client interface 

# This script serves two purposes:
# 1) It demonstrates how the python API works under the sheets
# 2) It can be used to test the REST server when debugging problems

h = httplib.HTTPSConnection("rest.sqlshare.escience.washington.edu")

headers = {}
# format is ss_apikey <username> : <apikey>
# To generate an API Key for your user account, go to 
# https://sqlshare.escience.washington.edu/sqlshare/#s=credentials
# This account is a shared account used as an example.
headers["Authorization"] = "ss_apikey sqlshare@uw.edu : cc4f77d4afb8dfb0462aa6c008771765"

# Construct the url from the SQL query
sql = "SELECT * FROM periodic_table"
selector = "/REST.svc/v1/db/file?sql=%s" % (urllib.quote(sql),)

# Issue the call
h.request('GET', selector, '', headers)

# Interpret the resonse
res = h.getresponse()

if res.status == 200: 
  # Print the result
  print res.read()
  print "Test passed!"
else:
  raise Exception("%s: %s" % (res.status, res.read()))

