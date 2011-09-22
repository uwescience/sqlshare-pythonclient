"""
Fetch data from SQLShare using a SQL query
"""
import sys
import sqlshare
import httplib
httplib.HTTPConnection.debuglevel = 1 

"""print usage"""
def usage():
  cmd = """python %s <dataset_name> [<username>] [<api-key>]""" % __file__
  exmp = """
Example:
python %s billhowe@washington.edu foo "select * from sys.tables" 
""" % __file__
  return cmd + exmp

def gettags(dataset, username, password):
  conn = sqlshare.SQLShare(username,password)
  info = conn.get_userinfo()
  schema = info["schema"]
  print conn.get_tags(dataset, schema=schema)

def main():
  if len(sys.argv) < 2:
    print usage()
  else:
    dataset = sys.argv[1]
    username, password = (sys.argv + [None, None])[2:4]
    gettags(dataset, username, password)

if __name__ == '__main__':
  main()

