"""
Fetch data from SQLShare using a SQL query
"""
import sys
import sqlshare
import httplib
httplib.HTTPConnection.debuglevel = 1 

"""print usage"""
def usage():
  cmd = """python %s <dataset_name> "<tag1> <tag2> ... <tagn>" [<username>] [<api-key>]""" % __file__
  exmp = """
Example:
python %s billhowe@washington.edu foo "select * from sys.tables" 
""" % __file__
  return cmd + exmp

def settags(dataset, tags, username, password):
  conn = sqlshare.SQLShare(username,password)
  success = conn.set_tags(dataset, tags)
  if success: print "tags %s added to dataset %s" % (tags, dataset)

def main():
  if len(sys.argv) < 2:
    print usage()
  else:
    dataset = sys.argv[1]
    tags = sys.argv[2]
    username, password = (sys.argv + [None, None])[3:5]
    settags(dataset, tags.split(), username, password)

if __name__ == '__main__':
  main()

