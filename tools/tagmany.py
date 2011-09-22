"""
Fetch data from SQLShare using a SQL query
"""
import sys
import sqlshare
import httplib
httplib.HTTPConnection.debuglevel = 1 

"""print usage"""
def usage():
  cmd = """python %s <dataset_pattern> "<tag1> <tag2> ... <tagn>" [<username>] [<api-key>]""" % __file__
  exmp = """
Example:
python %s "For Share%.txt" "biomed HT_screening_result" billhowe@washington.edu foo
""" % __file__
  return cmd + exmp

def settags(dataset_pattern, tags, username, password):
  conn = sqlshare.SQLShare(username,password)
  tbls = conn.download_sql_result("select name from sys.views where name like '%%%s%%'" % dataset_pattern)
  #tbls = conn.execute_sql("select * from sys.tables")
  for dataset in tbls.split("\n")[1:-1]:
    success = conn.set_tags(dataset.strip(), tags)
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

