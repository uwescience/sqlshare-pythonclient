"""
Upload multiple files to sqlshare
"""

import sys
import sqlshare


"""print usage"""
def usage():
  cmd = """python %s <username> <password> <file1> <file2> ... <fileN>""" % __file__
  exmp = """
Example:
python %s armbrustlab <password> *.txt
""" % __file__
  return cmd + exmp

"""Upload multiple files matched by a glob pattern"""
def multiupload(exprs,username, password):
  conn = sqlshare.SQLShare(username,password)
  for globexpr in exprs:
    print "uploading %s" % globexpr
    for response in conn.upload(globexpr):
      print "Successfully uploaded " + response

def main():
  if len(sys.argv) < 4:
    print usage()
  else:
    username, password = sys.argv[1:3]
    exprs = sys.argv[3:]
    multiupload(exprs, username, password)

if __name__ == '__main__':
  main()
