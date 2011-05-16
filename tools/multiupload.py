"""
Upload multiple files to sqlshare
"""

import sys
import sqlshare
from optparse import OptionParser


"""print usage"""
def usage():
  cmd = """python %s [-u <username> -p <password>] <file1> <file2> ... <fileN>""" % __file__
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
  parser = OptionParser(usage="usage: %prog [options] <file1> <file2> ... <fileN>")
  parser.add_option('-u', '--user', dest='username', help='SQLshare user name')
  parser.add_option('-p', '--password', dest='password', help='SQLshare password')

  (options, args) = parser.parse_args()

  if not args:
    parser.error('no input datafile')

  multiupload(args, options.username, options.password)

if __name__ == '__main__':
  main()
