"""
append multiple files to given sqlshare dataset
"""

import sys
import sqlshare
import itertools
import glob
from optparse import OptionParser

def main():
  parser = OptionParser(usage="usage: %prog [options] datasetName <file1> <file2> ... <fileN>")
  parser.add_option('-u', '--user', dest='username', help='SQLshare user name')
  parser.add_option('-p', '--password', dest='password', help='SQLshare password')

  (options, args) = parser.parse_args()

  if len(args) < 2:
      parser.error('not enough number of arguments')

  datasetName = args[0]

  files = []
  for globPattern in args[1:]:
      files.extend( glob.glob(globPattern) )

  conn = sqlshare.SQLShare(options.username, options.password)

  # check whether the dataset already exists or not
  datasetExist = conn.table_exists(datasetName)
  if not datasetExist:
      print "The dataset [%s].[%s] does not exist in SQLShare. Please create it first." % (conn.username, datasetName)
      sys.exit(1)

  for fn in files:
      # dataset already exists. force append always
      print "uploading %s and append to %s" % ( fn, datasetName )
      conn.uploadone(fn,datasetName, True)

if __name__ == '__main__':
  main()
