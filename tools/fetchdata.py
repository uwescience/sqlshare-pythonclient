#! /usr/bin/env python
# vim: tabstop=2 softtabstop=2 shiftwidth=2 expandtab
"""
Fetch data from SQLShare using a SQL query
"""

# Check Python version first.
import sys
if sys.version_info < (2,7):
  raise NotImplementedError("fetchdata.py requires Python 2.7 or later.")

import argparse
import httplib
import sqlshare

# Debugging
httplib.HTTPConnection.debuglevel = 0 

"""print usage"""
def usage():
  cmd = """python %s <sql> [<username>] [<api-key>]""" % __file__
  exmp = """
Example:
python %s billhowe@washington.edu foo "select * from sys.tables" 
""" % __file__
  return cmd + exmp

def fetchdata(sql, format, output):
  conn = sqlshare.SQLShare()
  return conn.download_sql_result(sql, format, output)

def get_parser():
  """Build the parser for the arguments to this program."""
  parser = argparse.ArgumentParser(description='Download a dataset or query from SQLShare.')

  # This class implements the Action API for the -d option, which stores a
  #   simple SELECT * FROM dataset query in the 'sql' variable.
  # (See: http://docs.python.org/2/library/argparse.html#action)
  class DatasetAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
      setattr(namespace, self.dest, 'SELECT * FROM %s' % values)

  # This group holds the various ways to specify what to download
  download_group = parser.add_mutually_exclusive_group(required=True)
  download_group.add_argument('--dataset', '-d',
      help='The name of the dataset to be downloaded.',
      action=DatasetAction,
      dest='sql',
      default=None)
  download_group.add_argument('--sql', '-s',
      help='The SQL query, the answer to which will be downloaded.',
      default=None)

  # The format of the downloaded data
  parser.add_argument('--format', '-f',
      help='The format in which the data will be downloaded (default: %(default)s).',
      default='csv',
      choices=['tsv', 'csv'])
  # The output filename
  parser.add_argument('--output', '-o',
      help='Where to save the downloaded file.',
      type=argparse.FileType('w'),
      required=False)
  return parser

def main():
  # Get the parser, parse the arguments
  parser = get_parser()
  args = parser.parse_args()
  # Fetch the data
  data = fetchdata(args.sql, args.format, args.output)
  if args.output:
    args.output.close()


if __name__ == '__main__':
  main()

