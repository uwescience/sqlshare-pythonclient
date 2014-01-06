#! /usr/bin/env python
"""
append multiple files to given sqlshare dataset
"""

import sqlshare
import itertools
import glob
from optparse import OptionParser

def main():
    "Append multiple files to given sqlshare dataset"
    parser = OptionParser(usage="usage: %prog [options] dataset_name <file1> <file2> ... <fileN>")
    parser.add_option('-u', '--user', dest='username',
            help='SQLshare user name')
    parser.add_option('-p', '--password', dest='password',
            help='SQLshare password')

    (options, args) = parser.parse_args()

    if len(args) < 2:
        parser.error('not enough number of arguments')

    dataset_name = args[0]

    files = []
    for glob_pattern in args[1:]:
        files.extend(glob.glob(glob_pattern))

    conn = sqlshare.SQLShare(options.username, options.password)

    # check whether the dataset already exists or not
    start = 0
    if not conn.table_exists(dataset_name):
        # upload the first file and create the dataset
        print "creating dataset %s and uploading %s" % (dataset_name, files[0])
        conn.uploadone(files[0], dataset_name)
        start = 1

    for filename in itertools.islice(files, start, None):
        # dataset already exists. force append always
        print "uploading %s and append to %s" % (filename, dataset_name)
        conn.uploadone(filename, dataset_name, True)

if __name__ == '__main__':
    main()
