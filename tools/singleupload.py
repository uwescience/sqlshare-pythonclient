#!/usr/bin/env python
"""
Upload a single file to sqlshare.  Allow renaming and chunking.
"""

import os
from optparse import OptionParser

import sqlshare

def main():
    """
    Upload a single file to sqlshare.  Allow renaming and chunking.
    """
    parser = OptionParser(usage="usage: %prog [options] <filename>")
    parser.add_option('-u', '--user', dest='username',
            help='SQLshare user name')
    parser.add_option('-p', '--password', dest='password',
            help='SQLshare password')
    parser.add_option('-d', '--datasetname', dest='datasetname',
            help='Dataset name (defaults to filename if not supplied')

    (options, args) = parser.parse_args()

    if not args:
        parser.error('no input datafile')

    conn = sqlshare.SQLShare(options.username, options.password)

    if not options.datasetname:
        datasetname = os.path.basename(args[0])
    else:
        datasetname = options.datasetname

    conn.uploadone(args[0], datasetname)

if __name__ == '__main__':
    main()
