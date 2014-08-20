#!/usr/bin/env python
"""
Upload multiple files to sqlshare
"""

import sqlshare
from optparse import OptionParser


def multiupload(exprs, username, password):
    """Upload multiple files matched by a glob pattern"""
    conn = sqlshare.SQLShare(username, password)
    for globexpr in exprs:
        print "uploading {dataset}".format(dataset=globexpr)
        for response in conn.upload(globexpr):
            print response

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
