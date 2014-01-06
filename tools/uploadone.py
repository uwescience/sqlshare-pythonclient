"""
Upload multiple files to sqlshare
"""

import sys
import sqlshare
import time
import os


"""print usage"""
def usage():
    cmd = """python %s <username> <password> <file1> <tablename>""" % __file__
    exmp = """
  Example:
  python %s armbrustlab <password> 
  """ % __file__
    return cmd + exmp
  
def main():
    if len(sys.argv) < 4:
        print usage()
    else:
        username, password = sys.argv[1:3]
        filename = sys.argv[3]
        print filename
        if len(sys.argv) > 4: 
            datasetname = sys.argv[4]
        else: 
            datasetname = os.path.basename(filename)    
       
        conn = sqlshare.SQLShare(username,password)
        conn.uploadone(filename, datasetname)
    
if __name__ == '__main__':
    main()
