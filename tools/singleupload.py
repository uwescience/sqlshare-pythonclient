"""
Upload multiple files to sqlshare
"""

import sys
import sqlshare


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
    if len(sys.argv) > 3: 
      datasetname = sys.argv[4]
    else: 
      datasetname = filename
 
    conn = sqlshare.SQLShare(username,password)
    info = conn.get_userinfo()
    schema = info["schema"]
    conn.uploadone(filename, datasetname, schema)

if __name__ == '__main__':
  main()
