"""
Upload multiple files to sqlshare
"""

import sys
import sqlshare

"""print usage"""
def usage():
  cmd = """python %s <username> <password> <sql> <name> <description>""" % __file__
  exmp = """
Example:
python %s billhowe@washington.edu foo "select * from sys.tables" "alltables"
""" % __file__
  return cmd + exmp

def savequery(sql,name, description,username, password):
  conn = sqlshare.SQLShare(username,password)
  info = conn.get_userinfo()
  schema = info["schema"]
  conn.save_query(sql,schema,name, description)

def main():
  if len(sys.argv) < 4:
    print usage()
  else:
    username, password = sys.argv[1:3]
    sql, name, description = sys.argv[3:]
    savequery(sql, name, description, username, password)

if __name__ == '__main__':
  main()
