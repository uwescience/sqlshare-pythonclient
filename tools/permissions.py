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
  parser = OptionParser(usage="usage: %prog [options] (add|remove|print) <user1> <user2> ... <userN>")
  parser.add_option('-u', '--user', dest='username', help='SQLshare user name')
  parser.add_option('-p', '--password', dest='password', help='SQLshare password')
  parser.add_option('-t', '--table', dest='tables', help='SQLshare dataset', action='append')

  (options, args) = parser.parse_args()

  if not options.tables:
    parser.error('can not find datasets')

  if not args:
    parser.error('command not found')

  cmd = args[0]

  if cmd not in ( 'add', 'print', 'remove'):
      parser.error('unknonwn command: '+cmd)

  if cmd != 'print':
      users = args[1:]
      if not users:
          parser.error('user list is required for %s command' % cmd)
      users = set(users)

  conn = sqlshare.SQLShare(options.username, options.password)

  perms = {}
  for table in options.tables:
    perms[table] = conn.get_permissions(table)
    if cmd == 'print':
      print perms[table]
    elif cmd == 'add':
      viewers = list(set(perms[table]['authorized_viewers']) | users)
      perms[table]['authorized_viewers'] = viewers
#     print perms[table]
      res = conn.set_permissions(table,**perms[table])
#      print res
    elif cmd == 'remove':
      viewers = list(set(perms[table]['authorized_viewers']) - users)
      perms[table]['authorized_viewers'] = viewers
      res = conn.set_permissions(table,**perms[table])

if __name__ == '__main__':
  main()
