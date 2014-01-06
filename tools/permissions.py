"""
Change permissions on a dataset
"""

import sqlshare
from optparse import OptionParser


def main():
    parser = OptionParser(usage="usage: %prog [options] (add|remove|print|public|private) <user1> <user2> ... <userN>")
    parser.add_option('-u', '--user', dest='username', help='SQLshare user name')
    parser.add_option('-p', '--password', dest='password', help='SQLshare password')
    parser.add_option('-t', '--table', dest='tables', help='SQLshare dataset', action='append')

    (options, args) = parser.parse_args()

    if not options.tables:
        parser.error('can not find datasets')

    if not args:
        parser.error('command not found')

    cmd = args[0]

    if cmd not in ('add', 'print', 'public', 'private', 'remove'):
        parser.error('unknown command: '+cmd)

    if cmd == 'add' or cmd == 'remove':
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
            continue
        if cmd == 'add':
            viewers = list(set(perms[table]['authorized_viewers']) | users)
            perms[table]['authorized_viewers'] = viewers
        elif cmd == 'remove':
            viewers = list(set(perms[table]['authorized_viewers']) - users)
            perms[table]['authorized_viewers'] = viewers
            conn.set_permissions(table, **perms[table])
        elif cmd == 'public':
            perms[table]['is_public'] = True
        elif cmd == 'private':
            perms[table]['is_public'] = False
        conn.set_permissions(table, **perms[table])

if __name__ == '__main__':
    main()
