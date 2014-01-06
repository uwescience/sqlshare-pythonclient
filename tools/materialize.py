"""
Upload multiple files to sqlshare
"""

import sys
import sqlshare
import time
import os


"""print usage"""
def usage():
    cmd = """python %s <username> <password> <query_name> [new_query_name] [table_name]""" % __file__
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
        query_name = sys.argv[3]
        new_query_name = None
        table_name = None
        if len(sys.argv) > 4: 
            new_query_name = sys.argv[4]      
            
        if len(sys.argv) > 5: 
            table_name = sys.argv[5]
          
        print query_name
        print new_query_name
        print table_name
     
        conn = sqlshare.SQLShare(username,password)
        print conn.materialize_table(query_name, table_name, new_query_name)
    
if __name__ == '__main__':
    main()
