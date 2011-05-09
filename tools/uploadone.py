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
    #x = {u'table': {u'description': None, u'row_count': 9999, u'is_public': False, u'schema': u'gbc3', u'options': {u'on_name_conflict': u'overwrite', u'on_name_conflict_type_mismatch': u'fail'}, u'columns': [{u'max_length': 20, u'type': 3, u'name': u'database'}, {u'max_length': 7, u'type': 3, u'name': u'gene'}, {u'max_length': -1, u'type': 2, u'name': u'size'}, {u'max_length': 7, u'type': 3, u'name': u'site'}, {u'max_length': 14, u'type': 3, u'name': u'read'}, {u'max_length': 8, u'type': 3, u'name': u'orf_coordinate'}, {u'max_length': 24, u'type': 3, u'name': u'attempted_classification'}, {u'max_length': 24, u'type': 3, u'name': u'actual_classification'}, {u'max_length': 6, u'type': 3, u'name': u'tax_id'}, {u'max_length': -1, u'type': 1, u'name': u'pp'}], u'name': u'table_test_append_1'}, u'delimiter': u',', u'first_row_line_num': 0, u'has_column_headers': True}
    #conn.put_table('test_append_1', x)    

if __name__ == '__main__':
  main()
