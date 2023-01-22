
from zipfile import ZipFile
import os

def extract_zip():  
    file_name = "sql.zip"
    
    with ZipFile(file_name, 'r') as zip:
        print('Extracting all the files now...')
        zip.extractall()
        print('Done!')

startpath = 'sql'

def list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        dirs.sort()
        folder = str(root)
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
#        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        files.sort()
        for f in files:
#            print('{}{}'.format(subindent, f))
            if str(f).endswith('.sql'):
                abs_path = os.path.abspath(folder + '/' + f)
                print(abs_path)
                sql_file = open(abs_path)
                to_execute = sql_file.read()
                sql_file.close()
                

extract_zip()
list_files(startpath)