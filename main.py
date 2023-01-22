
from zipfile import ZipFile
import os
import mysql.connector

def extract_zip(file_name):  
   
    with ZipFile(file_name, 'r') as zip:
        print('Extracting all the files now...')
        zip.extractall()
        print('Done!')

def list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        dirs.sort()
        folder = str(root)
        level = root.replace(startpath, '').count(os.sep)
        #indent = ' ' * 4 * (level)
        #print('{}{}/'.format(indent, os.path.basename(root)))
        #subindent = ' ' * 4 * (level + 1)
        files.sort()
        for f in files:
            #print('{}{}'.format(subindent, f))
            if str(f).endswith('.sql'):
                abs_path = os.path.abspath(folder + '/' + f)
                print(abs_path)
                sql_exec(abs_path)


def sql_exec(file):

    try:
        
        host_args = {
            "host": os.environ["HOST"],
            "user": os.environ["USER"],
            "password": os.environ["PASSWORD"],
            "database": os.environ["DATABASE"]
        }

        con = mysql.connector.connect(**host_args)

        cur = con.cursor(dictionary=True)
    except:
        print("Can't connect to MySql")

    try:
        with open(file, 'r') as sql_file:
            result_iterator = cur.execute(sql_file.read(), multi=True)
            for res in result_iterator:
                print("Running query: ") 
                print(f"Affected {res.rowcount} rows" )

            con.commit()
            
    except mysql.connector.Error as err:
        print(err)


if __name__ == "__main__":
    
    file_name = 'sql.zip'
    startpath = 'sql'

    extract_zip(file_name)
    list_files(startpath)