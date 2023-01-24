import json
import os
import boto3
import botocore
from zipfile import ZipFile
import mysql.connector

def lambda_handler(event, context):

    try:
        print('Starting lambda processing...')

        db_args = {
            "host": event['dbhost'],
            "user": event['dbuser'],
            "password": event['password'],
            "database": event['database']
        }

        download_file(event)
        os.chdir('/tmp')
        extract_zip()
        process_files(db_args)
        print('End lambda processing')

    except:
        raise

    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }

def download_file(event):

    try:
        print('Connecting with S3...')
        
        BUCKET_NAME = event['bucket']
        KEY = 'sql.zip'

        s3 = boto3.client('s3')
        print('Connected with S3')

    except botocore.exceptions.ClientError as e: 
        print('Error connecting with S3: ', e)
        raise

    try:
        print('Getting File from S3...')
        s3.download_file(Filename='/tmp/sql.zip',Bucket=BUCKET_NAME,Key=KEY)
        print('File downloaded from S3')

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist in S3")
        else:
            print("Error getting file: ",e)
            raise


def extract_zip():  
    file_name = 'sql.zip'
    try:
        with ZipFile(file_name, 'r') as zip:
            print('Extracting all the files...')
            zip.extractall()
            print('Done extracting files.')
    except:
        print('Failed to extract file')
        raise

def process_files(db_args):
    startpath = 'sql'
    try:
        print('Processing files...')
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
                    sql_exec(abs_path,db_args)
                    print('File processed.')
    except:
        print('Failed to process files')

def sql_exec(file,db_args):

    try:        
        print('Connecting with DB...')
        host_args = db_args
        con = mysql.connector.connect(**host_args)
        cur = con.cursor(dictionary=True)
        print('Connected to DB')

    except Exception as e:
        print("Database connection failed due to {}".format(e))

    try:
        with open(file, 'r') as sql_file:
            print('Executing query...')
            result_iterator = cur.execute(sql_file.read(), multi=True)
            for res in result_iterator:
                print(f"Affected {res.rowcount} rows" )

            con.commit()
            print('Query executed.')
            
    except mysql.connector.Error as err:
        print(err)


if __name__ == "__main__":

    event = {
           "file": "sql.zip",
           "bucket": "sully-infra-artifacts",
           "dbhost": "localhost",
           "database" : "proyectoSully_test",
           "dbuser" : "root",
           "password" : "secret123"
        }

    lambda_handler(event,'test')