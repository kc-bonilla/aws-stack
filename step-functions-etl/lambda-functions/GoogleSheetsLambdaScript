import json
import csv
import os
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

import boto3
import time
#Function for executing athena queries
def run_query(query, database, s3_output):
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
            },
        ResultConfiguration={
            'OutputLocation': s3_output,
            }
        )
    print('Execution ID: ' + response['QueryExecutionId'])
    

    return response['QueryExecutionId']

def checkQuery_status(QueryExecutionId):
    client = boto3.client('athena')
    response = client.get_query_execution(
    QueryExecutionId=QueryExecutionId
    )
    #print(response)
    return response

def get_Results(QueryExecutionId):
    client = boto3.client('athena')
    response = client.get_query_results(
    QueryExecutionId=QueryExecutionId,
    MaxResults=123
    )
    return response


def create_keyfile_dict():
	
	variables_keys = {
		"type": os.environ.get("TYPE"),
		"project_id": os.environ.get("PROJECT_ID"),
		"private_key_id": os.environ.get("PRIVATE_KEY_ID"),
		"private_key": os.environ.get("PRIVATE_KEY").replace('\\n','\n'),
		"client_email": os.environ.get("CLIENT_EMAIL"),
		"client_id": os.environ.get("CLIENT_ID"),
		"auth_uri": os.environ.get("AUTH_URI"),
		"token_uri": os.environ.get("TOKEN_URI"),
		"auth_provider_x509_cert_url": os.environ.get("AUTH_PROVIDER_X509_CERT_URL"),
		"client_x509_cert_url": os.environ.get("CLIENT_X509_CERT_URL")
	}
	return variables_keys



def lambda_handler(event, context):
	try:
		
		
		scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
		# use creds to create a client to interact with the Google Drive API
		keys = create_keyfile_dict()
		#print(keys)
		creds = ServiceAccountCredentials.from_json_keyfile_dict(keys, scope)
		client = gspread.authorize(creds)
		# Find a workbook by name and open the first sheet
		# Make sure you use the right name here.  
		sheet = client.open(os.environ.get("SHEET_NAME")).sheet1
		# Extract and print all of the values
		resultsList = sheet.get_all_values()
		#print(resultsList)
		with open("/tmp/Talent.csv", "w", newline="") as f:
			writer = csv.writer(f,delimiter ='|')
			writer.writerows(resultsList)
	
		client = boto3.client('s3')
		client.upload_file('/tmp/Talent.csv', os.environ.get("S3_BUCKET_NAME"),os.environ.get("CSV_FILE_PATH"))
  
		# Copy to Internal Table
		s3_ouput = 's3://aws-athena-query-results-627284573236-us-west-2/Unsaved/2019/09'
		database = 'talent'
		table = 'stc_talent_info'
		drop_table="drop table talent.stc_talent_info"
		create_table = \
	 	"""CREATE TABLE talent.stc_talent_info WITH ( format = 'Parquet', parquet_compression = 'SNAPPY') AS SELECT distinct event_date, section_type, stat, total_count, total_value
		FROM talent.stc_talent;""" 

		#Query definitions
		query_1 = "SELECT count(1) FROM %s.%s ;" % (database, table)

		#Execute all queries
		queries = [drop_table,create_table, query_1]
		for q in queries:
			print("Executing query: %s" % (q))
			res = run_query(q, database, s3_ouput)
   
			flag='none'
			while(flag!='SUCCEEDED'):
				response=checkQuery_status(res)
				flag=response['QueryExecution']['Status']['State']
				if(flag=='FAILED'):
					print("Status of Query failed")
					break
				if(flag=='Running'):
					print("Running")
					time.sleep(5)
   
			if flag=='SUCCEEDED':
				print("status=",flag)
	   
		print("Number Of Records Inserted = ",get_Results(res)['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])       

		
	
		
	 
	 
	 
	except Exception as err:
		print("Error happened: {}".format(err))
		#print(keys)
		
	finally:
		return {'statusCode': 200,'body': json.dumps("Function Execution Completed")}
