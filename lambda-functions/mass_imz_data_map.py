import json
import os
import time
import datetime as dt
import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
# import pandas as pd
# import awswrangler as wr

	
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
		"type": os.environ.get("type"),
		"project_id": os.environ.get("project_id"),
		"private_key_id": os.environ.get("private_key_id"),
		"private_key": os.environ.get("private_key").replace('\\n','\n'),
		"client_email": os.environ.get("client_email"),
		"client_id": os.environ.get("client_id"),
		"auth_uri": os.environ.get("auth_uri"),
		"token_uri": os.environ.get("token_uri"),
		"auth_provider_x509_cert_url": os.environ.get("auth_provider_x509_cert_url"),
		"client_x509_cert_url": os.environ.get("client_x509_cert_url")
	}
	return variables_keys
def lambda_handler(event, context):
	try:
		
		
		# use creds to create a client to interact with the Google Drive API
		scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
		keys = create_keyfile_dict()
		#print(keys)
		creds = ServiceAccountCredentials.from_json_keyfile_dict(keys, scope)
		client = gspread.authorize(creds)
		# Find a workbook by name and open the first sheet
		sheet = client.open(os.environ.get("sheet_name")).sheet1
		# Extract and print all of the values
		resultsList = sheet.get_all_values()
		#print(resultsList)
		with open("/tmp/Community Vaccinator (Responses).csv", "w", newline="") as f:
			writer = csv.writer(f,delimiter =',')
			writer.writerows(resultsList)
	
		client = boto3.client('s3')
		client.upload_file('/tmp/Community Vaccinator (Responses).csv', os.environ.get("s3_bucket_name"),os.environ.get("csv_file_path"))
  
		# Copy to Internal Table
		date = dt.date.today()
		year, month, day = map(str, date.strftime("%Y %m %d").split())
		s3_ouput = 's3://aws-athena-query-results-627284573236-us-west-2/Unsaved/' + year + '/' + month
		database = 'talent'
		table = 'mass_imz_map_data'
		drop_table = "drop table {0}.{1}".format(database, table);
		create_table = \
		"""CREATE EXTERNAL TABLE IF NOT EXISTS talent.mass_imz_map_data (
		`timestamp` string,
		`company_name` string,
		`description` string,
		`contact_name` string,
		`contact_email` string,
		`contact_phone_number` string,
		`service_area` string 
		)
		ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
		WITH SERDEPROPERTIES (
		'serialization.format' = ',',
		'field.delim' = ','
		) LOCATION 's3://mass-imz-extracts/'
		TBLPROPERTIES ('has_encrypted_data'='false');"""
		
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
