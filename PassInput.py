import json
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):

 bucket = 'datapipeline-analytics-shootproof-com'
 key = 'step-functions/scripts/SF_Input_Items.json'

 data = s3.get_object(Bucket=bucket, Key=key)
 json_data = data['Body'].read().decode('utf-8')
 
 return json.loads(json_data)