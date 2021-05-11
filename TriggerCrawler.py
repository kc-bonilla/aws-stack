import json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import boto3
import os
import logging
import sys
import traceback

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('glue')
glue = boto3.client(service_name = 'glue', region_name = 'us-east-2',
              endpoint_url = 'https://glue.us-east-2.amazonaws.com')

client_sf = boto3.client('stepfunctions')

def lambda_handler(event, context):
    
    context_info = {"lambda id" : context.aws_request_id, "lambda name" : context.function_name}
    logger.info('context_info: [{context_info}]'.format(context_info=context_info))
    
    logger.info("Starting Glue Crawler")
    
    class CrawlerException(Exception):
        pass
    
    try:
        crawlerInput = event['crawler']
        response = client.start_crawler(Name = crawlerInput)
        
    except Exception as e:
        # send activity failure token
        activity = event['activity']
        task = client_sf.get_activity_task(activityArn = activity, workerName = crawlerInput)
        response = client_sf.send_task_failure(taskToken = task['taskToken'])
        logger.info('Problem while invoking crawler')
        
        exception_type, exception_value, exception_traceback = sys.exc_info()
        traceback_string = traceback.format_exception(exception_type, exception_value, exception_traceback)
        err_msg = json.dumps({
            "errorType": exception_type.__name__,
            "errorMessage": str(exception_value),
            "stackTrace": traceback_string
        })
        
        logger.error(err_msg)
       
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    logger.info('## EVENT')
    logger.info(event)
