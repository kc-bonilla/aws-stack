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
glue = boto3.client(service_name = 'glue', region_name = 'us-east-2', endpoint_url = 'https://glue.us-east-2.amazonaws.com')

def lambda_handler(event, context):

    inputJob = event['JobName']
    try:
        client.start_job_run(JobName = inputJob)

    # inputJob = "shootproof_ref_order_lab_status_extract"
    context_info = {"lambda id" : context.aws_request_id, "lambda name" : context.function_name}
    logger.info('context_info')
    
    except Exception as e:
        exception_type, exception_value, exception_traceback = sys.exc_info()
        traceback_string = traceback.format_exception(exception_type, exception_value, exception_traceback)
        err_msg = json.dumps({
            "errorType": exception_type.__name__,
            "errorMessage": str(exception_value),
            "stackTrace": traceback_string})
        
        logger.error(err_msg)
        logger.info(lambda_id)

    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    logger.info('## EVENT')
    logger.info(event)
    
    # return (json.dumps(context_info))
