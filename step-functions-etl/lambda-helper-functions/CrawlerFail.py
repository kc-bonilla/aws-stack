import os
import boto3
import json
import re
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# step-fn-activity
client_sf = boto3.client('stepfunctions')

def lambda_handler(event, context):
    activity = event['activity']
    crawlerInput = event['crawler']
    
    task = client_sf.get_activity_task(activityArn=activity, workerName = crawlerInput)
    response = client_sf.send_task_failure(taskToken=task['taskToken'])
    
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    logger.info('## EVENT')
    logger.info(event)
