# checks the status of the crawler that was invoked by the previous Lambda function

import json
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import sys
import logging
import traceback

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('glue')
glue = boto3.client(service_name='glue', region_name='us-east-2',
              endpoint_url='https://glue.us-east-2.amazonaws.com')

client_sf = boto3.client('stepfunctions')

def lambda_handler(event, context):
    
    context_info = {"lambda id" : context.aws_request_id, "lambda name" : context.function_name}
    logger.info('context_info: [{context_info}]'.format(context_info=context_info))
    crawlerInput = event['crawler']
    activity = event['activity']
    
    class CrawlerException(Exception):
        pass
    
    response = client.get_crawler_metrics(CrawlerNameList = [event['crawler']])

    logger.info('response: [{response}]'.format(response=response))
    
    crawler_name = response['CrawlerMetricsList'][0]['CrawlerName']
    crawler_time_left = response['CrawlerMetricsList'][0]['TimeLeftSeconds']
    crawler_estimating = response['CrawlerMetricsList'][0]['StillEstimating']
    
    logger.info('crawler_name: [{crawler_name}]'.format(crawler_name=crawler_name))
    logger.info('crawler_time_left: [{crawler_time_left}]'.format(crawler_time_left=crawler_time_left))
    logger.info('crawler_estimating: [{crawler_estimating}]'.format(crawler_estimating=crawler_estimating))
    
    if (crawler_estimating):
        raise CrawlerException('Crawler Still Estimating')
    elif (crawler_time_left > 0):
        raise CrawlerException('Crawler In Progress, ' + str(crawler_time_left) + ' seconds remaining')
    else :
        #send activity success token
        task = client_sf.get_activity_task(activityArn = activity, workerName = event['crawler'])
        response = client_sf.send_task_success(taskToken=task['taskToken'], output=json.dumps({'message':'Crawler Completed'}))
