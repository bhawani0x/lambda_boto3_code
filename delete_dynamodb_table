import time
import json
import boto3
def lambda_handler(event, context):
#delete DYNAMODB TABLE#
# Get the service resource.
    dynamodb = boto3.client('dynamodb')
    
    response = dynamodb.delete_table(
        TableName='Instance_schedule'
    )

