import time
import json
import boto3
def lambda_handler(event, context):

# Get the service resource.
    dynamodb = boto3.client('dynamodb')
    #CREATE DYNAMODB TABLE#    
    table = dynamodb.create_table(
        TableName='Instance_schedule',
        KeySchema=[
            {
                'AttributeName': 'InstanceId',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'ContainerName',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'InstanceId',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'ContainerName',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 100,
            'WriteCapacityUnits': 100
        }
    )

