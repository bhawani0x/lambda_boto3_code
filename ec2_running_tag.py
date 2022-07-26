import time
import json 
import boto3

def lambda_handler(event, context):
    
    # boto3 client
    client = boto3.client("ec2")
    ec2 = boto3.resource('ec2',"us-east-1")

    count_instance = 0
    created_instance =[]   
    created_date = []
    tag_instance =[]
    cpu_utilization_ =[] 
    for instance in ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running'] } ]):
        try:
            for i in instance.tags:
                #print(i)
                if(i['Value'] == 'ankit-lab'): 
                    print(i['Value']) 
        except: 
            print("except ")  