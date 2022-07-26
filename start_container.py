import time
import json
import boto3
def lambda_handler(event, context):
    
    # boto3 client
    client = boto3.client("ec2")
    ssm = boto3.client("ssm")
    
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    
    table = dynamodb.Table('Instance_schedule')   ##table stored in dynamodb
    
    response = table.scan()
    InstanceId = response['Items']  # empty list to fatch instance_id from dynamodb
    
    while 'LastEvaluatedKey' in response:
        
    
        #add more instances from list
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        InstanceId.extend(response['Items'])
        
    start = "sudo docker start "

    # print(list)
    for each_instances in InstanceId:
        container_string = each_instances["ContainerName"]
        instance_string = each_instances["InstanceId"]
        instance_list = [instance_string]
        run_command = start + container_string
        run_command_list = [run_command]
        
        #start instance ()
        # client.start_instances(InstanceIds=instance_list)
        # print('start your instances: ' + str(instance_list))
        
        if container_string !=  "no_container" :
            # command to be executed on instance
            response = ssm.send_command(
                InstanceIds = instance_list,
                DocumentName = "AWS-RunShellScript",
                Parameters={
                    "commands": run_command_list
                },  # replace command_to_be_executed with command
            )
            
            print(f'Started Instanceid {instance_string}: containers {container_string}')

