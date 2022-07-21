import time
import json 
import boto3
from datetime import datetime, timedelta
from operator import itemgetter 
def lambda_handler(event, context):
    
    # boto3 client
    client = boto3.client("ec2")
    ec2 = boto3.resource('ec2',"us-east-1")
    ses = boto3.client('ses')
    client_cw = boto3.client('cloudwatch')
    client_ec2 = boto3.client('ec2')
    #  cpu utilization
    lis = []
    instace_id = []
    instance_tag= []
    tag_instance = []
    now = datetime.utcnow()  # Now time in UTC format   
    past = now - timedelta(days=7)  # 7 days  
     
    # Amazon Cloud Watch connection
    client_cw = boto3.client('cloudwatch')
    
    # Amazon EC2 connection
    client_ec2 = boto3.client('ec2')
    
    response = client_ec2.describe_instances()  # Get all instances from Amazon EC2
    
    def cpu_utilization_instance(instace_id):
            
        # Get CPU Utilization for each InstanceID
        CPUUtilization = client_cw.get_metric_statistics(
            Namespace='AWS/EC2',
            MetricName='CPUUtilization',
            Dimensions=[{'Name': 'InstanceId', 'Value': instace_id}],
            StartTime=past,
            EndTime=now,
            Period=3600, 
            Statistics=['Average']) 
            
        
        try:
            datapoints = CPUUtilization['Datapoints']                               # CPU Utilization results
            last_datapoint = sorted(datapoints, key=itemgetter('Timestamp'))[-1]    # Last result
            utilization = last_datapoint['Average']                                  # Last utilization
            load = round((utilization / 10.0), 3)                                  # Last utilization in %
            timestamp = str(last_datapoint['Timestamp'])                            # Last utilization timestamp
            # print("{0} load at {1}".format(load, timestamp)) 

            cpu_utilization = round((utilization),2) 
            return cpu_utilization
        except:
            return("0") 

    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:  
    
            # This will print output the value of the Dictionary key 'InstanceId'
            
            #print(instance["InstanceId"])
            cpu = cpu_utilization_instance(instance["InstanceId"])
            #cpu = cpu_utilization_instance("i-043dcca13aa764a27")
            instace_id.append(instance["InstanceId"])
            # print(instance["InstanceId"])   
            #print(instance.tags)
            
            lis.append(cpu) 
            # print(cpu) 
            try: 
                tags = (instance["Tags"])
                for i in tags:  #[{'Key': 'Name', 'Value': 'ankit-lab'}, {'Key': 'demo', 'Value': 'ffd'}]
                    #print(i) # [{'Key': 'Name', 'Value': 'cloudastra.in'}]
                    if(i['Key'] == "Name" ):
                        tag_instance.append(i['Value'])
            except: 
                tag_instance.append("-")
            
            
            
    print(lis)  
    print(instace_id)
    print(tag_instance)
             
            
    main_part = " " 
    
    for i in range(len(instace_id)):  
        main_part+= (f'<tr><th>{instace_id[i]} </th><th>{tag_instance[i]}</th><th>{lis[i]}</th></tr>')
    main_part = str(main_part) 
        
    email_content = """ 
    <html>
        <head></head>
      	<h4>list of instances id with tag and cpu utilizationlasted in 7 days</h4>
        <body>
            <table cellpadding="0" cellspacing="0" width="640" align="center" border="1">
                <tr>
                    <th>Instance ID</th>
                    <th>Tags(Name)</th>
                    <th>CPU Utilization</th>
                    
                </tr>""" + main_part + """ 
            </table> 
        </body>
    </html> """ 
    
    ses.send_email(  Source = 'bs01rathore@gmail.com',    # from
	    Destination = {
		    'ToAddresses': [
			    'bsrathore1122571@gmail.com'
		    ]
	    },
	    Message = { 
		    'Subject': {
			    'Data': 'created new instance in last 7 day ',
			    'Charset': 'UTF-8'
		    },
		    'Body': {
			    'Html':{
				    'Data': email_content,
				    'Charset': 'UTF-8'
			    }
		    }
	    }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully sent email from Lambda using Amazon SES')
    }  
