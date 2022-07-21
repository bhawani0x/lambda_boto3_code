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
    
    odd_month = [1,3,5,7,8,10,12]
    even_month =[4,6,9,11]
    feb_month = [2]
    
    
    current_day = int(time.strftime("%d")) 
    current_month = int(time.strftime("%m")) 
    current_year = int(time.strftime("%Y"))
   
    count_instance = 0
    created_instance =[]   
    created_date = []
    tag_instance =[]
    cpu_utilization_ =[]
    
    
    now = datetime.utcnow()  # Now time in UTC format   
    past = now - timedelta(days=7)  # 8 days
    
    response = client_ec2.describe_instances() 
    
    def cpu_utilization_instance(instace_id):
            
        # Get CPU Utilization for each InstanceID
        CPUUtilization = client_cw.get_metric_statistics(
            Namespace='AWS/EC2',
            MetricName='CPUUtilization',
            Dimensions=[{'Name': 'InstanceId', 'Value': instace_id}],
            StartTime=past,
            EndTime=now,
            Period=1440,
            Unit = 'Percent',
            Statistics=['Average']) 
            
        
        try:
            datapoints = CPUUtilization['Datapoints']                               # CPU Utilization results
            last_datapoint = sorted(datapoints, key=itemgetter('Timestamp'))[-1]    # Last result
            utilization = last_datapoint['Average']                                  # Last utilization
            load = round((utilization / 10.0), 3)                                  # Last utilization in %
            timestamp = str(last_datapoint['Timestamp'])                            # Last utilization timestamp
            # print("{0} load at {1}".format(load, timestamp)) 

            cpu_utilization = round((utilization),2) 
            return str(cpu_utilization)
        except:
            print ("unable to calculate") 
     
    ## print all volume  
 
    for instance in ec2.instances.all():
         for volume in instance.volumes.all():
             #print(instance.volumes)
            
            #  try:
            #      for i in instance.tags:
            #          print(i)
            #  except :
            #      print('none ')   
            
             #date condition
             
             create_date= volume.create_time.day
             create_month = volume.create_time.month 
             create_year= volume.create_time.year
          
             if (current_day > 6 and current_month == create_month):
                 demo_current_day = current_day
                 for i in range(7):
                     if(create_date == demo_current_day):
                         count_instance +=1
                         created_instance.append(f'{instance.id}')
                         created_date.append(f'{create_date}/{create_month}/{create_year}')
                         cpu = cpu_utilization_instance(instance.id)
                         cpu_utilization_.append(cpu)
                         #  print(cpu_utilization_)   
                         try:
                             for i in instance.tags: 
                                 #print(i) # [{'Key': 'Name', 'Value': 'cloudastra.in'}]
                                 if(i['Key'] == "Name" ):
                                     tag_instance.append(i['Value'])
                                     
                                    #  print(i['Value'])
                                    #  print(instance.id) 
                         except: 
                             tag_instance.append('-')  
                         break
                     else:
                         demo_current_day = demo_current_day - 1
                          
             if(current_day < 7 and (current_month == create_month )):
                 demo_current_day = current_day
                 for i in range(demo_current_day,0,-1):
                     if(create_date == demo_current_day):
                         count_instance +=1
                         created_instance.append(f'{instance.id}')
                         created_date.append(f'{create_date}/{create_month}/{create_year}')
                         try:
                             for i in instance.tags:
                                 #print(i) # [{'Key': 'Name', 'Value': 'cloudastra.in'}]
                                 if(i['Key'] == "Name" ):
                                     tag_instance.append(i['Value'])
                                     #print(i['Value'])
                         except:
                             tag_instance.append('-') 
                         break
                     else:
                         demo_current_day = demo_current_day - 1 
                        
             if(current_day < 7 and (current_month > create_month)):
                 if (create_month in even_month):
                     demo_current_day = current_day 
                     even_month_day = 7 - demo_current_day
                     for i in range(even_month_day,0,-1):
                         if(create_date == 31-i):
                             count_instance +=1
                             created_instance.append(f'{instance.id}')
                             created_date.append(f'{create_date}/{create_month}/{create_year}')
                             try:
                                 for i in instance.tags:
                                     #print(i) # [{'Key': 'Name', 'Value': 'cloudastra.in'}]
                                     if(i['Key'] == "Name" ):
                                         tag_instance.append(i['Value'])
                                         #print(i['Value'])
                             except:
                                 tag_instance.append('-') 
                             break 
                  
                 if(create_month in odd_month):
                     demo_current_day = current_day 
                     odd_month_day = 7 - demo_current_day 
                     for i in range(odd_month_day,0,-1):
                         if(create_date == 32-i):
                             count_instance +=1
                             created_instance.append(f'{instance.id}')
                             created_date.append(f'{create_date}/{create_month}/{create_year}')
                             try:
                                 for i in instance.tags:
                                     #print(i) # [{'Key': 'Name', 'Value': 'cloudastra.in'}]
                                     if(i['Key'] == "Name" ):
                                         tag_instance.append(i['Value'])
                                         #print(i['Value'])
                             except:
                                 tag_instance.append('-') 
                             break 
                 
                 if(create_month in feb_month):   # feb monnth of 28 days
                     demo_current_day = current_day 
                     odd_month_day = 7 - demo_current_day 
                     for i in range(odd_month_day,0,-1):
                         if(create_date == 29-i):
                             count_instance +=1
                             created_instance.append(f'{instance.id}')
                             created_date.append(f'{create_date}/{create_month}/{create_year}')
                             try:
                                 for i in instance.tags:
                                     #print(i) # [{'Key': 'Name', 'Value': 'cloudastra.in'}]
                                     if(i['Key'] == "Name" ):
                                         tag_instance.append(i['Value'])
                                         #print(i['Value'])
                             except:
                                 tag_instance.append('-')  
                             break  
                          
                         
    # print(count_instance)                     
    # print(cpu_utilization_)
    # print(tag_instance) 
    # print(created_instance)
    # print(created_date) 
    
    main_part = ""
    for i in range(count_instance):
        main_part+= (f'<tr><th>{created_instance[i]} </th> <th>{created_date[i]}</th><th>{tag_instance[i]}</th><th>{cpu_utilization_[i]}</th></tr>')
    main_part = str(main_part) 
        
    email_content = """
    <html>
        <head></head>
      	<h4>list of instances created or modified(attached new volume) in last 7 days</h4>
        <body>
            <table cellpadding="0" cellspacing="0" width="640" align="center" border="1">
                <tr>
                    <th>Instance ID</th>
                    <th>Created or Modified date</th>
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
  
