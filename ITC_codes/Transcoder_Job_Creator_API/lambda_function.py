import json
import uuid
import boto3
import copy
import datetime
from pytz import timezone

sns = boto3.client('sns')
sns_topic_arn = "arn:aws:sns:ap-south-1:840426506290:ITC_IA_SNS"
template_name = "Transcoding_template"
table_name = 'ItcTranscodingData'
dynamo_client = boto3.client('dynamodb')
input_dict = {
          "AudioSelectors": {
            "Audio Selector 1": {
              "DefaultSelection": "DEFAULT"
            }
          },
          "VideoSelector": {},
          "TimecodeSource": "ZEROBASED",
          "FileInput": "s3://itc-input-source-files/rawvideo/Test1/TTF HINDI 30SEC 02 HD_1.mp4"
        }
        
def lambda_handler(event, context):
    
    print("Event:",json.dumps(event))
    first_key = next(iter(event), None)
    if "Input" in first_key:
        JobStatus='In-Progress'
        jobmetadata = {}
        assetID = str(uuid.uuid4())
        jobmetadata["assetID"]=assetID
        identity = event["Identity"]
        mobile_no = event["MobileNo"]
        name = event["Name"]
        domain_name=""
        TimeStamp=""
        EndTime="NA"
        if "DomainName" in event:
            domain_name=event['DomainName']
       
        # fetching environmental variables
        region = "ap-south-1"
        endpoint_url = "https://htunurlzb.mediaconvert.ap-south-1.amazonaws.com"
        media_convert_role = "arn:aws:iam::840426506290:role/MediaConvert_Role"
        queue = "Default"
        destination_bucket = "test-mount-bucket-iaweb"
        
        # create boto3 client for media_convert
        media_client = boto3.client("mediaconvert", region_name=region, endpoint_url=endpoint_url, verify=False)
        
        # create a object for getting job template
        response = media_client.get_job_template(
                Name=template_name
            )
        
        ############################### get template service ########################
         
        try:
            setting = response["JobTemplate"]["Settings"]
            
            
            if "Input1" in event.keys():
                if "s3://" in event["Input1"]:
                    inputfilepath1 = event["Input1"]
                    setting['Inputs'][0]['FileInput']=inputfilepath1
                else:
                    raise Exception("Input1 Missing!")
                    return
            else:
                raise Exception("Input1 Missing!")
                return
                    
            # if "Input2" in event.keys():
            #     if "s3://" in event["Input2"]:
            #         inputfilepath2 = event["Input2"]
            #         input_setting2 = copy.deepcopy(input_dict)
            #         input_setting2['FileInput'] = inputfilepath2
            #         setting['Inputs'].append(input_setting2)
            #     else:
            #         raise Exception("Input2 Missing!")
            #         return
            # else:
            #     raise Exception("Input2 Missing!")
            #     return
                
            # if "Input3" in event.keys():
            #     if "s3://" in event["Input3"]:
            #         inputfilepath3 = event["Input3"]
            #         input_setting3 = copy.deepcopy(input_dict)
            #         input_setting3['FileInput'] = inputfilepath3
            #         setting['Inputs'].append(input_setting3)
            #     else:
            #         raise Exception("Input3 Missing!")
            #         return
            # else:
            #     raise Exception("Input3 Missing!")
            #     return
                
            # if "Input4" in event.keys():
            #     if "s3://" in event["Input4"]:
            #         inputfilepath4 = event["Input4"]
            #         input_setting4 = copy.deepcopy(input_dict)
            #         input_setting4['FileInput'] = inputfilepath4
            #         setting['Inputs'].append(input_setting4)
            #     else:
            #         raise Exception("Input4 Missing!")
            #         return
            # else:
            #     raise Exception("Input4 Missing!")
            #     return
                
            # if "Input5" in event.keys():
            #     if "s3://" in event["Input5"]:
            #         inputfilepath5 = event["Input5"]
            #         input_setting5 = copy.deepcopy(input_dict)
            #         input_setting5['FileInput'] = inputfilepath5
            #         setting['Inputs'].append(input_setting5)
            
            #     else:
            #         raise Exception("Input5 Missing!")
            #         return
            # else:
            #     raise Exception("Input5 Missing!")
            #     return
            
            
            # if "Input6" in event.keys():
            #     if "s3://" in event["Input6"]:
            #         inputfilepath6 = event["Input6"]
            #         input_setting6 = copy.deepcopy(input_dict)
            #         input_setting6['FileInput'] = inputfilepath6
            #         setting['Inputs'].append(input_setting6)
            #     else:
            #         raise Exception("Input6 Missing!")
            #         return
            # else:
            #     raise Exception("Input6 Missing!")
            #     return
            
            destination_path="s3://" + destination_bucket+"/"+ identity
            
            
            for item in setting["OutputGroups"]:
                if item["Name"] == "File Group":
                    item['OutputGroupSettings']['FileGroupSettings']['Destination'] = destination_path
            
            print("----------------------------------------------------------")
            print(json.dumps(setting))
            
            ######################################### CREATE JOB #############################################
        
            # # creating job using create_job function of media_client service
            job = media_client.create_job(
                        JobTemplate=template_name,
                        Role=media_convert_role,
                        Queue =queue,
                        UserMetadata=jobmetadata,
                        Settings=setting
                        )
                        
            # dumping the job created information using json
            jsoninput = json.dumps(job, default = str)
            jsondata = json.loads(jsoninput)
            
            # getting job id of the job created
            jobid = jsondata['Job']["Id"]
            response_result = {"ResourceID" : assetID, "JobStatus":JobStatus}
            print("assetID",assetID)
            
            format = "%Y-%m-%d %H:%M:%S"
            now_utc = datetime.datetime.now(timezone('UTC'))
            TimeStamp1 = now_utc.astimezone(timezone('Asia/Kolkata'))
            TimeStamp=TimeStamp1.strftime(format)
            
            if domain_name:
                
                response = dynamo_client.put_item(TableName=table_name,Item={'InputFileName':{'S':inputfilepath1},'DomainName':{'S':domain_name},"ResourceID":{'S':assetID},'JobStatus':{'S':JobStatus},'MobileNo':{'S':mobile_no},'Filename':{'S':identity},'Username':{'S':name},'StartTime':{'S':TimeStamp},'EndTime':{'S':EndTime},'JobId':{'S':jobid}})
            else:
            
                response = dynamo_client.put_item(TableName=table_name,Item={'InputFileName':{'S':inputfilepath1},"ResourceID":{'S':assetID},'JobStatus':{'S':JobStatus},'MobileNo':{'S':mobile_no},'Filename':{'S':identity},'Username':{'S':name},'StartTime':{'S':TimeStamp},'EndTime':{'S':EndTime},'JobId':{'S':jobid}})
            
            # returning ResourceID and JobStatus of the created job in the form of json response
            return response_result
            
        except Exception as e:
            message = str(e).replace("'",'"').replace('"',"")
            # sns_message = f'Hi team,\r\n Please find the details below for which an warning occured while creating a Media Convert job:\r\n ResourceID: "{assetID}"\r\n Error: "{message}"'
            # sns_response = sns.publish(
            #     TopicArn = sns_topic_arn,
            #     Message = sns_message,
            #     Subject='Warning: Transcoder_Job_Creator_API')
            status_code = 404
            response = {"StatusCode":status_code,"Message":message}
            return response
    
    elif "ResourceID" in event.keys():
        response = dynamo_client.query(
            TableName = table_name,
            KeyConditionExpression='ResourceID = :RID',
            ExpressionAttributeValues={':RID': {'S': event['ResourceID']}},
            ProjectionExpression = 'JobStatus,OutputPath'
        )
        JobStatus=response['Items'][0]['JobStatus']['S']
        if "COMPLETE" in JobStatus:
            OutputPath=response['Items'][0]['OutputPath']['S']
            response_result = {"OutputPath" : OutputPath.replace("s3://test-mount-bucket-iaweb","https://df.ia-beta.in/myad").replace(".mp4",""), "JobStatus":JobStatus}
            
        elif "In-Progress" in JobStatus:
            status_code = 102
            response_result = {"JobStatus":JobStatus,"StatusCode":status_code}
            
        else:
            status_code =  500
            response_result = {"JobStatus":"ERROR","StatusCode":status_code}
        return response_result
        
    else:
        print("Bad Request")
        response_result = {"Message":"Bad Request","StatusCode":400}
        return response_result
    
