import json
import boto3
import requests
import datetime
from pytz import timezone

sns = boto3.client('sns')
sns_topic_arn = "arn:aws:sns:ap-south-1:840426506290:ITC_IA_SNS"
table_name = 'ItcTranscodingData'
trancoded_bucket="test-mount-bucket-iaweb"
dynamo_client = boto3.client('dynamodb')
s3_client= boto3.client("s3")

def lambda_handler(event, context):
    try:
        print("event",event)
        # print("ABhishel")
        assetId = event['detail']["userMetadata"]["assetID"]
        db_data={}
        # db_data["VideoId"] = event['detail']['jobId']     
        db_data["JobStatus"] = event['detail']['status']
        
        db_data["outputPath"] = event['detail']['outputGroupDetails'][0]["outputDetails"][0]['outputFilePaths'][0]
        
        print(db_data)
        DomainName="df.ia-beta.in"
        
        if db_data["JobStatus"] == "COMPLETE":
            
            format = "%Y-%m-%d %H:%M:%S"
            now_utc = datetime.datetime.now(timezone('UTC'))
            TimeStamp1 = now_utc.astimezone(timezone('Asia/Kolkata'))
            TimeStamp=TimeStamp1.strftime(format)
            
            response2 = dynamo_client.query(
                TableName=table_name,
                KeyConditionExpression = "ResourceID=:rd",
                ExpressionAttributeValues = {':rd':{'S':assetId}},
                ProjectionExpression = 'MobileNo,Filename,Username,DomainName'
                )
            mobile_no = response2['Items'][0]['MobileNo']['S']
            identity = response2['Items'][0]['Filename']['S']

            name = response2['Items'][0]['Username']['S']
            if "DomainName" in response2['Items'][0]:
                DomainName=response2['Items'][0]['DomainName']['S']
                
                
            
            #renaming output file         
            src_key=db_data["outputPath"].replace("s3://test-mount-bucket-iaweb/","")
            dest_key=src_key.replace("_1080","")
            try:
                response=s3_client.copy_object(Bucket=trancoded_bucket,CopySource={'Bucket':trancoded_bucket,'Key':src_key},Key=dest_key)
                print('Copy Response',response)
            except Exception as e:
                print('Exception In Copy Response',e)
            
            s3_client.delete_object(Bucket=trancoded_bucket, Key=src_key)
            print("delete source key with 1080 ")
            # calling whatsapp api to send playable url to specific mobile number
            url = "https://app.yellow.ai/api/engagements/notifications/v2/push?bot=x1623046926082"
            
            payload = json.dumps({
                "userDetails": {
                    "number": f"91{mobile_no}"
                },
                "notification": {
                    "type": "whatsapp",
                    "sender": "919845775520",
                    "templateId": "output2",
                    "language": "en",
                    "namespace": "a9c07100_1c0e_4851_a0f0_0039c80bc1ef",
                    "params": {
                      "1": name,
                      "2": f"https://{DomainName}/myad/{identity}"
                }
                }
            })
            
            headers = {
                "x-api-key": "aZBKIJDlLfIG9dgtBa0HtpQP4On6GI-oY4gdAfAa",
                "Content-Type": "application/json"
            }
            
            response = requests.request("POST",url,headers=headers,data=payload)
            print("whatsapp api response--->",response.text)
            print("whatsapp api status code--->",response.status_code)
            
            update_expression = "SET JobStatus = :jobstatus, OutputPath = :op, EndTime =:et"
        
            expression_attribute_values = {
                ":jobstatus": {"S": db_data["JobStatus"]},
                ":op": {"S": db_data["outputPath"].replace("_1080","")},
                ":et": {"S": TimeStamp}
            }
            response = dynamo_client.update_item(
                TableName=table_name,
                Key={'ResourceID': {'S': assetId}},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values)
                
            
            
        elif db_data["JobStatus"] == "ERROR":
            update_expression = "SET JobStatus = :jobstatus"
        
            expression_attribute_values = {
                ":jobstatus": {"S": db_data["JobStatus"]}
            }
            response = dynamo_client.update_item(
                TableName=table_name,
                Key={'ResourceID': {'S': assetId}},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values)
                
    except Exception as e:
        print("In Exception",e)
        message = str(e).replace("'",'"')
        sns_message = f'Hi team,\r\n Please find the details below for which an warning occured while creating a Media Convert job:\r\n ResourceID: "{assetId}"\r\n Error: {message}'
        sns_response = sns.publish(
            TopicArn = sns_topic_arn,
            Message = sns_message,
            Subject='Warning: Staging Post_Transcoding_Processing function'
            )
        if db_data["JobStatus"] == "ERROR":
            update_expression = "SET JobStatus = :jobstatus"
        
            expression_attribute_values = {
                ":jobstatus": {"S": db_data["JobStatus"]}
            }
            response = dynamo_client.update_item(
                TableName=table_name,
                Key={'ResourceID': {'S': assetId}},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values)
            
