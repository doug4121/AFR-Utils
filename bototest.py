from boto3.s3.transfer import S3Transfer
import boto3
from datetime import datetime

sts_client = boto3.client('sts')
assumed_role_object=sts_client.assume_role(
    RoleArn="arn:aws:iam::195039344825:role/AFR",
    RoleSessionName="AssumeRoleSession1"
)
credentials=assumed_role_object['Credentials']

sessionToken = credentials['SessionToken']
accessKey = credentials['AccessKeyId']
secretKey = credentials['SecretAccessKey']
region = 'us-west-2'
tableName = 'imageprocessing'
imageId = '3d468884-ab83-4468-8abe-1c221dddc554'
status = 'HELLO'

now = datetime.now()
isoformat = now.isoformat()
diy_isoformat = now.strftime("%Y-%m-%dT%H:%M:%S.%f")
assert isoformat == diy_isoformat
time = isoformat

dynamodb = boto3.resource(
    'dynamodb', 
    aws_access_key_id = accessKey, 
    aws_secret_access_key = secretKey,
    region_name = region,
    aws_session_token = sessionToken)
table = dynamodb.Table(tableName)

currentDateTime = get_date_time()
table.update_item(
    Key = {'imageId': imageId},
    ConditionExpression = 'attribute_exists(imageId)',
    UpdateExpression = 'SET #status = :val, #dt = :dtval',
    ExpressionAttributeNames = {
        '#status': 'status', 
        '#dt': 'updatedDateTime'
        },
    ExpressionAttributeValues = {
        ':val': status, 
        ':dtval': currentDateTime
        },
)
