from boto3.s3.transfer import S3Transfer
import boto3
from datetime import datetime

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
    region_name = region)
table = dynamodb.Table(tableName)

currentDateTime = time
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


boto3.client('s3', region_name = region).download_file('afrinput', 'smallportrait.jpg', '/smallportrait.jpg')