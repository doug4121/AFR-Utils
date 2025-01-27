from boto3.s3.transfer import S3Transfer
import boto3
import sys

accessKey = sys.argv[1]
secretKey = sys.argv[2]

bucketName = sys.argv[3]
inputFilePath = sys.argv[4]
outputFileName = sys.argv[5]


client = boto3.client('s3', aws_access_key_id = accessKey, aws_secret_access_key = secretKey)

transfer = S3Transfer(client)

transfer.upload_file(inputFilePath, bucketName, outputFileName)
