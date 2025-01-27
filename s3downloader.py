from boto3.session import Session
import boto3
import sys

accessKey = sys.argv[1]
secretKey = sys.argv[2]

bucketName = sys.argv[3]
inputFileName = sys.argv[4]
outputFileName = sys.argv[5]


client = boto3.client('s3', aws_access_key_id=accessKey , aws_secret_access_key=secretKey)
client.download_file(bucketName, inputFileName, outputFileName)