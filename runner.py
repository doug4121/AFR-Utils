from boto3.s3.transfer import S3Transfer
import boto3
import sys
import os.path
import multiprocessing
import time
from datetime import datetime

sys.path.insert(1, '/AFR-Core')
from fawkes.protection import Fawkes


def main(
    downloadFilePath, 
    mode, 
    extension, 
    accessKey, 
    secretKey, 
    inputBucketName, 
    inputFileName, 
    outputBucketName, 
    outputFilePath, 
    outputFileName, 
    tableName, 
    imageId,
    timeout_seconds):
    
    try:
        image_paths = {downloadFilePath}       

        update_status(
            accessKey, 
            secretKey, 
            tableName, 
            imageId, 
            'PROCESSING')
        try:
            s3client = get_s3_client(
                accessKey, 
                secretKey)
        except:
            raise

        try:
            download_file(
                s3client,
                inputBucketName,
                inputFileName,
                downloadFilePath)
        except:
            raise

        try:
            if timeout_seconds <= 0:
                run_image_protection(
                    mode,
                    image_paths,
                    extension)
            else:
                run_image_protection_timeout(
                    mode,
                    image_paths,
                    extension,
                    timeout_seconds)
            
        except:
            raise

        try:
            upload_file(
                s3client,
                outputFilePath,
                outputBucketName,
                outputFileName)
        except:
            raise

        update_status(
            accessKey, 
            secretKey, 
            tableName, 
            imageId, 
            'SUCCESS')
    except:
        raise

def run_image_protection(
    mode,
    image_paths,
    extension):
    
    my_fawkes = Fawkes(
        "extractor_2", 
        '0', 
        1, 
        mode)
    my_fawkes.run_protection(
        image_paths, 
        debug = True, 
        format = extension)

def run_image_protection_timeout(
    mode,
    image_paths,
    extension,
    timeout_seconds):

    try:
        p = multiprocessing.Process(target=run_image_protection, args=(mode, image_paths, extension), name = ('afr_process_1'))
        startTime = time.time()
        p.start()

        while time.time() - startTime <= timeout_seconds:
            if p.is_alive():
                print(f"Process still alive with {timeout_seconds - (time.time() - startTime)} seconds remaining")
                time.sleep(1)
            else:
                break

        if p.is_alive():
            print("Process is still alive attempting to terminate")
            p.terminate()
            p.join()
            raise TimeoutError
    except:
        try:
            if p.is_alive():
                p.terminate()
                p.join()
        except:
            pass
        raise

def upload_file(
    client,
    outputFilePath,
    outputBucketName,
    outputFileName):

    transfer = S3Transfer(client)
    transfer.upload_file(
        outputFilePath, 
        outputBucketName, 
        outputFileName)
    
    if not os.path.isfile(outputFilePath):
        raise FileNotFoundError

def get_s3_client(
    accessKey,
    secretKey):

    return boto3.client(
        's3', 
        aws_access_key_id = accessKey, 
        aws_secret_access_key = secretKey)

def download_file(
    client,
    inputBucketName,
    inputFileName,
    downloadFilePath):

    client.download_file(
        inputBucketName, 
        inputFileName, 
        downloadFilePath)
    
    if not os.path.isfile(downloadFilePath):
        raise FileNotFoundError

def update_status(
        accessKey, 
        secretKey, 
        tableName, 
        imageId, 
        status):
    dynamodb = boto3.resource(
        'dynamodb', 
        aws_access_key_id = accessKey, 
        aws_secret_access_key = secretKey)
    table = dynamodb.Table(tableName)

    currentDateTime = get_date_time()
    try:
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
    except:
        raise

def get_date_time():
    now = datetime.now()
    isoformat = now.isoformat()
    diy_isoformat = now.strftime("%Y-%m-%dT%H:%M:%S.%f")
    assert isoformat == diy_isoformat
    return isoformat

if __name__ == "__main__":
    try:
        downloadFilePath = sys.argv[1]
        image_paths = {downloadFilePath}

        mode = sys.argv[2]
        extension = sys.argv[3]

        accessKey = sys.argv[4]
        secretKey = sys.argv[5]

        inputBucketName = sys.argv[6]
        inputFileName = sys.argv[7]

        outputBucketName = sys.argv[8]
        outputFilePath = sys.argv[9]
        outputFileName = sys.argv[10]

        tableName = sys.argv[11]
        imageId = sys.argv[12]

        timeout_seconds = float(sys.argv[13])

        main(
            downloadFilePath, 
            mode, 
            extension, 
            accessKey, 
            secretKey, 
            inputBucketName, 
            inputFileName, 
            outputBucketName, 
            outputFilePath, 
            outputFileName, 
            tableName, 
            imageId,
            timeout_seconds)
    except TimeoutError:
        update_status(
            accessKey, 
            secretKey, 
            tableName, 
            imageId, 
            'ERROR_EC2_TIMEOUT')
        raise
    except FileNotFoundError:
        update_status(
            accessKey, 
            secretKey, 
            tableName, 
            imageId, 
            'ERROR_EC2_FILE_NOT_FOUND')
        raise
    except:
        update_status(
            accessKey, 
            secretKey, 
            tableName, 
            imageId, 
            'ERROR_EC2_UNKNOWN')
        raise