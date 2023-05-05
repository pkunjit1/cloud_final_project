#!/usr/bin/env python

import glob
import json
import os
import uuid
import boto3
import datetime
import random


from botocore.client import ClientError

def handler(event, context):


    #input_bucket_name = event['Records'][0]['s3']['bucket']['name']
    input_bucket_key = event['Records'][0]['s3']['object']['key']
    input_bucket_prefix = os.path.dirname(input_bucket_key)
    output_bucket_name = os.environ['DestinationBucket']

    if event['Records'][0]['eventName'].startswith('s3:ObjectRemoved:'):
        # The input object was deleted, delete the corresponding folder in the output bucket
        output_bucket_prefix = 'assets/' + input_bucket_prefix
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(output_bucket_name)
        for obj in bucket.objects.filter(Prefix=output_bucket_prefix):
            obj.delete()
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Folder deleted from output bucket.'}),
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        }

    sourceS3Bucket = event['Records'][0]['s3']['bucket']['name']
    sourceS3Key = event['Records'][0]['s3']['object']['key']
    sourceS3 = 's3://'+ sourceS3Bucket + '/' + sourceS3Key
    sourceS3Basename = os.path.splitext(os.path.basename(sourceS3Key))[0]
    destinationS3 = 's3://' + os.environ['DestinationBucket']
    destinationS3basename = os.path.splitext(os.path.basename(destinationS3))[0]
    mediaConvertRole = os.environ['MediaConvertRole']
    region = os.environ['AWS_DEFAULT_REGION']
    statusCode = 200
    body = {}
    
    # Use MediaConvert SDK UserMetadata to tag jobs with the assetID 
    # Events from MediaConvert will have the assetID in UserMedata
    jobMetadata = {'sourceVideoName': sourceS3Basename}

    print (json.dumps(event))
    
    try:
        # Job settings are in the lambda zip file in the current working directory
        with open('job.json') as json_data:
            jobSettings = json.load(json_data)
            print(jobSettings)
        
        # get the account-specific mediaconvert endpoint for this region
        mc_client = boto3.client('mediaconvert', region_name=region)
        endpoints = mc_client.describe_endpoints()

        # add the account-specific endpoint to the client session 
        client = boto3.client('mediaconvert', region_name=region, endpoint_url=endpoints['Endpoints'][0]['Url'], verify=False)

        # Update the job settings with the source video from the S3 event and destination 
        # paths for converted videos
        jobSettings['Inputs'][0]['FileInput'] = sourceS3
        
        S3KeyHLS = 'assets/' + sourceS3Basename + '/HLS/' + sourceS3Basename
        jobSettings['OutputGroups'][0]['OutputGroupSettings']['HlsGroupSettings']['Destination'] \
            = destinationS3 + '/' + S3KeyHLS
         
        S3KeyWatermark = 'assets/' + sourceS3Basename + '/MP4/' + sourceS3Basename
        jobSettings['OutputGroups'][1]['OutputGroupSettings']['FileGroupSettings']['Destination'] \
            = destinationS3 + '/' + S3KeyWatermark
        
        S3KeyThumbnails = 'assets/' + sourceS3Basename + '/Thumbnails/' + sourceS3Basename
        jobSettings['OutputGroups'][2]['OutputGroupSettings']['FileGroupSettings']['Destination'] \
            = destinationS3 + '/' + S3KeyThumbnails     

        print('jobSettings:')
        print(json.dumps(jobSettings))

        # Convert the video using AWS Elemental MediaConvert
        job = client.create_job(Role=mediaConvertRole, UserMetadata=jobMetadata, Settings=jobSettings)
        print (json.dumps(job, default=str))

    except Exception as e:
        print ('Exception: %s' % e)
        statusCode = 500
        raise

    finally:
        return {
            'statusCode': statusCode,
            'body': json.dumps(body),
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        }
