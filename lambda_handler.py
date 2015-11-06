from __future__ import print_function

import os

import boto3
import json

def put_record(client, stream, key, record):
    print("Putting: {0} :: {1}".format(key, record))
    response = client.put_record(
        StreamName=stream,
        Data=record,
        PartitionKey=key
    )
    print("Rsp: {0}".format(response))
    return response

def lambda_handler(event, context):
    region_name = 'us-east-1'
    stream_name = os.environ.get("STREAM", "ifs-test")
    client = boto3.client('kinesis')
    key = event.pop("key", None)
    data_string = json.dumps(event, indent=2)
    return "%s" % put_record(client, stream_name, key, data_string)
