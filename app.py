from __future__ import print_function

import os
from bottle import route, run, request

import boto
import argparse
import json
import threading
import time
import datetime

from argparse import RawTextHelpFormatter
from random import choice
from string import lowercase
from boto.kinesis.exceptions import ResourceNotFoundException

def get_stream(stream_name):
    stream = kinesis.describe_stream(stream_name)
    print (json.dumps(stream, sort_keys=True, indent=2,
        separators=(',', ': ')))
    return stream

def put_record(kinesis, stream, key, record):
    print("Putting: {0} :: {1}".format(key, record))
    response = kinesis.put_record(
        stream_name=stream,
        data=record, partition_key=key)
    print("Rsp: {0}".format(response))
    return response

def describe_stream(kinesis, stream_name):
    stream = kinesis.describe_stream(stream_name)
    print (json.dumps(stream, sort_keys=True, indent=2,
        separators=(',', ': ')))

region_name = 'us-east-1'
stream_name = os.environ.get("STREAM", "test-stream")
kinesis = boto.kinesis.connect_to_region(region_name)
describe_stream(kinesis, stream_name)

@route('/<key>', method='POST')
def body(key):
    data = request.body.read()
    return "%s" % put_record(kinesis, stream_name, key, data)

@route('/')
def index():
    return "STREAM=%s" % stream_name

run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
