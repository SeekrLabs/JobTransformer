 #!/usr/bin/env python -W ignore::DeprecationWarning
import json
import boto3
import requests
from requests_aws4auth import AWS4Auth
import os

region = os.environ['AWS_REGION']
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, \
        service, session_token=credentials.token)
host = 'https://' + os.environ['DOMAIN_ENDPOINT']
index = os.environ['DOMAIN_INDEX']
type = 'indeed'

url = host + '/' + index + '/' + type + '/'
headers = { "Content-Type": "application/json" }

def no_transform(event, context):
    print(event)
    count = 0
    items = process_event(event)
    print(url)
    for item in items:
        id = item['url'].replace('/', '*')
        del(item['url'])
        print("Issuing to: " + url + id, end="\n")
        print(item)
        r = requests.put(url + id, json=item, headers=headers)
        if r.status_code != 200:
            print(r.text)
        count += 1
        
    return str(count) + " jobs processed."

def process_event(event):
    jobs = []
    for m in event['Records']:
        js = json.loads(m['body'])
        for j in js:
            jobs.append(j)
    return jobs