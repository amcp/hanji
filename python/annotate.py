#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import re
import json
from joblib import Parallel, delayed
import fileinput
import sys
import socket

import boto3
from boto3.dynamodb.conditions import Key
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def get_service():
    credentials = GoogleCredentials.get_application_default()
    return discovery.build('language', 'v1', credentials=credentials)


def annotate(base):
    # get hanji id
    base_re = re.compile(r".*(\d{3}).(\d{6}).*", re.UNICODE)
    url_match_obj = base_re.match(base)
    if not url_match_obj:
        eprint('regex broken for ' + base)
        return -1
    hanji_id = url_match_obj.group(2)

    # query table for all entries for hanji id
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
    table = dynamodb.Table('case_data')
    cases_for_id = table.query(KeyConditionExpression=Key('hanji_id').eq(hanji_id),
                                ReturnConsumedCapacity='TOTAL')
    google_natural_language_service = get_service()
    s3 = boto3.client('s3')
    for item in cases_for_id['Items']:
        text_key = base + '.txt'
        # # get text in hanji-text bucket
        # response = s3.get_object(Bucket='hanji-text', Key=text_key)
        # text = response['Body'].read()
        with open('text/' + text_key, 'r') as myfile:
            text = myfile.read()
        if len(text) > 1000000:
            eprint(base + " was " + str(len(text)) + " bytes long; skipping")
            continue

        # create google nls request body
        body = {
            "document":{
                "type":"PLAIN_TEXT",
                "content":text
            },
            "features": {
                "extractSyntax": True,
                "extractEntities": True,
                "extractDocumentSentiment": True,
            },
            "encodingType":"UTF8"
        }

        # analyze the text
        request = google_natural_language_service.documents().annotateText(body=body)
        response = request.execute()
        annotated_key = base + ".json"

        # put analysis in s3'
        s3.put_object(ACL='public-read', Bucket='hanji-text-annotated', Key=annotated_key,
                      Body=json.dumps(response, ensure_ascii=False, indent=4, separators=(',', ': ')), ContentType='application/json')
        annotated_s3_url = 'https://s3.dualstack.ap-northeast-1.amazonaws.com/hanji-text-annotated/' + annotated_key

        # add analysis link to dynamodb item
        table.update_item(Key={'hanji_id': item['hanji_id'], 'category': item['category']},
                          UpdateExpression='SET s3_annotated_url = :url',
                          ExpressionAttributeValues={':url': annotated_s3_url}, ReturnConsumedCapacity='TOTAL')
    print(base)
    return 0


timeout = 10
socket.setdefaulttimeout(timeout)

#with open('converted_bases') as f:
#    urls = f.readlines()
# results = Parallel(n_jobs=30)(delayed(annotate)(url.strip()) for url in urls)


if __name__ == '__main__':
    for line in fileinput.input():
        result = annotate(line.rstrip())
        if result != 0:
            sys.exit(result)
    sys.exit(0)
