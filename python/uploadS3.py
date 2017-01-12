#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import fileinput
import sys
import re
import requests
import boto3
from boto3.dynamodb.conditions import Key


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def upload_pdf_and_update_case_data(base):
    s3key = base + '_hanrei.pdf'
    url = 'http://www.courts.go.jp/app/files/hanrei_jp/' + s3key
    r = requests.get(url)
    if r.status_code != 200:
        eprint('unable to download ' + url)
        return -1

    # upload file to s3
    s3 = boto3.client('s3')
    url_re = re.compile(r".*(\d{3}).(\d{6}).*", re.UNICODE)
    url_match_obj = url_re.match(url)
    if not url_match_obj:
        eprint('regex broken for ' + url)
        return -1
    hanji_id = url_match_obj.group(2)
    s3.put_object(ACL='public-read', Bucket='hanji', Key=s3key, Body=r.content, ContentType=r.headers['content-type'])

    # update metadata
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
    table = dynamodb.Table('case_data')

    cases_for_url = table.query(KeyConditionExpression=Key('hanji_id').eq(hanji_id.lstrip('0')),
                                ReturnConsumedCapacity='TOTAL')
    for item in cases_for_url['Items']:
        if not item['caseUrl'] == url:
            continue
        table.update_item(Key={'hanji_id':item['hanji_id'], 'category':item['category']},
                          UpdateExpression='SET s3_url = :url',
                          ExpressionAttributeValues={':url': 'https://s3.dualstack.ap-northeast-1.amazonaws.com/hanji/' + s3key},
                          ReturnConsumedCapacity='TOTAL')
    print(base)
    return 0


if __name__ == '__main__':
    for line in fileinput.input():
        result = upload_pdf_and_update_case_data(line.rstrip())
        if result != 0:
            sys.exit(result)
    sys.exit(0)
