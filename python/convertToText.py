#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import os
import re
import uuid
import sys
import fileinput
import boto3
from boto3.dynamodb.conditions import Key
from watson_developer_cloud import DocumentConversionV1
from watson_developer_cloud import WatsonException


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


document_conversion = DocumentConversionV1(
    username=os.environ['BLUEMIX_USERNAME'],
    password=os.environ['BLUEMIX_PASSWORD'],
    version='2015-12-15'
)


def convert_pdf(base):
    # get the S3 key name
    base_re = re.compile(r".*(\d{3}).(\d{6}).*", re.UNICODE)
    base_match_obj = base_re.match(base)
    if not base_match_obj:
        eprint('regex broken for ' + base)
        return -1
    hanji_id = base_match_obj.group(2)

    # download the PDF
    s3key = base + '_hanrei.pdf'
    s3 = boto3.client('s3')
    filename = str(uuid.uuid4()) + '.pdf'
    bucket = 'hanji'
    s3.download_file(bucket, s3key, filename)

    # convert document
    with open(filename, 'r') as doc:
        try:
            response = document_conversion.convert_document(document=doc, config={'conversion_target': 'NORMALIZED_TEXT'})
        except WatsonException as e:
            if "Error: The Media Type [text/plain] of the input document is not supported" in e.message:
                eprint("Malformed PDF at s3://" + bucket + '/' + s3key)
                return -1
            elif "Code: 500" in e.message:
                eprint("Retry ISE on " + base)
                return 0  # TODO refactor after IBM replies with a retry strategy
            else:
                raise e
        finally:
            os.remove(os.getcwd() + "/" + filename)

    # put conversion output in S3
    converted_key = base + ".txt"
    s3.put_object(ACL='public-read', Bucket='hanji-text', Key=converted_key, Body=response.content,
                  ContentType='text/plain')

    # update metadata
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
    table = dynamodb.Table('case_data')
    cases_for_url = table.query(KeyConditionExpression=Key('hanji_id').eq(hanji_id),
                                ReturnConsumedCapacity='TOTAL')
    converted_s3_url = 'https://s3.dualstack.ap-northeast-1.amazonaws.com/hanji-text/' + converted_key
    for item in cases_for_url['Items']:
        if base not in item['caseUrl']:
            continue
        table.update_item(Key={'hanji_id':item['hanji_id'], 'category':item['category']},
                          UpdateExpression='SET s3_text_url = :url',
                          ExpressionAttributeValues={':url': converted_s3_url}, ReturnConsumedCapacity='TOTAL')
    print(base)
    return 0


if __name__ == '__main__':
    for line in fileinput.input():
        result = convert_pdf(line.rstrip())
        if result != 0:
            sys.exit(result)
    sys.exit(0)
