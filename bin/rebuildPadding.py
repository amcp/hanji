#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import fileinput
import sys
import boto3
from boto3.dynamodb.conditions import Key


def fix(id):
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
    table = dynamodb.Table('case_data')
    cases_for_id = table.query(KeyConditionExpression=Key('hanji_id').eq(id),
                                ReturnConsumedCapacity='TOTAL')
    for item in cases_for_id['Items']:
        unpadded = item['hanji_id']
        if len(unpadded) == 6:
            continue
        item['hanji_id'] = unpadded.zfill(6)
        table.put_item(Item=item)
        table.delete_item(Key={'hanji_id': unpadded, 'category': item['category']})
    return 0


if __name__ == '__main__':
    for line in fileinput.input():
        result = fix(line.rstrip())
        if result != 0:
            sys.exit(result)
    sys.exit(0)
