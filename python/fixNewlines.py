from __future__ import print_function
import boto3
from boto3.dynamodb.conditions import Key


def fix(id):
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
    table = dynamodb.Table('case_data')
    cases_for_id = table.query(KeyConditionExpression=Key('hanji_id').eq(id),
                                ReturnConsumedCapacity='TOTAL')
    for item in cases_for_id['Items']:
        table.delete_item(Key={'hanji_id':item['hanji_id'], 'category':item['category']})


for id in ['11493\n', '11700\n', '16758\n', '22059\n', '22059\n']:
    fix(id)
