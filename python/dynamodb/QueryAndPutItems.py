import json
import boto3
from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('COLLECT_AR_INVOICES_V10')
    total = 0
    data = []
    response = table.query(
        KeyConditionExpression=Key('id').eq("a9a29b8f-0211-5767-842e-a6ac7a23f32d") & Key('metaKey').begins_with("account#")
    )
    for item in response['Items']:
        data.append(item)
    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression=Key('id').eq("a9a29b8f-0211-5767-842e-a6ac7a23f32d") & Key('metaKey').begins_with("account#"),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        for item in response['Items']:
            data.append(item)
    print("done query")
    table = dynamodb.Table('PLANNING-SERVICE-TEST')
    for item in data:
        response = table.put_item(
            Item=item
        )
        print(response)
    return data;
    
    
