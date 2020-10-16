import json
import boto3
from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('COLLECT_AR_INVOICES_V6')
    total = 0
    data = []
    response = table.query(
        IndexName='amzChannel-marketplace-index',
        KeyConditionExpression=
            Key('amzChannel').eq("ADVERTISING") & Key('marketplace').eq("UK")
    )
    for item in response['Items']:
        data.append(item)
    while 'LastEvaluatedKey' in response:
        response = table.query(
            IndexName='amzChannel-marketplace-index',
            KeyConditionExpression=
            Key('amzChannel').eq("ADVERTISING") & Key('marketplace').eq("UK"),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        for item in response['Items']:
            data.append(item)
    print("done query")
    return len(data);
    
