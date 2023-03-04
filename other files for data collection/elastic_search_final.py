import json
import boto3
import requests
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key

# from botocore.vendored import requests

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')


def insertIntoElasticSearch():
    dynamoDbData = table.scan()
    i = 1
    url = 'https://search-ak9691-sx2236-chatbot-w2q6ytziztm6jtq7mmozbtmsiq.us-east-1.es.amazonaws.com/restaurants/restaurant'
    headers = {"Content-Type": "application/json"}
    while True:
        for item in dynamoDbData['Items']:
            body = {"Business_ID": item['Business_ID'], "Cuisine": item['Cuisine']}
            r = requests.post(url, auth=("ak9691-sx2236", "Ak9691-Sx2236"), data=json.dumps(body).encode("utf-8"),
                              headers=headers)
            print(r.content.decode('utf-8'))
            i += 1
            # break
        if 'LastEvaluatedKey' in dynamoDbData:
            dynamoDbData = table.scan(
                ExclusiveStartKey=dynamoDbData['LastEvaluatedKey']
            )
            # break;
        else:
            break
        print(i)


insertIntoElasticSearch()
