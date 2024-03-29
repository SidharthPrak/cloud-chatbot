import uuid
import datetime
import logging
import boto3
import json
from botocore.exceptions import ClientError
import requests
import decimal
import os

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def replace_decimals(obj):
    if isinstance(obj, list):
        for i in range(0, len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = replace_decimals(obj[k])
        return obj
    elif isinstance(obj, decimal.Decimal):
        return str(obj)
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj


def get_sqs_data(queue_URL):
    sqs = boto3.client('sqs')
    queue_url = queue_URL

    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'time', 'cuisine', 'location', 'num_people', 'phNo'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )

        messages = response['Messages'] if 'Messages' in response.keys() else []

        for message in messages:
            receiptHandle = message['ReceiptHandle']
            sqs.delete_message(QueueUrl=queue_URL, ReceiptHandle=receiptHandle)
        return messages

    except ClientError as e:
        logging.error(e)
        return []


def es_search(host, query):
    response = json.loads(requests.get(
        'https://search-ak9691-sx2236-chatbot-w2q6ytziztm6jtq7mmozbtmsiq.us-east-1.es.amazonaws.com/restaurants/restaurant/_search',
        auth=("ak9691-sx2236", "Ak9691-Sx2236"),
        headers={"Content-Type": "application/json"}, data=json.dumps(query)).content.decode('utf-8'))

    # return response
    logger.info(response)
    return response


def get_dynamo_data(dynno, table, value):
    response = table.get_item(Key={'Business_ID': value}, TableName='yelp-restaurants')

    name = response['Item']['Name']
    address_list = response['Item']['Address']
    return '{}, {}'.format(name, address_list)


def send_email_to_customer(location, cuisine, number, details):
    SENDER = "abhishekkandukuri@gmail.com"
    RECIPIENT = "abhishekkandukuri98@gmail.com"
    SUBJECT = "Restaurants Recommendation from Dining Bot Concierge"

    BODY_HTML = """<html>
                    <head></head>
                    <body>
                    <p> Hello, <br> <br>
                    Hello and welcome to our Dining Concierge Bot! We're thrilled to assist you with your dining needs. <br> <br>
                    Thank you for choosing our service. We have curated an exciting list of resturants of """ + cuisine + """ cuisine in """ + location + """ for """ + number + """ people. Here are your suggestions: <br> <br>
                    """ + details + """ <br>
                    Thanks, <br>
                    Dining Bot Team <br>
                    Abhishek (ak9691@nyu.edu)
                    Sidharth (sx2236@nyu.edu)
                    <br>
                    </p>
                    </body>
                    </html>
                """

    CHARSET = "UTF-8"

    print(BODY_HTML)

    client = boto3.client('ses')

    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': "",
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    except ClientError as e:
        print(e.response['Error']['Message'])


def write_suggestions_to_suggestion_table(details):
    user_table_name = 'user_suggestions'

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    try:
        user_table = dynamodb.Table(user_table_name)
        user_table.put_item(
            Item={
                'user_id':'user1',
                'suggestions': details
            }
        )
    except ClientError as e:
        logging.error(e)


def lambda_handler(event, context):

    es_host = 'https://search-ak9691-sx2236-chatbot-w2q6ytziztm6jtq7mmozbtmsiq.us-east-1.es.amazonaws.com'
    table_name = 'yelp-restaurants'

    messages = get_sqs_data('https://sqs.us-east-1.amazonaws.com/237440262663/cloud-as1-search-parameters-queue')

    print(f" Message is : {messages}")

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    for message in messages:
        logging.info(message)
        msg_attributes = message['MessageAttributes']
        query = {"from": 0, "size": 5, "query": {"match": {"Cuisine": msg_attributes["cuisine"]["StringValue"]}}}
        es_search_result = es_search(es_host, query)
        number_of_records_found = int(es_search_result["hits"]["total"]["value"])
        print(f"Total Number of records in ES : {number_of_records_found}")
        hits = es_search_result['hits']['hits']
        print(f"Data are : {hits}")
        suggested_restaurants = []
        for hit in hits:
            id = hit['_source']['Business_ID']
            suggested_restaurant = get_dynamo_data(dynamodb, table, id)
            print(f"LOG:{suggested_restaurant}")
            suggested_restaurants.append(suggested_restaurant)
            details = suggested_restaurants
        print(f"Suggested Resturants are : {suggested_restaurants}")
        logging.info("reached")
        data = ""

        for i, rest in enumerate(suggested_restaurants):
            data += str(i + 1) + ". " + rest + "<br>"

        write_suggestions_to_suggestion_table(details)
        send_email_to_customer(msg_attributes['location']['StringValue'], msg_attributes['cuisine']['StringValue'],
                   msg_attributes['number']['StringValue'], data)
