import os
import json
import boto3
import logging

def populate_slot_value(val):
    return {
        "value": {
            "interpretedValue": val,
            "originalValue": val,
            "resolvedValues": [
                val
                ]
            }
        }


def session_return_handler(event):
    session = event['sessionState']
    session['dialogAction'] = {'type': 'Delegate'}
    return {"sessionState": session}
    
def get_user_suggestions_from_table():
    user_table_name = 'user_suggestions'
    dynamodb = boto3.resource('dynamodb')
    suggestions = []
    try:
        table = dynamodb.Table(user_table_name)
        response = table.get_item(Key={'user_id': 'user1'}, TableName=user_table_name)
        suggestions = response['Item']['suggestions']
    except error as e:
        logging.error(e)
    return suggestions

def lambda_handler(event, context):
    
    
    if (event['sessionState']['intent']['name'] == 'ExistingSuggestionIntent'):
        
        slots = event['interpretations'][0]['intent']['slots']
        
        if(event['sessionState']['intent']['slots']['suggestions']['value']['interpretedValue'] =='yes'):
            suggestions = get_user_suggestions_from_table()
            
            if(len(suggestions)>0):
                event['sessionState']['intent']['slots']['suggestions'] = populate_slot_value('Some of the suggestions are: <br>')
                for i in range(len(suggestions)):
                    event['sessionState']['intent']['slots']['suggestions']['value']['interpretedValue'] += suggestions[i] + "<br>"
            else:
                event['sessionState']['intent']['slots']['suggestions'] = populate_slot_value('No suggestions available') 
        
    else:
        
        try:
            slots = event['interpretations'][0]['intent']['slots']
            
            # Pushing values to SQS
            sqs_client = boto3.client("sqs", region_name="us-east-1")
    
            response = sqs_client.send_message(
                QueueUrl="https://sqs.us-east-1.amazonaws.com/237440262663/cloud-as1-search-parameters-queue",
                MessageAttributes={
                    'cuisine': {
                        'DataType': 'String',
                        'StringValue': slots['Cuisine']['value']['interpretedValue']
                    },
                    'location': {
                        'DataType': 'String',
                        'StringValue': slots['Location']['value']['interpretedValue']
                    },
                    'email': {
                        'DataType': 'String',
                        'StringValue': slots['EmailAddress']['value']['interpretedValue']
                    },
                    'time': {
                        'DataType': 'String',
                        'StringValue': slots['DineTime']['value']['interpretedValue']
                    },
                    'date': {
                        'DataType': 'String',
                        'StringValue': slots['DineDate']['value']['interpretedValue']
                    },
                    'number': {
                        'DataType': 'Number',
                        'StringValue': slots['PeopleCount']['value']['interpretedValue']
                    }
                },
                MessageBody=(
                    'Details for Dining'
                )
            )
            
        except:
            print("ERROR: Could not send message to SQS")
            pass
    
    return session_return_handler(event)
