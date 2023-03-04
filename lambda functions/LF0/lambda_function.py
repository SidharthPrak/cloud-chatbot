import json
import time
import boto3


def lambda_handler(event, context):
    
    text_to_bot = 'hi'
    try:
        text_to_bot = event['messages'][0]['unstructured']['text']
    except:
        pass
    
    client = boto3.client('lexv2-runtime', region_name = 'us-east-1')
    
    # Configure bot details
    botId = 'LXW1AUT4FY'
    botAliasId = 'TSTALIASID'
    localeId = 'en_US'
    # We have used a hardcoded user instance here as was confirmed this was for 1 user
    sessionId = '93479311249556'
    
    response = client.recognize_text(
        botId = botId,
        botAliasId = botAliasId,
        localeId = localeId,
        sessionId = sessionId,
        text = text_to_bot
        )
    
    bot_messages = response['messages']
    resp_message_list = []
    for message in bot_messages:
        message_dict = {
            'type': 'unstructured', 
            'unstructured': { 
                'id': 111, 
                'text': message['content'], 
                'timestamp': time.strftime("%H:%M:%S", time.localtime())}
            
        }
        resp_message_list.append(message_dict)
    
    return {
        'statusCode': 200,
        'messages': resp_message_list
    }
