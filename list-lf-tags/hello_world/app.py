import json
import os
from urllib import response
import boto3
client = boto3.client('lakeformation')

def exception_handler(e,tag_key):
    # build exception response here
    status_code = 400
    resp_body_item = {
        "tag_key": tag_key,
        # "error": str(e),
        "error": "EntityNotFoundException",
        "error_message": "string",
        }
    resp_body = {
        "statusCode": status_code,
        "body": resp_body_item
    } 
    resp_body_formatted = json.dumps(
        resp_body,
        indent = 4, 
        separators = (", ", " : "), 
    ) 
    return {
        "statusCode": status_code,
        "body": resp_body_formatted
    }

def list_tags(event):
    # build list lf tags response here
    event_body = json.loads(event['body'])
    tag_keys = event_body['tag_key']
    resp_body_item = []
    for tag_key in tag_keys:
        try:
            status_code = 207
            response = client.get_lf_tag(TagKey=tag_key)
        except Exception as e:
             # if there is an exception call the exception_handler
            return exception_handler(e,tag_key)
        resp_body_item.append({
              "catalog_id": response['CatalogId'],
              "tag_key": tag_key,
              "tag_values": response['TagValues'],
        }) 
    # build normal response    
    resp_body = {
        "statusCode": status_code,
        "body": resp_body_item
    }    
    resp_body_formatted = json.dumps(
        resp_body,
        indent = 4, 
        separators = (", ", " : "), 
    ) 
    return {
        'statusCode': status_code,
        'body': resp_body_formatted
          }     
def handler(event, context):
    """
    Lambda Entrypoint
    """
    return list_tags(event)