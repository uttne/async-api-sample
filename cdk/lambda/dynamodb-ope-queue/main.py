import json
import re
import os
import boto3
import time
import ulid
# ------------------------------------------------------------------


DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
DYNAMODB_KEY_NAME = os.environ["DYNAMODB_KEY_NAME"]
DYNAMODB_TTL_ITEM_NAME = os.environ["DYNAMODB_TTL_ITEM_NAME"]
S3_BUKET_NAME = os.environ["S3_BUKET_NAME"]


# ------------------------------------------------------------------


RESPONSE_404 = {
    'statusCode': 404,
    'body': json.dumps('Not Found')
}
RESPONSE_405 = {
    'statusCode': 405,
    'body': json.dumps('Method Not Allowed')
}


# ------------------------------------------------------------------


def convert_path(event) -> str:
    if not event:
        return ""
    requestContext = event.get("requestContext") or {}
    http = requestContext.get("http") or {}
    path = http.get("path") or ""
    return path


def convert_method(event) -> str:
    if not event:
        return ""

    requestContext = event.get("requestContext") or {}
    http = requestContext.get("http") or {}
    method = http.get("method") or ""
    return method


def convert_parameters(event) -> dict:
    if not event:
        return {}
    return event.get("queryStringParameters") or {}


def convert_body_data(event) -> dict:
    if not event:
        return {}
    return json.loads(event.get("body") or "{}")

# ------------------------------------------------------------------


DYNAMODB = boto3.resource("dynamodb")
TABLE = DYNAMODB.Table(DYNAMODB_TABLE_NAME)


def put_ope():

    key = str(ulid.new())
    ttl = int(time.time()) + 3600
    response = TABLE.put_item(
        Item={
            DYNAMODB_KEY_NAME: key,
            DYNAMODB_TTL_ITEM_NAME: ttl
        }
    )

    print(response)


# ------------------------------------------------------------------


def handler(event, context):

    print("event")
    print(event)
    print("context")
    print(context)

    print(str(ulid.new()))

    path = convert_path(event)
    method = convert_method(event)
    body_data = convert_body_data(event)
    parameters = convert_parameters(event)

    if (m := re.match(r".*[/]dy-queue", path)):
        if method == "GET":
            return {
                'statusCode': 200,
                'body': 'Hello, CDK!'
            }
        elif method == "POST":
            return {
                'statusCode': 200,
                'body': 'Hello, CDK!'
            }

    return RESPONSE_404
