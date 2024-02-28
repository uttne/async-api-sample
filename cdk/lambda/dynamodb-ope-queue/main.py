import json
import re
import os
import boto3
import time
import ulid
import boto3.dynamodb.conditions as cond
# ------------------------------------------------------------------


DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
DYNAMODB_CHUNK_KEY_NAME = os.environ["DYNAMODB_CHUNK_KEY_NAME"]
DYNAMODB_SORT_KEY_NAME = os.environ["DYNAMODB_SORT_KEY_NAME"]
DYNAMODB_TTL_ITEM_NAME = os.environ["DYNAMODB_TTL_ITEM_NAME"]
DYNAMODB_OPE_ITEM_NAME = "ope"

DYNAMODB_SORT_KEY_PREFIX = "OPE_"

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

    ckey = "TEST"
    skey = DYNAMODB_SORT_KEY_PREFIX + str(ulid.new())
    ttl = int(time.time()) + 3600
    ope = {
        "v": "1",
        "m": "insert",
        "d": skey
    }
    response = TABLE.put_item(
        Item={
            DYNAMODB_CHUNK_KEY_NAME: ckey,
            DYNAMODB_SORT_KEY_NAME: skey,
            DYNAMODB_TTL_ITEM_NAME: ttl,
            DYNAMODB_OPE_ITEM_NAME: ope,
        }
    )

    print(response)


def query():

    top_skey = DYNAMODB_SORT_KEY_PREFIX + str(ulid.new())
    response = TABLE.query(
        KeyConditionExpression=cond.Key(DYNAMODB_CHUNK_KEY_NAME).eq("TEST")
        & cond.Key(DYNAMODB_SORT_KEY_NAME).gte(top_skey),
        FilterExpression=cond.Attr(DYNAMODB_SORT_KEY_NAME).begins_with(
            DYNAMODB_SORT_KEY_PREFIX),
        # Limit=5
    )

    print(response)
    items = response["Items"]

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
            put_ope()
            return {
                'statusCode': 200,
                'body': 'Hello, CDK!'
            }

    return RESPONSE_404
