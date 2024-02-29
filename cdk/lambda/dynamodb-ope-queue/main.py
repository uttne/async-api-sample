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

DYNAMODB_SORT_KEY_OPE_SUFIX = "_OPE"
DYNAMODB_SORT_KEY_SAV_SUFIX = "_SAV"

S3_BUKET_NAME = os.environ["S3_BUKET_NAME"]

S3_DEFAULT_DB_KEY = "db.json"

S3_DB_SNAPSHOT_FOLDER = "snapshot/"


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

S3 = boto3.client("s3")


class Db(dict):

    def __init__(self, src: dict | None = None):
        if (src):
            super().__init__(src)

    @property
    def skey(self) -> str | None:
        return self.get("skey")

    @skey.setter
    def skey(self, value: str | None) -> None:
        self["skey"] = value

    @property
    def data(self) -> list[str]:

        value = self.get("data")
        if not value:
            self["data"] = value = []
        return value

    @data.setter
    def data(self, value: list[str]) -> None:
        self["data"] = value


class Ope(dict):

    def __init__(self, src: dict | None = None):
        if (src):
            super().__init__(src)

    @property
    def version(self) -> str:
        return self["v"]

    @version.setter
    def version(self, value: str) -> None:
        self["v"] = value

    @property
    def method(self) -> str:
        return self["m"]

    @method.setter
    def method(self, value: str) -> None:
        self["m"] = value

    @property
    def data(self) -> str:
        return self["d"]

    @data.setter
    def data(self, value: str) -> None:
        self["d"] = value


def new_ope_skey(ulid_text: str) -> str:
    return DYNAMODB_SORT_KEY_OPE_SUFIX + ulid_text


def new_sav_skey(ulid_text: str) -> str:
    return DYNAMODB_SORT_KEY_SAV_SUFIX + ulid_text


def put_ope(ckey_suffix: str, skey: str, data: str):

    ckey = "TEST" + ckey_suffix
    ttl = int(time.time()) + 60

    ope = Ope()
    ope.version = "1"
    ope.method = "insert"
    ope.data = data

    response = TABLE.put_item(
        Item={
            DYNAMODB_CHUNK_KEY_NAME: ckey,
            DYNAMODB_SORT_KEY_NAME: skey,
            DYNAMODB_TTL_ITEM_NAME: ttl,
            DYNAMODB_OPE_ITEM_NAME: ope,
        }
    )

    print(response)


def query(ckey_suffix: str, top_skey: str | None) -> list[dict]:

    if top_skey:
        kce = cond.Key(DYNAMODB_CHUNK_KEY_NAME).eq("TEST" + ckey_suffix) \
            & cond.Key(DYNAMODB_SORT_KEY_NAME).gt(top_skey)
    else:
        kce = cond.Key(DYNAMODB_CHUNK_KEY_NAME).eq("TEST" + ckey_suffix)

    response = TABLE.query(
        KeyConditionExpression=kce,
        ConsistentRead=True,
        # Limit=5
    )

    print(response)
    items = response["Items"]

    return items


def save(db_obj: Db, skey: str | None = None):

    object_key = (S3_DB_SNAPSHOT_FOLDER + skey + ".json") if skey else (
        S3_DEFAULT_DB_KEY
    )
    data = json.dumps(db_obj).encode("utf-8")
    S3.put_object(
        Body=data, Bucket=S3_BUKET_NAME, Key=object_key
    )


def load(skey: str | None = None) -> Db:

    object_key = S3_DB_SNAPSHOT_FOLDER + skey + ".json" if skey else (
        S3_DEFAULT_DB_KEY
    )

    try:
        response = S3.get_object(Bucket=S3_BUKET_NAME, Key=object_key)

        data_text = response["Body"].read().decode("utf-8")
        return Db(json.loads(data_text))
    except:
        return Db()

# ------------------------------------------------------------------


def post():

    ulid_text = str(ulid.new())

    db = load()

    skey = ulid_text
    put_ope(ckey_suffix=DYNAMODB_SORT_KEY_OPE_SUFIX, skey=skey, data=skey)

    items = query(ckey_suffix=DYNAMODB_SORT_KEY_OPE_SUFIX, top_skey=db.skey)

    items = sorted(items, key=lambda x: x[DYNAMODB_SORT_KEY_NAME])

    for item in items:
        skey = item[DYNAMODB_SORT_KEY_NAME]
        ope = Ope(item[DYNAMODB_OPE_ITEM_NAME])

        db.skey = skey
        db.data.append(ope.data)

    save(db_obj=db, skey=skey)

    skey = ulid_text
    put_ope(ckey_suffix=DYNAMODB_SORT_KEY_SAV_SUFIX, skey=skey, data=skey)

    sav_items = query(ckey_suffix=DYNAMODB_SORT_KEY_SAV_SUFIX, top_skey=skey)

    last_sav_item = next(
        iter(sorted(sav_items, key=lambda x: x[DYNAMODB_SORT_KEY_NAME], reverse=True)), None)

    if not last_sav_item:
        save(db_obj=db)
        return db
    else:
        ope = Ope(last_sav_item[DYNAMODB_OPE_ITEM_NAME])
        last_db = load(skey=ope.data)
        save(db_obj=last_db)
        return last_db


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
            db = post()
            return {
                'statusCode': 200,
                'body': json.dumps(db)
            }

    return RESPONSE_404
