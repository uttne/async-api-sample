import json
import re
import os
import boto3
from botocore.exceptions import ClientError
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

    INSERT = "insert"
    DROP = "drop"

    def __init__(self, src: dict | None = None):
        if (src):
            super().__init__(src)

        if "v" not in self:
            self["v"] = "1"

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


class Result(dict):

    def __init__(self, src: dict | None = None):
        if (src):
            super().__init__(src)

        if "status" not in self:
            self["status"] = ""

        if "message" not in self:
            self["message"] = ""

    @property
    def status(self) -> str:
        return self["status"]

    @status.setter
    def status(self, value: str) -> None:
        self["status"] = value

    @property
    def message(self) -> str:
        return self["message"]

    @message.setter
    def message(self, value: str) -> None:
        self["message"] = value


def put_ope(ckey_suffix: str, skey: str, data: str, method: str = Ope.INSERT):

    ckey = "TEST" + ckey_suffix
    ttl = int(time.time()) + 60

    ope = Ope()
    ope.method = method
    ope.data = data

    response = TABLE.put_item(
        Item={
            DYNAMODB_CHUNK_KEY_NAME: ckey,
            DYNAMODB_SORT_KEY_NAME: skey,
            DYNAMODB_TTL_ITEM_NAME: ttl,
            DYNAMODB_OPE_ITEM_NAME: ope,
        }
    )


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
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return Db()
        raise
    except:
        raise


def cp(src_skey: str | None = None, dest_skey: str | None = None):

    src_object_key = S3_DB_SNAPSHOT_FOLDER + src_skey + ".json" if src_skey else (
        S3_DEFAULT_DB_KEY
    )

    dest_object_key = S3_DB_SNAPSHOT_FOLDER + dest_skey + ".json" if dest_skey else (
        S3_DEFAULT_DB_KEY
    )
    src = {
        "Bucket": S3_BUKET_NAME,
        "Key": src_object_key
    }
    try:
        S3.copy(src, S3_BUKET_NAME, dest_object_key)
    except Exception as e:
        print(
            f"[Failed] S3 Copy src : {src_object_key}, dest : {dest_object_key}")
        print(e)
        raise

# ------------------------------------------------------------------


def do(db: Db, ope: Ope) -> Result:

    if ope.method == Ope.INSERT:
        db.data.append(ope.data)
    elif ope.method == Ope.DROP:
        db.data.clear()
    res = Result()

    res.status = "ok"
    res.message = ""

    return res


def new_skey(top_skey: str | None = None) -> str:
    if not top_skey:
        return str(ulid.new())
    while (skey := str(ulid.new())) <= top_skey:
        pass
    return skey


def post_data(data: str, method: str):

    db = load()

    print(f"[debug] : {data} : loaded db : {db.skey}")

    current_skey = new_skey(db.skey)
    print(f"[debug] : {data} : new skey : {current_skey}")

    skey = current_skey
    put_ope(ckey_suffix=DYNAMODB_SORT_KEY_OPE_SUFIX,
            skey=skey, data=data, method=method)
    print(f"[debug] : {data} : write ope : {current_skey}")

    items = query(ckey_suffix=DYNAMODB_SORT_KEY_OPE_SUFIX, top_skey=db.skey)

    items = sorted(items, key=lambda x: x[DYNAMODB_SORT_KEY_NAME])
    print(
        f"[debug] : {data} : query ope : {','.join([i[DYNAMODB_SORT_KEY_NAME] for i in items])}")

    current_res = Result()
    for item in items:
        skey = item[DYNAMODB_SORT_KEY_NAME]
        ope = Ope(item[DYNAMODB_OPE_ITEM_NAME])

        db.skey = skey

        res = do(db=db, ope=ope)

        if skey == current_skey:
            current_res = res

    save(db_obj=db, skey=skey)
    print(f"[debug] : {data} : snap db : {skey}")

    put_ope(ckey_suffix=DYNAMODB_SORT_KEY_SAV_SUFIX, skey=skey, data=skey)
    print(f"[debug] : {data} : write db : {skey}")

    sav_items = query(ckey_suffix=DYNAMODB_SORT_KEY_SAV_SUFIX, top_skey=skey)

    last_sav_item = next(
        iter(sorted(sav_items, key=lambda x: x[DYNAMODB_SORT_KEY_NAME], reverse=True)), None)
    print(
        f"[debug] : {data} : query last db : {last_sav_item[DYNAMODB_SORT_KEY_NAME] if last_sav_item else None}")
    if not last_sav_item:
        save(db_obj=db)
        print(f"[debug] : {data} : save db : {skey}")
    else:
        src_skey = last_sav_item[DYNAMODB_SORT_KEY_NAME]
        cp(src_skey=src_skey)
        print(f"[debug] : {data} : cp db : {src_skey}")

    return current_res


def get_db() -> dict:
    return load()

# ------------------------------------------------------------------


def handler(event, context):

    # print("event")
    # print(event)
    # print("context")
    # print(context)

    path = convert_path(event)
    method = convert_method(event)
    body_data = convert_body_data(event)
    parameters = convert_parameters(event)

    if (m := re.match(r".*[/]dy-queue", path)):
        if method == "GET":
            db = get_db()
            return {
                'statusCode': 200,
                'body': json.dumps(db)
            }
        elif method == "POST":
            res = post_data(data=body_data["data"], method=Ope.INSERT)
            return {
                'statusCode': 200,
                'body': json.dumps(res)
            }
        elif method == "DELETE":
            res = post_data(data="", method=Ope.DROP)
            return {
                'statusCode': 200,
                'body': json.dumps(res)
            }

    return RESPONSE_404
