import json
import re
import os
import boto3
from botocore.exceptions import ClientError
import time
import ulid
import boto3.dynamodb.conditions as cond

import logging
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL") or "INFO")
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

DEBUG_DATA = ""
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

    logger.info({"msg": "put to dynamodb - before",
                "data": DEBUG_DATA, "ckey": ckey, "skey": skey})
    response = TABLE.put_item(
        Item={
            DYNAMODB_CHUNK_KEY_NAME: ckey,
            DYNAMODB_SORT_KEY_NAME: skey,
            DYNAMODB_TTL_ITEM_NAME: ttl,
            DYNAMODB_OPE_ITEM_NAME: ope,
        }
    )
    logger.info({"msg": "put to dynamodb - after",
                "data": DEBUG_DATA, "ckey": ckey, "skey": skey})


def set_current_skey(skey: str) -> bool:
    chunk_key = "TEST_META"
    sort_key = "CURRENT_DB"

    try:
        logger.info({"msg": "check current db skey from dynamo - before",
                     "data": DEBUG_DATA, "ckey": chunk_key, "skey": sort_key, "cur": skey})
        response = TABLE.update_item(
            Key={
                DYNAMODB_CHUNK_KEY_NAME: chunk_key,
                DYNAMODB_SORT_KEY_NAME: sort_key
            },
            UpdateExpression='SET cur = :newSkey',
            ExpressionAttributeValues={
                ':newSkey': skey
            },
            ConditionExpression='attribute_not_exists(cur) OR cur <= :newSkey',
        )
        logger.info({"msg": "check current db skey from dynamo - after",
                     "data": DEBUG_DATA, "ckey": chunk_key, "skey": sort_key, "cur": skey})
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] != 'ConditionalCheckFailedException':
            raise
        else:
            return False


def get_current_skey() -> str:
    chunk_key = "TEST_META"
    sort_key = "CURRENT_DB"

    logger.info({"msg": "get current db skey from dynamo - before",
                 "data": DEBUG_DATA, "ckey": chunk_key, "skey": sort_key})
    response = TABLE.get_item(
        Key={
            DYNAMODB_CHUNK_KEY_NAME: chunk_key,
            DYNAMODB_SORT_KEY_NAME: sort_key
        },
        ConsistentRead=True,
    )
    logger.info({"msg": "get current db skey from dynamo - after",
                 "data": DEBUG_DATA, "ckey": chunk_key, "skey": sort_key})

    skey = response['Item']["cur"] if ('Item' in response) else ""

    logger.info({"msg": "get current skey",
                 "data": DEBUG_DATA, "ckey": chunk_key, "skey": sort_key, "cur": skey})

    return skey


def query(ckey_suffix: str, top_skey: str | None = None, last_skey: str | None = None) -> list[dict]:

    kce = cond.Key(DYNAMODB_CHUNK_KEY_NAME).eq("TEST" + ckey_suffix)

    if top_skey and last_skey:
        kce = kce & cond.Key(DYNAMODB_SORT_KEY_NAME).between(
            top_skey, last_skey)
    elif top_skey:
        kce = kce & cond.Key(DYNAMODB_SORT_KEY_NAME).gte(top_skey)
    elif last_skey:
        kce = kce & cond.Key(DYNAMODB_SORT_KEY_NAME).lte(last_skey)

    logger.info({"msg": "query to dynamodb - before", "data": DEBUG_DATA,
                 "ckey_suffix": ckey_suffix, "top_skey": top_skey, "last_skey": last_skey})
    response = TABLE.query(
        KeyConditionExpression=kce,
        ConsistentRead=True,
        # Limit=5
    )
    logger.info({"msg": "query to dynamodb - after", "data": DEBUG_DATA,
                 "ckey_suffix": ckey_suffix, "top_skey": top_skey})

    items = response["Items"]

    logger.info({"msg": "query items", "data": DEBUG_DATA,
                 "ckey_suffix": ckey_suffix, "top_skey": top_skey, "skeys": [i[DYNAMODB_SORT_KEY_NAME] for i in items]})

    return items


def save(db_obj: Db, skey: str | None = None):

    object_key = (S3_DB_SNAPSHOT_FOLDER + skey + ".json") if skey else (
        S3_DEFAULT_DB_KEY
    )
    data = json.dumps(db_obj).encode("utf-8")

    logger.info({"msg": "save to s3 - before",
                "data": DEBUG_DATA, "object_key": object_key})
    S3.put_object(
        Body=data, Bucket=S3_BUKET_NAME, Key=object_key
    )
    logger.info({"msg": "save to s3 - after",
                "data": DEBUG_DATA, "object_key": object_key})


def load(skey: str | None = None) -> Db:

    object_key = S3_DB_SNAPSHOT_FOLDER + skey + ".json" if skey else (
        S3_DEFAULT_DB_KEY
    )

    try:
        logger.info({"msg": "load from s3 - before",
                    "data": DEBUG_DATA, "object_key": object_key})
        response = S3.get_object(Bucket=S3_BUKET_NAME, Key=object_key)
        logger.info({"msg": "load from s3 - after",
                    "data": DEBUG_DATA, "object_key": object_key})

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
        logger.info({"msg": "cp to s3 - before", "data": DEBUG_DATA,
                     "src": src_object_key, "dest": dest_object_key})
        S3.copy(src, S3_BUKET_NAME, dest_object_key)
        logger.info({"msg": "cp to s3 - after", "data": DEBUG_DATA,
                     "src": src_object_key, "dest": dest_object_key})
    except Exception as e:
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

    current_skey = new_skey(db.skey)

    skey = current_skey
    put_ope(ckey_suffix=DYNAMODB_SORT_KEY_OPE_SUFIX,
            skey=skey, data=data, method=method)

    # 自分より前に実行される必要のある他のプロセスの ope がコミットされるのを待つ
    time.sleep(0.05)

    items = query(ckey_suffix=DYNAMODB_SORT_KEY_OPE_SUFIX,
                  top_skey=db.skey, last_skey=skey)

    # ここで取得できる操作に抜けが発生するのは自分が登録した操作以降のデータも取得しようとすると
    # 自分の書き込み以降のデータ取得についても抜けがないかリスクを負うことになるので
    # 自分の操作までで取得を止める
    # 自分の操作以降も取得できると効率化にはつながるので何か対策があれば実施

    items = sorted(items, key=lambda x: x[DYNAMODB_SORT_KEY_NAME])

    current_res = Result()
    for item in items:
        skey = item[DYNAMODB_SORT_KEY_NAME]

        if skey == db.skey:
            continue

        ope = Ope(item[DYNAMODB_OPE_ITEM_NAME])

        db.skey = skey

        res = do(db=db, ope=ope)

        if skey == current_skey:
            current_res = res

    save(db_obj=db, skey=skey)

    # put_ope(ckey_suffix=DYNAMODB_SORT_KEY_SAV_SUFIX, skey=skey, data=skey)

    # # sav については取得データに途中に抜けがあっても結果的に整合するので待機はしない

    # sav_items = query(ckey_suffix=DYNAMODB_SORT_KEY_SAV_SUFIX, top_skey=skey)

    # last_sav_item = next(
    #     iter(sorted(sav_items, key=lambda x: x[DYNAMODB_SORT_KEY_NAME], reverse=True)), None)

    # src_skey = last_sav_item[DYNAMODB_SORT_KEY_NAME] if last_sav_item else skey

    # # 他の Lambda によって過去のバージョンに DB ファイルが戻されている可能性があるため
    # # 現在の実行によって保存されたDBのバージョンよりも最新のDBファイルのバージョンが新しくなっていることを確認する
    # sleep_time = 0
    # while (last_db := load()) and (last_db.skey < src_skey):
    #     cp(src_skey=src_skey)
    #     time.sleep(sleep_time)
    #     sleep_time = 0.1

    while set_current_skey(skey=skey):
        cp(src_skey=skey)
        current_skey = get_current_skey()
        if skey == current_skey:
            break
        skey = current_skey

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

    global DEBUG_DATA
    DEBUG_DATA = body_data.get("data")

    logger.info({"msg": "prcess start", "data": DEBUG_DATA})
    try:
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
    except Exception as e:
        logger.exception({"msg": "Failed", "exception": f"{e}"})
        return {
            'statusCode': 500,
            'body': f"{e}"
        }
    finally:
        logger.info({"msg": "prcess end", "data": DEBUG_DATA})
