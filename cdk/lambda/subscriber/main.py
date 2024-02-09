import boto3

S3 = boto3.client("s3")


def handler(event, context):
    print(event)

    records = event["Records"]
    for record in records:
        s3_obj = record["s3"]
        bucket = s3_obj["bucket"]["name"]
        key: str = s3_obj["object"]["key"]
        response = S3.get_object(Bucket=bucket, Key=key)
        data = response["Body"].read()

        response_key = "reponse/" + key.split("/")[-1]
        S3.put_object(
            Body=data, Bucket="async-api-sample--response-bucket", Key=response_key)
    return {
        'statusCode': 200,
        'body': 'Hello, CDK!'
    }
