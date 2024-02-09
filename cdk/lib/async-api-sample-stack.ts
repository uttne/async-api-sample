import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
// import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as notifications from "aws-cdk-lib/aws-s3-notifications";
import * as iam from "aws-cdk-lib/aws-iam";
import * as path from "path";

export class AsyncApiSampleStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const bucket = new s3.Bucket(this, "ApiBucket", {
      bucketName: "async-api-sample--api-bucket",
      versioned: false,
      // 検証環境で削除ができるように指定する
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      lifecycleRules: [
        {
          id: "DeleteExpiredData",
          enabled: true,
          prefix: "topic/",
          expiration: cdk.Duration.days(1),
        },
      ],
    });

    const responseBucket = new s3.Bucket(this, "ResponseBucket", {
      bucketName: "async-api-sample--response-bucket",
      versioned: false,
      // 検証環境で削除ができるように指定する
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      lifecycleRules: [
        {
          id: "DeleteExpiredData",
          enabled: true,
          prefix: "reponse/",
          expiration: cdk.Duration.days(1),
        },
      ],
    });

    const subscriberFunction = new lambda.Function(this, "SubscriberFunction", {
      functionName: "async-api-sample--subscriber-function",
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "main.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/subscriber")),
    });

    const writePolicy = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["s3:PutObject"],
      resources: [responseBucket.bucketArn + "/*"],
    });

    const readPolicy = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["s3:GetObject"],
      resources: [bucket.bucketArn + "/*"],
    });

    subscriberFunction.addToRolePolicy(readPolicy);
    subscriberFunction.addToRolePolicy(writePolicy);

    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED_PUT,
      new notifications.LambdaDestination(subscriberFunction)
      // { prefix: "topic/", suffix: ".json" }
    );

    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED_POST,
      new notifications.LambdaDestination(subscriberFunction)
      // { prefix: "topic/", suffix: ".json" }
    );
  }
}
