import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
// import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as notifications from "aws-cdk-lib/aws-s3-notifications";
import * as iam from "aws-cdk-lib/aws-iam";
import * as path from "path";
import * as apigw from "aws-cdk-lib/aws-apigatewayv2";
import { HttpLambdaIntegration } from "aws-cdk-lib/aws-apigatewayv2-integrations";

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
      // 同時実行数を1に制限
      reservedConcurrentExecutions: 1,
    });

    const publisherFunction = new lambda.Function(this, "PublisherFunction", {
      functionName: "async-api-sample--publisher-function",
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "main.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/publisher")),
      // 同時実行数を1に制限
      reservedConcurrentExecutions: 1,
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

    publisherFunction.addToRolePolicy(readPolicy);
    publisherFunction.addToRolePolicy(writePolicy);

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

    const api = new apigw.HttpApi(this, "Api", {
      apiName: "async-api-sample--api-gw",
    });

    const s3Integration = new HttpLambdaIntegration(
      "S3LambdaIntegration",
      publisherFunction
    );

    api.addRoutes({
      path: "/s3",
      methods: [apigw.HttpMethod.POST, apigw.HttpMethod.GET],
      integration: s3Integration,
    });
  }
}
