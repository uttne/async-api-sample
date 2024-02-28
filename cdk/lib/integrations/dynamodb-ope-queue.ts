import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import { HttpLambdaIntegration } from "aws-cdk-lib/aws-apigatewayv2-integrations";
import * as path from "path";

export class DynamoDbOpeQueueIntegration {
  public integration: HttpLambdaIntegration;
  constructor(scope: Construct, prefix: string) {
    // SQLite3 のファイルを保管するS3
    const dbBucket = new s3.Bucket(scope, prefix + "DbBucket", {
      bucketName: prefix + "db-bucket",
      versioned: false,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      lifecycleRules: [
        {
          id: prefix + "DeleteExpiredData",
          enabled: true,
          prefix: "snapshot/",
          expiration: cdk.Duration.days(1),
        },
      ],
    });

    const opeQueueDynamoDb = new dynamodb.Table(scope, prefix + "QueueTable", {
      tableName: prefix + "queue-table",
      partitionKey: {
        name: "key",
        type: dynamodb.AttributeType.STRING,
      },
      // オンデマンド
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      // Point In Time Recovery は無効化
      pointInTimeRecovery: false,
      // TTL データを格納する属性
      timeToLiveAttribute: "expired",
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const layer = new lambda.LayerVersion(scope, prefix + "OpeFunctionLayer", {
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_12],
      code: lambda.Code.fromAsset(
        path.join(__dirname, "../../lambda/dynamodb-ope-queue/layer")
      ),
      license: "MIT",
    });

    const lambdaFunction = new lambda.Function(scope, prefix + "OpeFunction", {
      functionName: prefix + "ope-function",
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "main.handler",
      code: lambda.Code.fromAsset(
        path.join(__dirname, "../../lambda/dynamodb-ope-queue"),
        {
          exclude: ["layer"],
        }
      ),
      environment: {
        DYNAMODB_TABLE_NAME: opeQueueDynamoDb.tableName,
        DYNAMODB_KEY_NAME: "key",
        DYNAMODB_TTL_ITEM_NAME: "expired",
        S3_BUKET_NAME: dbBucket.bucketName,
      },
      layers: [layer],
    });

    const writePolicy = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["s3:PutObject"],
      resources: [dbBucket.bucketArn + "/*"],
    });

    const readPolicy = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["s3:GetObject"],
      resources: [dbBucket.bucketArn + "/*"],
    });

    lambdaFunction.addToRolePolicy(readPolicy);
    lambdaFunction.addToRolePolicy(writePolicy);

    // Lambda に DynamoDB の読み書きアクセス権限を付与
    opeQueueDynamoDb.grantReadWriteData(lambdaFunction);

    const integration = new HttpLambdaIntegration(
      prefix + "DynamoDbOpeQueueLambdaIntegration",
      lambdaFunction
    );

    this.integration = integration;
  }
}
