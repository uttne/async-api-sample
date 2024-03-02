import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
// import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as apigw from "aws-cdk-lib/aws-apigatewayv2";
import { DynamoDbOpeQueueIntegration } from "./integrations/dynamodb-ope-queue";
// import { S3PubSubIntegration } from "./integrations/s3-pub-sub";
import * as logs from "aws-cdk-lib/aws-logs";

export class AsyncApiSampleStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const api = new apigw.HttpApi(this, "Api", {
      apiName: "async-api-sample--api-gw",
    });

    const logGroup = new logs.LogGroup(this, "ApiLog", {
      retention: logs.RetentionDays.ONE_DAY,
    });

    new apigw.CfnStage(this, "ApiStage", {
      apiId: api.apiId,
      stageName: "dev",
      autoDeploy: true,
      accessLogSettings: {
        destinationArn: logGroup.logGroupArn,
        format: JSON.stringify({
          requestId: "$context.requestId",
          ip: "$context.identity.sourceIp",
          caller: "$context.identity.caller",
          user: "$context.identity.user",
          requestTime: "$context.requestTime",
          httpMethod: "$context.httpMethod",
          resourcePath: "$context.resourcePath",
          status: "$context.status",
          protocol: "$context.protocol",
          responseLength: "$context.responseLength",
          "integration.error": "$context.integration.error",
          "integration.integrationStatus":
            "$context.integration.integrationStatus",
          "integration.latency": "$context.integration.latency",
          "integration.requestId": "$context.integration.requestId",
          "integration.status": "$context.integration.status",
          integrationErrorMessage: "$context.integrationErrorMessage",
        }),
      },
    });

    // const s3PubSubIntegration = new S3PubSubIntegration(
    //   this,
    //   "async-api-sample--s3-pub-sub--"
    // );

    // api.addRoutes({
    //   path: "/s3",
    //   methods: [apigw.HttpMethod.POST, apigw.HttpMethod.GET],
    //   integration: s3PubSubIntegration.integration,
    // });

    const dynamoDbOpeQueueIntegration = new DynamoDbOpeQueueIntegration(
      this,
      "async-api-sample--dynamodb-ope-queue--"
    );

    api.addRoutes({
      path: "/dy-queue",
      methods: [
        apigw.HttpMethod.POST,
        apigw.HttpMethod.GET,
        apigw.HttpMethod.DELETE,
        apigw.HttpMethod.PATCH,
      ],
      integration: dynamoDbOpeQueueIntegration.integration,
    });
  }
}
