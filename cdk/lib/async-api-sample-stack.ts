import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
// import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as apigw from "aws-cdk-lib/aws-apigatewayv2";
import { S3PubSubIntegration } from "./integrations/s3-pub-sub";

export class AsyncApiSampleStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const s3PubSubIntegration = new S3PubSubIntegration(
      this,
      "async-api-sample--s3-pub-sub--"
    );

    const api = new apigw.HttpApi(this, "Api", {
      apiName: "async-api-sample--api-gw",
    });

    api.addRoutes({
      path: "/s3",
      methods: [apigw.HttpMethod.POST, apigw.HttpMethod.GET],
      integration: s3PubSubIntegration.integration,
    });
  }
}
