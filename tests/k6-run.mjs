import { execSync } from "child_process";
import * as dotenv from "dotenv";
import {
  ApiGatewayV2Client,
  GetApisCommand,
} from "@aws-sdk/client-apigatewayv2";

const API_NAME = "async-api-sample--api-gw";

async function getApiAsync() {
  const apiClient = new ApiGatewayV2Client();

  const response = await apiClient.send(new GetApisCommand({}));
  const api = response.Items.filter((x) => x.Name === API_NAME)[0];

  if (!api) {
    throw new Error(`Not found : '${API_NAME}'`);
  }
  return api.ApiEndpoint;
}

function getEnv(apiEndpoint) {
  const envResult = dotenv.config();
  if (envResult.error) {
    throw envResult.error;
  }

  const env = { ...envResult.parsed, ...{ API_URL_BASE: apiEndpoint } };
  return env;
}

async function main() {
  const api = await getApiAsync();

  const env = getEnv(api.ApiEndpoint);

  const args = process.argv.slice(2).join(" ");

  const cmd = `k6 run ${args}`;

  console.log(process.cwd());
  console.log(cmd);
  console.log(env);
  console.log(api);

  try {
    execSync(cmd, { stdio: "inherit", env: { ...env } });
  } catch {
    process.exit(1);
  }
}

await main();
