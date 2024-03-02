import http from "k6/http";
import { sleep, check } from "k6";
import { randomIntBetween } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
const API_URL_BASE = __ENV.API_URL_BASE;

export const options = {
  vus: 10,
  duration: "5s",
  thresholds: {
    http_req_failed: ["rate<0.01"], // リクエストの失敗率は1%未満
    http_req_duration: ["p(95)<1000"], // 95%のリクエストで処理時間が200ms未満
  },
};

export function setup() {
  console.log("Setup: テスト前にデータの削除を行う");
  const url = API_URL_BASE + "/dy-queue";
  const headers = { "Content-Type": "application/json" };
  const body = JSON.stringify({ data: "" });
  const response = http.del(url, body, { headers: headers });

  if (response.status !== 200) {
    throw new Error("Setup failed: データの削除に失敗しました");
  }
}
export default function () {
  const url = API_URL_BASE + "/dy-queue";
  const headers = { "Content-Type": "application/json" };

  const data = `${("000" + __VU.toString()).slice(-3)}-${(
    "000" + __ITER.toString()
  ).slice(-3)}`;

  const body = JSON.stringify({ data: data });
  const response = http.post(url, body, { headers: headers });

  const res = check(response, {
    "is status 200": (r) => r.status === 200,
  });
  if (!res) {
    console.log(`failed. status: ${response.status}, body: ${response.body}`);
  }
  console.log(data + " : " + response.timings.duration.toString());
  sleep(randomIntBetween(100, 500) / 250);
}

export function teardown() {
  console.log("Teardown: テスト後のデータ確認");

  const url = API_URL_BASE + "/dy-queue";
  const response = http.get(url);

  if (response.status !== 200) {
    console.log("データ取得失敗");
  } else {
    const body = JSON.parse(response.body);
    console.log(`skey : ${body.skey}`);
    console.log(`data.length : ${body.data.length}`);
    console.log(body.data.slice().sort());
  }
}
