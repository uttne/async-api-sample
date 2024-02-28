import http from "k6/http";

const API_URL_BASE = __ENV.API_URL_BASE;

export const options = {
  vus: 10,
  thresholds: {
    http_req_failed: ["rate<0.01"], // リクエストの失敗率は1%未満
    http_req_duration: ["p(95)<200"], // 95%のリクエストで処理時間が200ms未満
  },
};

export default function () {
  http.get(API_URL_BASE + "/dy-queue");
}
