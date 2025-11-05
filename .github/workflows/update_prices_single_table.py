import os
import json
import time
import math
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

BASE = "https://open.feishu.cn/open-apis"

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET")
FEISHU_APP_TOKEN = os.getenv("FEISHU_APP_TOKEN")
TABLE_ID = os.getenv("TABLE_ID")

CODE_FIELD = os.getenv("CODE_FIELD", "代码")
PRICE_FIELD = os.getenv("PRICE_FIELD", "最新价")
UPDATED_AT_FIELD = os.getenv("UPDATED_AT_FIELD", "最新价更新时间")

class FeishuClient:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token = None

    def get_tenant_access_token(self):
        url = f"{BASE}/auth/v3/tenant_access_token/internal"
        resp = requests.post(url, json={"app_id": self.app_id, "app_secret": self.app_secret})
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Get token failed: {data}")
        self.token = data["data"]["tenant_access_token"]
        return self.token

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json; charset=utf-8"}

    def list_records(self, app_token, table_id, page_size=500):
        url = f"{BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        items = []
        page_token = None
        while True:
            params = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            resp = requests.get(url, headers=self._headers(), params=params)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != 0:
                raise RuntimeError(f"List records failed: {data}")
            items.extend(data["data"]["items"])
            page_token = data["data"].get("page_token")
            if not page_token:
                break
        return items

    def update_record(self, app_token, table_id, record_id, fields: dict):
        url = f"{BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        resp = requests.patch(url, headers=self._headers(), data=json.dumps({"fields": fields}, ensure_ascii=False))
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Update record failed: {data}")
        return data["data"]

def normalize_symbol(symbol: str) -> str:
    s = str(symbol).strip().upper()
    if s.endswith(".SH"):
        s = s[:-3] + ".SS"
    return s

def fetch_prices(symbols):
    import yfinance as yf
    results = {}
    for sym in symbols:
        yf_sym = normalize_symbol(sym)
        try:
            t = yf.Ticker(yf_sym)
            price = None
            fi = getattr(t, "fast_info", None)
            if fi and "last_price" in fi:
                price = fi["last_price"]
            if price is None:
                hist = t.history(period="1d")
                if not hist.empty:
                    price = float(hist["Close"].iloc[-1])
            if price is None or (isinstance(price, float) and math.isnan(price)):
                print(f"[warn] 无法获取价格: {sym}")
                continue
            results[sym] = float(price)
            time.sleep(0.1)
        except Exception as e:
            print(f"[error] 获取 {sym} 行情失败: {e}")
    return results

def main():
    client = FeishuClient(FEISHU_APP_ID, FEISHU_APP_SECRET)
    client.get_tenant_access_token()

    rows = client.list_records(FEISHU_APP_TOKEN, TABLE_ID)
    records = []
    codes = []
    for r in rows:
        fields = r.get("fields", {})
        code = fields.get(CODE_FIELD)
        if code and str(code).strip():
            code_str = str(code).strip()
            records.append({"record_id": r["record_id"], "code": code_str})
            codes.append(code_str)

    if not codes:
        print(f"在表 {TABLE_ID} 中未发现列“{CODE_FIELD}”或该列为空，请填写代码或设置 CODE_FIELD 环境变量为正确的列名。")
        return

    prices = fetch_prices(codes)
    now_str = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")

    updated = 0
    for rec in records:
        code = rec["code"]
        record_id = rec["record_id"]
        price = prices.get(code)
        if price is None:
            continue
        fields_to_update = {
            PRICE_FIELD: price,
            UPDATED_AT_FIELD: now_str,
        }
        try:
            client.update_record(FEISHU_APP_TOKEN, TABLE_ID, record_id, fields_to_update)
            updated += 1
        except Exception as e:
            print(f"[error] 更新 {code} 失败: {e}")

    print(f"更新完成：{updated}/{len(records)} 行。")

if __name__ == "__main__":
    main()
