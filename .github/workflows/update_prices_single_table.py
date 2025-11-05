import os
import json
import time
import math
import requests
from datetime import datetime
from zoneinfo import ZoneInfo # Python 3.9+ 引入

# 如果你的 Python 版本低于 3.9，请安装并使用 pytz 库
# from pytz import timezone
# def get_shanghai_time():
#     return datetime.now(timezone('Asia/Shanghai'))

BASE = "https://open.feishu.cn/open-apis"

# 从环境变量获取飞书应用凭证和表格信息
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET")
FEISHU_APP_TOKEN = os.getenv("FEISHU_APP_TOKEN")
TABLE_ID = os.getenv("TABLE_ID")

# 列名可通过环境变量覆盖；默认用中文列名
CODE_FIELD = os.getenv("CODE_FIELD", "代码")
PRICE_FIELD = os.getenv("PRICE_FIELD", "最新价")
UPDATED_AT_FIELD = os.getenv("UPDATED_AT_FIELD", "最新价更新时间")

def assert_env():
    """检查所有必要的环境变量是否已设置。"""
    missing = []
    for k in ["FEISHU_APP_ID", "FEISHU_APP_SECRET", "FEISHU_APP_TOKEN", "TABLE_ID"]:
        if not os.getenv(k):
            missing.append(k)
    if missing:
        raise SystemExit(f"缺少环境变量：{', '.join(missing)}")

class FeishuClient:
    """飞书开放平台客户端，用于获取 Access Token 和操作多维表格。"""
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token = None # tenant_access_token

    def get_tenant_access_token(self):
        """获取企业自建应用的 tenant_access_token。"""
        url = f"{BASE}/auth/v3/tenant_access_token/internal"
        resp = requests.post(url, json={"app_id": self.app_id, "app_secret": self.app_secret})
        resp.raise_for_status() # 如果请求失败（非2xx状态码），会抛出异常
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"获取 tenant_access_token 失败: {data}")
        self.token = data["data"]["tenant_access_token"]
        print("成功获取 tenant_access_token。")
        return self.token

    def _headers(self):
        """构造带有 Authorization 头部的请求头。"""
        if not self.token:
            self.get_tenant_access_token() # 确保 token 已获取
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json; charset=utf-8"}

    def list_records(self, app_token, table_id, page_size=500):
        """列出多维表格中的所有记录。"""
        url = f"{BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        items = []
        page_token = None
        print(f"正在从表 {table_id} 获取记录...")
        while True:
            params = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            resp = requests.get(url, headers=self._headers(), params=params)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != 0:
                raise RuntimeError(f"列出记录失败: {data}")
            items.extend(data["data"]["items"])
            page_token = data["data"].get("page_token")
            if not page_token:
                break
        print(f"成功获取 {len(items)} 条记录。")
        return items

    def update_record(self, app_token, table_id, record_id, fields: dict):
        """更新多维表格中的单条记录。"""
        url = f"{BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        # ensure_ascii=False 确保中文字符正确编码
        resp = requests.patch(url, headers=self._headers(), data=json.dumps({"fields": fields}, ensure_ascii=False))
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"更新记录 {record_id} 失败: {data}")
        return data["data"]

def normalize_symbol(symbol: str) -> str:
    """标准化股票代码以适应 yfinance。"""
    s = str(symbol).strip().upper()
    # A股 .SH（上交）在 yfinance 要用 .SS；深交 .SZ 不变
    if s.endswith(".SH"):
        s = s[:-3] + ".SS"
    return s

def fetch_prices(symbols):
    """从 yfinance 获取股票/基金的最新价格。"""
    import yfinance as yf
    results = {}
    print(f"正在获取 {len(symbols)} 个代码的行情数据...")
    for sym in symbols:
        yf_sym = normalize_symbol(sym)
        try:
            t = yf.Ticker(yf_sym)
            price = None
            # 尝试从 fast_info 获取最新价，通常更快
            fi = getattr(t, "fast_info", None)
            if fi and "last_price" in fi:
                price = fi["last_price"]
            
            # 如果 fast_info 没有，尝试从历史数据获取
            if price is None or (isinstance(price, float) and math.isnan(price)):
                hist = t.history(period="1d") # 获取最近一天的历史数据
                if not hist.empty:
                    price = float(hist["Close"].iloc[-1]) # 取收盘价

            if price is None or (isinstance(price, float) and math.isnan(price)):
                print(f"[warn] 无法获取价格: {sym} (yfinance 符号: {yf_sym})")
                continue
            results[sym] = float(price)
            # 为了避免请求过于频繁被封，每次请求后稍作等待
            time.sleep(0.1)
        except Exception as e:
            print(f"[error] 获取 {sym} ({yf_sym}) 行情失败: {e}")
    print(f"成功获取 {len(results)} 个代码的价格。")
    return results

def main():
    """主函数，执行价格更新逻辑。"""
    assert_env() # 检查环境变量

    client = FeishuClient(FEISHU_APP_ID, FEISHU_APP_SECRET)
    # token 在 _headers 方法中按需获取，这里可以省略，或者显式获取一次
    # client.get_tenant_access_token()

    # 1. 从飞书多维表格获取所有记录
    rows = client.list_records(FEISHU_APP_TOKEN, TABLE_ID)
    
    records_to_process = []
    codes_to_fetch = []
    for r in rows:
        fields = r.get("fields", {})
        code = fields.get(CODE_FIELD)
        if code and str(code).strip():
            code_str = str(code).strip()
            records_to_process.append({"record_id": r["record_id"], "code": code_str})
            codes_to_fetch.append(code_str)

    if not codes_to_fetch:
        print(f"在表 {TABLE_ID} 中未发现列“{CODE_FIELD}”或该列为空。请在表格中填写股票/基金代码，或检查 CODE_FIELD 环境变量是否正确设置为你的代码列名。")
        return

    # 2. 获取股票/基金的最新价格
    prices = fetch_prices(codes_to_fetch)
    
    # 获取当前北京时间
    now_str = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")

    # 3. 更新飞书多维表格
    updated_count = 0
    for rec in records_to_process:
        code = rec["code"]
        record_id = rec["record_id"]
        price = prices.get(code) # 从获取到的价格字典中查找
        
        if price is None:
            print(f"[info] 未获取到 {code} 的价格，跳过更新。")
            continue
        
        fields_to_update = {
            PRICE_FIELD: price,
            UPDATED_AT_FIELD: now_str,
        }
        try:
            client.update_record(FEISHU_APP_TOKEN, TABLE_ID, record_id, fields_to_update)
            updated_count += 1
            print(f"成功更新 {code} (Record ID: {record_id}) 为价格: {price}")
        except Exception as e:
            print(f"[error] 更新 {code} (Record ID: {record_id}) 失败: {e}")

    print(f"更新完成：成功更新 {updated_count}/{len(records_to_process)} 行。")

if __name__ == "__main__":
    main()
