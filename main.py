import os
import requests
import json
import pandas as pd
import yfinance as yf # 引入 yfinance 库

# 1. 配置
APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")
BASE_TOKEN = os.getenv("FEISHU_BASE_TOKEN")

# 你的 Table IDs
ASSETS_TABLE_ID = "tblTFq4Cqsz0SSa1" # 资产行情表 ID

# 飞书 API 终点
FEISHU_AUTH_URL = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
FEISHU_API_BASE = "https://open.feishu.cn/open-apis/bitable/v1/apps"

class FeishuClient:
    """处理飞书API认证和数据交互的客户端"""

    def __init__(self, app_id, app_secret, base_token):
        self.app_id = app_id
        self.app_secret = app_secret
        self.base_token = base_token
        self._access_token = self._get_app_access_token()
        self.headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }

    def _get_app_access_token(self):
        # ... (获取 Token 的代码保持不变，不再重复贴出) ...
        """发送请求获取 App Access Token"""
        payload = {"app_id": self.app_id, "app_secret": self.app_secret}
        headers = {"Content-Type": "application/json"}
        response = requests.post(FEISHU_AUTH_URL, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        data = response.json()
        if data.get("code") == 0:
            return data["app_access_token"]
        else:
            raise Exception(f"获取 App Token 失败: {data.get('msg')}")

    def _get_table_data(self, table_id):
        """通用方法：从飞书表格中读取所有记录"""
        url = f"{FEISHU_API_BASE}/{self.base_token}/tables/{table_id}/records"
        
        # 飞书 API 需要分页读取，这里只演示读取第一页
        print(f"正在读取表格数据: {table_id}...")
        
        response = requests.get(url, headers=self.headers, params={"page_size": 100})
        response.raise_for_status()
        data = response.json()
        
        if data.get("code") == 0:
            records = data["data"]["items"]
            print(f"读取成功，共 {len(records)} 条记录。")
            
            # 将飞书返回的格式（records[i]['fields']）转换为 Python 列表
            return [record['fields'] for record in records]
        else:
            raise Exception(f"读取表格失败: {data.get('msg')}")


# --- 核心数据获取函数 ---

def fetch_yfinance_price(symbols):
    """使用 yfinance 获取股票/ETF/外汇/加密货币的价格"""
    if not symbols:
        return {}
    
    ticker_data = yf.download(symbols, period="1d", progress=False)
    prices = {}
    
    # yfinance 返回的数据结构是 DataFrame，如果是多个 Symbol，则需要特殊处理
    if len(symbols) == 1:
        # 单个 Symbol 的价格直接从 'Close' 列获取
        prices[symbols[0]] = ticker_data['Close'].iloc[-1]
    else:
        # 多个 Symbol 的价格从 DataFrame 的最后一行的 'Close' 索引获取
        for symbol in symbols:
            # 确保数据存在，取最近一个交易日的收盘价
            if 'Close' in ticker_data:
                 prices[symbol] = ticker_data['Close'][symbol].iloc[-1]
            elif 'close' in ticker_data:
                prices[symbol] = ticker_data['close'][symbol].iloc[-1]
            
    print(f"Yahoo Finance 价格获取成功: {prices}")
    return prices


# --- 主程序入口 ---
def main():
    if not all([APP_ID, APP_SECRET, BASE_TOKEN]):
        print("错误：请确保在 GitHub Secrets 中配置了 FEISHU_APP_ID, FEISHU_APP_SECRET 和 FEISHU_BASE_TOKEN。")
        return

    try:
        feishu_client = FeishuClient(APP_ID, APP_SECRET, BASE_TOKEN)
        print("飞书 API 连接初始化完成。")

        # 1. 从飞书读取资产列表
        assets_data = feishu_client._get_table_data(ASSETS_TABLE_ID)
        
        # 2. 准备 yfinance 股票代码列表
        yfinance_symbols = []
        for asset in assets_data:
            # 假设你的资产表里有一个字段叫 'Code'
            if asset.get('Code'): 
                yfinance_symbols.append(asset['Code'])
        
        # 3. 获取实时价格
        realtime_prices = fetch_yfinance_price(yfinance_symbols)

        # TODO: 第四步：将价格更新回飞书表格 (下一阶段实现)
        
    except Exception as e:
        print(f"程序运行出错: {e}")

if __name__ == "__main__':
    main()
