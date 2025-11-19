import os
import requests
import json
import pandas as pd

# 1. 从环境变量读取密钥和ID
# GitHub Actions 会将 Secrets 注入到这些环境变量中
APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")
BASE_TOKEN = os.getenv("FEISHU_BASE_TOKEN")

# 飞书 API 授权终点 (Endpoint)
FEISHU_AUTH_URL = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"

class FeishuClient:
    """处理飞书API认证和数据交互的客户端"""

    def __init__(self, app_id, app_secret, base_token):
        self.app_id = app_id
        self.app_secret = app_secret
        self.base_token = base_token
        self._access_token = self._get_app_access_token()

    def _get_app_access_token(self):
        """发送请求获取 App Access Token"""
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        headers = {
            "Content-Type": "application/json"
        }

        print("正在获取 App Access Token...")
        response = requests.post(FEISHU_AUTH_URL, headers=headers, data=json.dumps(payload))
        response.raise_for_status() # 遇到 HTTP 错误时抛出异常

        data = response.json()
        if data.get("code") == 0:
            token = data["app_access_token"]
            print(f"授权成功。Token: {token[:10]}...")
            return token
        else:
            raise Exception(f"获取 App Token 失败: {data.get('msg')}")

    # TODO: 后续我们将在这里添加读取和写入表格数据的方法

# --- 主程序入口 ---
if __name__ == "__main__":
    if not all([APP_ID, APP_SECRET, BASE_TOKEN]):
        print("错误：请确保在 GitHub Secrets 中配置了 FEISHU_APP_ID, FEISHU_APP_SECRET 和 FEISHU_BASE_TOKEN。")
        exit(1)

    feishu_client = FeishuClient(APP_ID, APP_SECRET, BASE_TOKEN)
    print("飞书 API 连接初始化完成。")

    # TODO: 这里将是调用价格获取和数据写入的地方
