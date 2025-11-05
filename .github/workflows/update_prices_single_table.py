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
