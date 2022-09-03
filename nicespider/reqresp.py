import json
from typing import Union, Dict, Any

import requests


class Request:
    def __init__(self, url: str, params: Dict[str, str] = None, data: str = '', json_: Dict = None, method: str = '',
                 session: requests.Session = None, cookies: Dict[str, str] = None, headers: Dict[str, str] = None,
                 timeout=30, proxies: Dict[str, str] = None, id_: str | int = None):
        self.url = url
        self.params = params or {}
        self.data = data or ''
        self.json = json_ or {}
        self.method = method.upper()
        self.headers = headers or {}
        self.cookies = cookies or {}
        self.timeout = timeout or 30
        self.proxies = proxies or {}
        self.id = id_
        if self.id is None:
            self.id = self.url

        if session:
            self.headers.update({k: v for k, v in session.headers.items() if k not in self.headers})
            self.cookies.update({k: v for k, v in session.cookies.items() if k not in self.cookies})

        self.extra = {}

    def get_extra(self, k, default=None):
        return self.extra.get(k, default)

    def put_extra(self, **kwargs):
        self.extra.update(kwargs)
        return self

    @classmethod
    def dump(cls, req: "Request") -> str:
        return json.dumps(dict(
            url=req.url, params=req.params, data=req.data, json=req.json, method=req.method,
            headers=req.headers, cookies=req.cookies, timeout=req.timeout, proxies=req.proxies,
            extra=req.extra, id=req.id
        ))

    @classmethod
    def load(cls, content: str) -> "Request":
        kv: Dict = json.loads(content)
        req = Request(kv["url"])
        for k, v in kv.items():
            setattr(req, k, v)
        return req


class Response:
    def __init__(self, content: Union[str | bytes], raw: Any):
        self.content = content
        self.extra = {}
        self.raw = raw

    def json(self):
        return json.loads(self.content)

    def text(self, encoding='utf-8'):
        if isinstance(self.content, bytes):
            return self.content.decode(encoding)
        return self.content

    def get_extra(self, k: str, default=None):
        return self.extra.get(k, default)

    def put_extra(self, k: str, v):
        self.extra[k] = v
        return self
