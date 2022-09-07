from http import HTTPStatus

import requests

from nicespider import utils
from nicespider.download.base import Download
from nicespider.reqresp import Request, Response


class HttpDownload(Download):
    # noinspection PyMethodMayBeStatic
    def match(self, req: Request) -> bool:
        if req.url.startswith("http:") or req.url.startswith("https:"):
            return True
        return False

    @utils.retry(3)
    def download(self, req: Request) -> Response:
        headers = req.headers
        cookies = req.cookies

        resp_ = requests.request(
            method=req.method or 'GET', url=req.url, params=req.params, data=req.data, json=req.json,
            cookies=cookies, headers=headers, proxies=req.proxies, timeout=req.timeout
        )

        if resp_.status_code != HTTPStatus.OK:
            raise Exception(f"HTTP {resp_.status_code}: url={req.url}")

        return Response(content=resp_.content, raw=resp_)
