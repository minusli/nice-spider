import logging
import threading
from typing import List

import time

from nicespider.download import Download
from nicespider.handle import Handler
from nicespider.interceptor import Interceptor
from nicespider.queue import Queue, MemQueue
from nicespider.reqresp import Request


class Spider:
    def __init__(self, workers=1, queue=None):
        self.workers = workers
        self.queue: Queue = queue or MemQueue()

        self.interceptors: List[Interceptor] = []
        self.downloads: List[Download] = []
        self.handlers: List[Handler] = []

    def _worker(self):
        while True:
            req = None
            # noinspection PyBroadException
            try:
                req = self.queue.pull()
                if self.execute(req=req):
                    self.queue.success(req)
                else:
                    self.queue.fail(req)
            except Exception as e:
                logging.exception(e)
                if req:
                    self.queue.fail(req)

    def add_download(self, *downloads: Download) -> "Spider":
        self.downloads.extend(downloads)
        return self

    def add_handler(self, *handlers: Handler) -> "Spider":
        self.handlers.extend(handlers)
        return self

    def add_interceptor(self, *interceptors: Interceptor) -> "Spider":
        self.interceptors.extend(interceptors)
        return self

    def submit(self, *reqs: Request) -> "Spider":
        for req in reqs:
            self.queue.push(req)
        return self

    def execute(self, req: Request) -> bool:
        # interceptor
        for interceptor in self.interceptors:
            if not interceptor.intercept(req):
                return True

        # download
        download = None
        for d in self.downloads:
            if d.match(req):
                download = d
                break
        if not download:
            raise Exception(f"Download Not Found For Request: request.url={req.url}")
        resp = download.download(req)
        if not resp:
            raise Exception(f"Download Response Null: {resp}")

        # handler
        handler = None
        for h in self.handlers:
            if h.match(req, resp):
                handler = h
                break
        if not handler:
            raise Exception(f"Handler Not Found For Request: request.url={req.url}")
        success = handler.process(req=req, resp=resp)

        return success

    def start(self):
        threads = []
        for i in range(0, self.workers):
            t = threading.Thread(target=self._worker, name=f"Worker-{i}", daemon=True)
            threads.append(t)
            t.start()

        while True:
            logging.info(f"workers={[1 if t.is_alive() else 0 for t in threads]}, requests={self.queue.size()}")
            time.sleep(5)
