import logging
import signal
import threading
from enum import Enum
from typing import List

import time

from nicespider.download import Download
from nicespider.handle import Handler
from nicespider.interceptor import Interceptor
from nicespider.queue import MemQueue
from nicespider.reqresp import Request


class Spider:
    class Status(Enum):
        INIT = 0
        START = 1
        STOP = 2

    interceptors: List[Interceptor] = []
    downloads: List[Download] = []
    handlers: List[Handler] = []

    workers = 1
    queue = MemQueue()
    status = Status.INIT

    @classmethod
    def worker(cls):
        while True:
            if cls.status == cls.Status.STOP:
                break
            req = None
            # noinspection PyBroadException
            try:
                req = cls.queue.pull()
                if cls.execute(req=req):
                    cls.queue.success(req)
                else:
                    cls.queue.fail(req)
            except Exception as e:
                logging.exception(e)
                if req:
                    cls.queue.fail(req)

    @classmethod
    def submit(cls, *reqs: Request):
        for req in reqs:
            cls.queue.push(req)

    @classmethod
    def execute(cls, req: Request) -> bool:
        # interceptor
        for interceptor in cls.interceptors:
            if not interceptor.intercept(req):
                return True

        # download
        download = None
        for d in cls.downloads:
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
        for h in cls.handlers:
            if h.match(req, resp):
                handler = h
                break
        if not handler:
            raise Exception(f"Handler Not Found For Request: request.url={req.url}")
        success = handler.process(req=req, resp=resp)

        return success

    @classmethod
    def start(cls):
        signal.signal(signal.SIGINT, lambda signum, frame: cls.stop())
        signal.signal(signal.SIGTERM, lambda signum, frame: cls.stop())

        threads = []
        for i in range(0, cls.workers):
            t = threading.Thread(target=cls.worker, name=f"Worker-{i}")
            threads.append(t)
            t.start()

        threading.Thread(target=cls.heartbeat, args=(threads,), name="heartbeat", daemon=True)

    @classmethod
    def stop(cls):
        cls.status = cls.Status.STOP

    @classmethod
    def heartbeat(cls, threads: List[threading.Thread]):
        logging.info(f"requests={cls.queue.size()}, workers={[1 if t.is_alive() else 0 for t in threads]}")
        time.sleep(5)
