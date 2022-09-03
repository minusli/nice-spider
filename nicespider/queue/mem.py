import logging
import queue

import sys

from nicespider.queue import Queue
from nicespider.reqresp import Request


class MemQueue(Queue):
    def __init__(self, maxsize: int = sys.maxsize):
        self.maxsize = maxsize
        self.queue = queue.Queue(maxsize=self.maxsize)

    def pull(self) -> Request:
        while True:
            return self.queue.get()

    def push(self, req: Request):
        self.queue.put(req)

    def success(self, req: Request):
        logging.info(f"Success: {req.url}")

    def fail(self, req: Request):
        logging.error(f"Fail: {req.url}")
        self.push(req=req)

    def size(self) -> int:
        return self.queue.qsize()
