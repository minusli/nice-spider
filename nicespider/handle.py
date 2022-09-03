from abc import ABCMeta, abstractmethod

from nicespider.reqresp import Request, Response
from nicespider.spider import Spider


class Handler(metaclass=ABCMeta):
    def __init__(self, spider: Spider):
        self.spider = spider

    @abstractmethod
    def match(self, req: Request, resp: Response) -> bool:
        pass

    @abstractmethod
    def process(self, req: Request, resp: Response) -> bool:
        pass
