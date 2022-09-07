from abc import ABCMeta, abstractmethod
from enum import Enum

from nicespider.reqresp import Request


class Status(Enum):
    FAILED = -1
    TODO = 0
    DOING = 1


class Queue(metaclass=ABCMeta):
    @abstractmethod
    def pull(self) -> Request:
        pass

    @abstractmethod
    def push(self, req: Request):
        pass

    @abstractmethod
    def success(self, req: Request):
        pass

    @abstractmethod
    def fail(self, req: Request):
        pass

    @abstractmethod
    def size(self) -> int:
        pass
