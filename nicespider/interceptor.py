from abc import ABCMeta, abstractmethod

from nicespider.reqresp import Request


class Interceptor(metaclass=ABCMeta):
    @abstractmethod
    def intercept(self, req: Request) -> bool:
        pass
