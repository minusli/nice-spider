from abc import ABCMeta, abstractmethod

from nicespider.reqresp import Request, Response


class Download(metaclass=ABCMeta):
    @abstractmethod
    def match(self, req: Request) -> bool:
        pass

    @abstractmethod
    def download(self, req: Request) -> Response:
        pass
