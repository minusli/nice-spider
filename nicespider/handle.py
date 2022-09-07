from abc import ABCMeta, abstractmethod

from nicespider.reqresp import Request, Response


class Handler(metaclass=ABCMeta):
    @abstractmethod
    def match(self, req: Request, resp: Response) -> bool:
        pass

    @abstractmethod
    def process(self, req: Request, resp: Response) -> bool:
        pass
