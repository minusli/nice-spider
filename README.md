# minusSpider User Guide

------

Talk is cheap. Show me the code

```python
# demo
from nicespider import Spider
from nicespider import HttpDownload
from nicespider import Handler
from nicespider import Interceptor
from nicespider import Request, Response
from nicespider import MemQueue


class PrintHandler(Handler):
    def match(self, req: Request, resp: Response) -> bool:
        return True

    def process(self, req: Request, resp: Response) -> bool:
        print(resp.content)
        return True


class PassInterceptor(Interceptor):
    def intercept(self, req: Request) -> bool:
        return True


if __name__ == "__main__":
    q = MemQueue()
    spider = Spider(workers=3, queue=q)
    spider.add_interceptor(PassInterceptor())
    spider.add_download(HttpDownload())
    spider.add_handler(PrintHandler(spider))

    spider.submit(Request(url="https://www.baidu.com"))

    spider.start()

```
