from nicespider.queue import MemQueue
from nicespider.reqresp import Request


def test_memq():
    q = MemQueue()

    count = 10
    reqs = [Request(f"url_{i}") for i in range(count)]
    for req in reqs:
        q.push(req)
    assert q.size() == count
    c = 0
    while c < count:
        assert q.pull().url == f"url_{c}"
        assert q.size() == count - c - 1
        c += 1
    assert q.size() == 0
    req = Request("url_10")
    q.fail(req)
    assert q.size() == 1
    assert q.pull().url == "url_10"
    assert q.size() == 0
