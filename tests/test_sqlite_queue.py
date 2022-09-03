import math

from nicespider.queue import SqliteQueue
from nicespider.queue.sqlite import Status
from nicespider.reqresp import Request


def test_sqliteq():
    q = SqliteQueue(":memory:")

    count = 57
    reqs = [Request(f"url_{i}") for i in range(count)]
    for req in reqs:
        q.push(req)
    assert q.size() == count

    c = 0
    while c < count:
        req = q.pull()
        assert q.size() == count - c - 1
        if c % 2 == 0:
            q.success(req)
        else:
            q.fail(req)
        c += 1
    assert q.size() == 0
    assert q.db.count_by_status(Status.FAILED)['count'] == math.floor(count / 2)
