import random

import nicesql
import time
from nicesql.sqlengine.sqlite import Sqlite
from nicesql.sqlmodel import SqlModel

from nicespider.queue import Queue
from nicespider.queue.base import Status
from nicespider.reqresp import Request

__db_alias__ = "__sqlite_queue_db__"


class Task(SqlModel):
    def __init__(self):
        self.id = None
        self.content = None
        self.status = None
        self.create_time = None
        self.update_time = None


class Dao:
    def __init__(self, db):
        nicesql.register(Sqlite(db), alias=__db_alias__)
        Dao.init_tables()

    @classmethod
    def init_tables(cls):
        nicesql.execute("""
            create table if not exists nicespider_task_queue
            (
                id          INTEGER not null primary key,
                content     TEXT,
                status      INTEGER,
                create_time DATETIME default CURRENT_TIMESTAMP,
                update_time DATETIME default CURRENT_TIMESTAMP
            );
        """, engine=__db_alias__)

        nicesql.execute("""
           create trigger if not exists nicespider_task_queue_onupdate after update on nicespider_task_queue
           begin
               update nicespider_task_queue SET update_time = datetime('now') WHERE id = NEW.id;
           end;
       """, engine=__db_alias__)

        nicesql.execute("create index if not exists idx_nicespider_task_queue_status on nicespider_task_queue (status);", engine=__db_alias__)
        nicesql.execute("create index if not exists idx_nicespider_task_queue_create_time on nicespider_task_queue (create_time);", engine=__db_alias__)
        nicesql.execute("create index if not exists idx_nicespider_task_queue_update_time on nicespider_task_queue (update_time);", engine=__db_alias__)

    # noinspection PyShadowingBuiltins
    @nicesql.select("select * from nicespider_task_queue where id = {id}", model=Task, first=True, engine=__db_alias__)
    def get(self, id: int) -> Task:
        pass

    @nicesql.insert("insert into nicespider_task_queue(content, status) values({content}, {status.value})", engine=__db_alias__)
    def insert(self, content: str, status: Status = Status.TODO) -> int:
        pass

    # noinspection PyShadowingBuiltins
    @nicesql.delete("delete from nicespider_task_queue where id = {id}", engine=__db_alias__)
    def delete(self, id: int):
        pass

    @nicesql.select("select * from nicespider_task_queue where status={status.value} limit {index}, 1", model=Task, first=True, engine=__db_alias__)
    def get_by_status(self, status: Status = Status.TODO, index=0):
        pass

    # noinspection PyShadowingBuiltins
    @nicesql.update("update nicespider_task_queue set status={status.value} where id={id}", engine=__db_alias__)
    def update_status_by_id(self, id: int, status: Status):
        pass

    @nicesql.select("select count(1) as count from nicespider_task_queue where status={status.value}", engine=__db_alias__, first=True)
    def count_by_status(self, status: Status):
        pass


# noinspection DuplicatedCode
class SqliteQueue(Queue):
    def __init__(self, db: str, parallel: int = 10):
        self.dao = Dao(db)
        self.parallel = parallel

    def pull(self) -> Request:
        while True:
            task = self.dao.get_by_status(Status.TODO, random.randint(0, self.parallel - 1))
            if not task:
                task = self.dao.get_by_status(Status.TODO, 0)

            if not task:  # 无数据，休眠重来
                time.sleep(3)
                continue

            if not self.dao.update_status_by_id(task.id, Status.DOING):  # 加锁失败，快速重来
                continue

            req = Request.load(task.content)
            req.id = task.id
            return req

    def push(self, req: Request):
        self.dao.insert(Request.dump(req))

    def success(self, req: Request):
        self.dao.delete(int(req.id))

    def fail(self, req: Request):
        self.dao.update_status_by_id(int(req.id), Status.FAILED)

    def size(self) -> int:
        resp = self.dao.count_by_status(Status.TODO)
        return resp['count']
