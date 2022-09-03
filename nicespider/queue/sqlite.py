import random
from enum import Enum

import nicesql
import time

from nicespider.queue import Queue
from nicespider.reqresp import Request

__db_alias__ = "__sqlite_queue__"


class Status(Enum):
    FAILED = -1
    TODO = 0
    DOING = 1


class Task(nicesql.SqlModel):
    def __init__(self):
        self.id = None
        self.content = None
        self.status = None
        self.create_time = None
        self.update_time = None


class SqliteQueue(Queue):
    def __init__(self, db: str):
        self.db = DB(db)

    def pull(self) -> Request:
        while True:
            tasks = self.db.find_by_status(Status.TODO, random.randint(0, 9), 1)
            if not tasks:
                tasks = self.db.find_by_status(Status.TODO, offset=0, limit=1)

            if not tasks:  # 无数据，休眠重来
                time.sleep(3)
                continue

            task = tasks[0]
            if not self.db.update_status_by_id(task.id, Status.DOING):  # 加锁失败，快速重来
                continue

            req = Request.load(task.content)
            req.id = task.id
            return req

    def push(self, req: Request):
        self.db.insert(Request.dump(req))

    def success(self, req: Request):
        self.db.delete(int(req.id))

    def fail(self, req: Request):
        self.db.update_status_by_id(int(req.id), Status.FAILED)

    def size(self) -> int:
        resp = self.db.count_by_status(Status.TODO)
        return resp['count']


class DB:
    def __init__(self, db):
        nicesql.register(nicesql.Sqlite(db), alias=__db_alias__)
        DB.init_tables()

    @classmethod
    def init_tables(cls):
        nicesql.execute("""
            create table if not exists task
            (
                id          INTEGER not null primary key,
                content     TEXT,
                status      INTEGER,
                create_time DATETIME default CURRENT_TIMESTAMP,
                update_time DATETIME default CURRENT_TIMESTAMP
            );
        """, engine=__db_alias__)

        nicesql.execute("""
           create trigger if not exists task_onupdate after update on task
           begin
               update task SET update_time = datetime('now') WHERE id = NEW.id;
           end;
       """, engine=__db_alias__)

        nicesql.execute("create index if not exists idx_status on task (status);", engine=__db_alias__)
        nicesql.execute("create index if not exists idx_create_time on task (create_time);", engine=__db_alias__)
        nicesql.execute("create index if not exists idx_update_time on task (update_time);", engine=__db_alias__)

    @classmethod
    @nicesql.insert("insert into task(content, status) values({content}, {status.value})", engine=__db_alias__)
    def insert(cls, content: str, status: Status = Status.TODO) -> int:
        pass

    # noinspection PyShadowingBuiltins
    @classmethod
    @nicesql.select("select * from task where id = {id}", model=Task, first=True, engine=__db_alias__)
    def get(cls, id: int) -> Task:
        pass

    # noinspection PyShadowingBuiltins
    @classmethod
    @nicesql.delete("delete from task where id = {id}", engine=__db_alias__)
    def delete(cls, id: int):
        pass

    @classmethod
    @nicesql.select("select * from task where status={status.value} limit {offset},{limit}", model=Task, engine=__db_alias__)
    def find_by_status(cls, status: Status = Status.TODO, offset: int = 0, limit: int = 10):
        pass

    # noinspection PyShadowingBuiltins
    @classmethod
    @nicesql.update("update task set status={status.value} where id={id}", engine=__db_alias__)
    def update_status_by_id(cls, id: int, status: Status):
        pass

    @classmethod
    @nicesql.select("select count(1) as count from task where status={status.value}", engine=__db_alias__, first=True)
    def count_by_status(cls, status: Status):
        pass
