import random

import nicesql
import time
from nicesql.sqlengine.mysql import Mysql
from nicesql.sqlmodel import SqlModel

from nicespider.queue import Queue
from nicespider.queue.base import Status
from nicespider.reqresp import Request

__db_alias__ = "__mysql_queue_db__"


class Task(SqlModel):
    def __init__(self):
        self.id = None
        self.content = None
        self.status = None
        self.create_time = None
        self.update_time = None


class Dao:
    def __init__(self, dbname: str, host: str = '127.0.0.1', port: int = 3306, user: str = None, password: str = None):
        nicesql.register(Mysql(
            host=host, port=port, dbname=dbname, user=user, password=password
        ), alias=__db_alias__)
        Dao.init_tables()

    @classmethod
    def init_tables(cls):
        nicesql.execute("""
            create table if not exists nicespider_task_queue
            (
                id          INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                content     TEXT,
                status      TINYINT,
                create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                
                KEY `idx_status` (`status`),
                KEY `idx_create_time` (`create_time`), 
                KEY `idx_update_time` (`update_time`) 
            );
        """, engine=__db_alias__)

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

    @nicesql.bind("drop table if exists nicespider_task_queue", engine=__db_alias__)
    def drop_for_test(self):
        pass


# noinspection DuplicatedCode
class MysqlQueue(Queue):
    def __init__(self, dbname: str, host: str = '127.0.0.1', port: int = 3306, user: str = None, password: str = None):
        self.dao = Dao(
            host=host, port=port, dbname=dbname, user=user, password=password
        )

    def pull(self) -> Request:
        while True:
            task = self.dao.get_by_status(Status.TODO, random.randint(0, 10))
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
