#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : database.py
@Author: ChenXinqun
@Date  : 2019/1/19 12:02
'''
import os

from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool as DbPool
from crawlframe import configs
from crawlframe.utils.logger import slow_log


class PgDbModel:

    def __init__(self, conf: dict):
        '''
        :param conf:  dict(
        database='postgres',    # 库名
        user='postgres',        # 用户
        password='123456',      # 密码
        host='127.0.0.1',       # IP
        port='5432',            # 端口
        minconn=1,              # 最小连接数
        maxconn=5               # 最大连接数
        )
        '''
        # 创建连接池
        self.dpool = DbPool(**conf)

    # 获取连接
    def get_conn(self):
        return self.dpool.getconn()

    # 获取游标
    def get_cur(self, conn, dictcur=False):
        if dictcur == True:
            return conn.cursor(cursor_factory=RealDictCursor)
        else:
            return conn.cursor()

    # 提交事务
    def conn_commit(self, conn):
        return conn.commit()

    # 回滚事务
    def conn_rollback(self, conn):
        return conn.rollback()

    # 回收连接
    def conn_close(self, conn):
        return self.dpool.putconn(conn)

    # 关闭连接池
    def close_all(self):
        return self.dpool.closeall()

    # 返回查询结果
    def fetch_all(self, cur):
        return cur.fetchall()

    # 返回行数
    def row_count(self, cur):
        return cur.rowcount

    # 批量操作
    def executemany(self, cur, sql, data_list):
        if isinstance(data_list, list):
            cur.executemany(sql, data_list)

    # 单条操作
    def execute(self, cur, sql, data):
        if data is not None:
            cur.execute(sql, data)
        else:
            cur.execute(sql)

    # 查询操作
    # 慢查询日志装饰器
    @slow_log(
        'query', configs.settings.SLOW_LOGGER_QUERY_THRESHOLD,
        slow_off=configs.settings.SLOW_LOGGER_QUERY_OFF,
        log_title=configs.settings.SLOW_LOGGER_QUERY_TITLE,
        timezone=configs.settings.SLOW_LOGGER_QUERY_TIMEZONE
    )
    def select(self, cur, sql, data=None):
        self.execute(cur, sql, data)
        return self.fetch_all(cur)

    # 批量插入
    def insert_all(self, cur, sql, data_list):
        self.executemany(cur, sql, data_list)
        return self.row_count(cur)

    # 单条插入
    def insert_one(self, cur, sql, data):
        self.execute(cur, sql, data)
        return self.row_count(cur)

    # 批量更新
    def update_all(self, cur, sql, data_list):
        self.executemany(cur, sql, data_list)
        return self.row_count(cur)

    # 单条更新
    def update_one(self, cur, sql, data):
        self.execute(cur, sql, data)
        return self.row_count(cur)

    # 返回上次执行的SQL
    def get_query(self, cur):
        return cur.query.decode()

    # 检校合成的SQL是否正确
    def check_sql(self, cur, sql, data):
        return cur.mogrify(sql, data).decode()
