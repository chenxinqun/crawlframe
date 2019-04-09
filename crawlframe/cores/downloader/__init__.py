#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : __init__.py
@Author: ChenXinqun
@Date  : 2019/1/18 16:26
'''
import gc
from asyncio import events
from asyncio import sleep
from asyncio.locks import Lock
from crawlframe import configs
from crawlframe.utils import pools
from crawlframe.utils import queues


class MainDownloader:
    loop = events.get_event_loop()
    name = None
    _task_queues = queues.task_queues
    log_name = name
    timeout = configs.settings.TASKS_GET_TIMEOUT / 100
    _sleep = configs.settings.TASKS_GET_INTERVAL
    _dividend = 1000
    _task_pools = pools.task_pools
    _count = 0
    _lock = Lock()
    async def start_request(self):
        try:
            item = self._task_queues.task.get_nowait()
        except:
            item = None
        return item

    async def next_request(self):
        try:
            item = self._task_queues.next.get_nowait()
        except:
            item = None
        return item

    def count(self):
        self._count += 1

    def task_len(self):
        return self._task_queues.task.qsize() + self._task_queues.next.qsize()

    async def call(self, obj):
        await self._task_pools.pool.call(obj)
        return self.count()

    async def run(self, spider=None, i=None):
        if hasattr(configs.settings, 'CRAWLFRAME_STOP_SIGNALS') and configs.settings.CRAWLFRAME_STOP_SIGNALS:
            return
        if self._sleep > self._dividend:
            self._sleep = self._dividend
        _num = self._task_pools.maxsize
        task_list = []
        obj = await self.next_request()
        if not callable(obj):
            obj = await self.start_request()
        if callable(obj):
            await self.call(obj)
            spider.count = self._count
            
        if self.task_len() < 1:
            await sleep(self._sleep / self._dividend)
        gc.collect()







