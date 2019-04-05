#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : __init__.py
@Author: ChenXinqun
@Date  : 2019/1/18 16:26
'''
from asyncio import sleep
from collections import Iterator

from crawlframe import configs
from crawlframe.utils import queues


class BaseCrawler:
    name = 'BaseCrawler'
    timeout = configs.settings.TASKS_PUT_TIMEOUT
    _sleep = configs.settings.TASKS_GET_INTERVAL
    _task_queues = queues.task_queues
    _dividend = 1000

    def __str__(self):
        return '<{}.{} crawler obj at {}>'.format(self.__class__.__module__, self.__class__.__name__, self.name)

    __repr__ = __str__

    def start_url(self):
        '''
        子类必须实现的方法
        return 一个Request实例, 或者一个自定义的处理方法. 支持yield
        '''
        raise NotImplementedError

    def put(self, obj):
        return self._task_queues.next.put_nowait(obj)

    async def add_task(self, obj):
        return await self._task_queues.task.put(obj)

    def next(self, iterator):
        return next(iterator, None)

    def task_len(self):
        return self._task_queues.task.qsize() + self._task_queues.next.qsize()

    async def run(self):
        if hasattr(configs.settings, 'CRAWLFRAME_STOP_SIGNALS') and configs.settings.CRAWLFRAME_STOP_SIGNALS:
            return
        iterator = self.start_url()
        _num = self._task_queues.task.maxsize
        if isinstance(iterator, Iterator):
            for i in range(_num):
                obj = self.next(iterator)
                if callable(obj):
                    await self.add_task(obj)
                if obj is None:
                    break
        else:
            if callable(iterator):
                await self.add_task(iterator)
        if self._sleep > self._dividend:
            self._sleep = self._dividend
        await sleep(self._sleep / self._dividend)
