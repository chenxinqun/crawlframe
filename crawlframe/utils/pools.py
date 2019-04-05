#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : pools.py
@Author: ChenXinqun
@Date  : 2019/1/22 10:45
'''
'''
公共协程池, 控制进程内部并发量.
'''
import gc
from asyncio import (
    wait,
    events,
    coroutine,
    Semaphore,
)
from crawlframe import configs


class Pool:

    loop = events.get_event_loop()
    timeout = configs.settings.TASKS_RUN_TIMEOUT
    def __init__(self, size, loop=None):
        self.size = size
        if loop is not None:
            self.loop = loop
        self.__limit = Semaphore(size)

    async def call(self, obj):
        async with self.__limit as lock:
            await self.loop.run_in_executor(None, obj)

    @coroutine
    def map(self, iterator):
        tasks = [self.loop.create_task(self.call(x)) for x in iterator]
        if tasks:
            yield from wait(tasks, timeout=configs.settings.TASKS_RUN_TIMEOUT)
        return [], []

    def jionall(self, futures, timeout=None):
        if timeout is None:
            timeout = self.timeout
        for future in futures:
            future.result(timeout=timeout)

    @coroutine
    def apply_sync(self, iterator):
        tasks = [self.call(x) for x in iterator]
        for task in tasks:
            yield from task




class TaskPools:

    maxsize = configs.settings.TASKS_THREAD_MAXSIZE
    P = Pool

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = object.__new__(cls)
        return cls.instance

    def new(self, loop=None):
        return self.P(self.maxsize, loop)


get_task_pools = lambda: TaskPools()

task_pools = get_task_pools()
task_pools.pool = task_pools.new()