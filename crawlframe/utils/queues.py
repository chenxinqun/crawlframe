#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : queues.py
@Author: ChenXinqun
@Date  : 2019/1/18 16:26
'''

'''
进程间通信用的队列, 并创建相应的实例. 
'''
from asyncio import Queue
from crawlframe import configs


class CrawlerQueue:

    maxsize = configs.settings.TASKS_QUEUE_MAXSIZE
    Q = Queue

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = object.__new__(cls)
        return cls.instance

    def new(self):
        return self.Q(maxsize=self.maxsize)


get_task_queues = lambda: CrawlerQueue()

task_queues = get_task_queues()

task_queues.task = task_queues.new()

task_queues.next = task_queues.new()