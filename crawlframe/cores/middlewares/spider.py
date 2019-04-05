#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : spider.py
@Author: ChenXinqun
@Date  : 2019/2/16 15:19
'''


class SpiderMiddleware:
    def process_run(self, spider):
        '''子类必须实现此方法'''
        raise NotImplementedError

    def __call__(self, spider):
        self.process_run(spider)
