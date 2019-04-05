#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : response.py
@Author: ChenXinqun
@Date  : 2019/2/16 14:41
'''


class ResponseMiddleware:
    def process_run(self, response):
        '''子类必须实现此方法'''
        raise NotImplementedError

    def __call__(self, response):
        self.process_run(response)