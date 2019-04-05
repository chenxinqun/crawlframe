#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : request.py
@Author: ChenXinqun
@Date  : 2019/2/16 14:42
'''


class RequestMiddleware:
    def process_run(self, request):
        '''子类必须实现此方法'''
        raise NotImplementedError

    def __call__(self, request):
        self.process_run(request)
