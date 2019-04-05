#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : middle.py
@Author: ChenXinqun
@Date  : 2019/2/19 9:11
'''
from functools import wraps
from importlib import import_module
from crawlframe import configs
from crawlframe.cores.middlewares.spider import SpiderMiddleware
from crawlframe.cores.middlewares.request import RequestMiddleware
from crawlframe.cores.middlewares.response import ResponseMiddleware


class Middlewares:
    def __init__(self):
        self.spider = {}
        self.request = {}
        self.response = {}


def get_middleware():
    middleware = Middlewares()
    # 通过配置, 加载自定义中间件.
    for middle in configs.settings.INSTALLED_MIDDLEWARE:
        if isinstance(middle, dict):
            for mid in middle:
                mod = import_module(mid)
                # 对中间件进行优先级排序
                mid_list = sorted(middle[mid], key=lambda item: item[1])
                for m in mid_list:
                    obj = getattr(mod, m[0])
                    if callable(obj):
                        instance = obj()
                        middle_mod = '.'.join(instance.__module__.split('.')[:-1])
                        middleware.spider.setdefault(middle_mod, [])
                        if callable(instance):
                            if isinstance(instance, SpiderMiddleware):
                                middleware.spider[middle_mod].append(instance)
                            elif isinstance(instance, RequestMiddleware):
                                middleware.request[middle_mod].append(instance)
                            elif isinstance(instance, ResponseMiddleware):
                                middleware.response[middle_mod].append(instance)
    return middleware


middleware = get_middleware()


def install_process(middle_type, obj, obj_mod):
    middle_list = middleware.__dict__.get(middle_type, {}).get(obj_mod, [])
    # 加载request中间件
    for middle in middle_list:
        middle(obj)


def install_middle(middle_type):
    '''middle_type支持三种参数"spider", "request", "response"'''

    def load_middle(func):
        @wraps(func)
        def wrap(*args, **kwargs):
            if middle_type == 'spider':
                if middleware.__dict__.get(middle_type):
                    spider = args[0]
                    spider_mod = '.'.join((spider.__module__).split('.')[:-1])
                    install_process(middle_type, spider, spider_mod)

            elif middle_type == 'request':
                download, request = args
                if middleware.__dict__.get(middle_type):
                    # 提取回调函数的mod名
                    callback_mod = '.'.join((request.callback.__self__.__module__).split('.')[:-1])
                    install_process(middle_type, request, callback_mod)
                result = func(download, request, **kwargs)
                return result

            elif middle_type == 'response':
                response = func(*args, **kwargs)
                if middleware.__dict__.get(middle_type):
                    # 提取回调函数的mod名
                    callback_mod = '.'.join((response.callback.__self__.__module__).split('.')[:-1])
                    install_process(middle_type, response, callback_mod)

                return response

        return wrap

    return load_middle


@install_middle('spider')
def install_spider_middle(spider):
    print(spider)
