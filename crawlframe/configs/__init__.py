#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : __init__.py
@Author: ChenXinqun
@Date  : 2019/1/18 16:26
'''
import os
from importlib import import_module
from crawlframe.configs import default_settings

# 框架根目录
_CRAWLFRAME_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 工作路径环境变量
_CHDIR_ENV = 'CRAWLFRAME_SPIDER_CHDIR'

# 缓存文件夹环境变量
_CACHE_ENV = 'CRAWLFRAME_CACHE_DIR'

# 配置文件的环境变量, key名
_SETTINGS_ENV = 'CRAWLFRAME_SETTINGS_MODULE'

# 停止信号环境变量
_STOP_SIGNALS_ENV = 'CRAWLFRAME_STOP_SIGNALS'


_SURVIVE_SIGNALS_ENV = 'CRAWLFRAME_SURVIVE_MAX'


class Settings:
    def __init__(self, mod):
        self.update(mod)

    def update(self, mod):
        self._checkout(mod)
        for attr in dir(mod):
            if attr.isupper():
                self.__dict__[attr] = getattr(mod, attr)

    def _checkout(self, mod):
        if type(mod).__name__ != 'module':
            raise TypeError('args mod must be %s, got %s!' % ('module', type(mod).__name__))

    def __str__(self):
        return '%s object at %s' % (Settings, Settings.__name__)

    __repr__ = __str__


def get_settings(mod=None):
    # 先加载默认配置
    res = Settings(default_settings)
    # 再加载环境变量设定的配置
    if os.getenv(_SETTINGS_ENV):
        mod = import_module(os.getenv(_SETTINGS_ENV))
        res.update(mod)

    # 再加载外部配置
    if mod:
        res.update(mod)
    # 返回配置对象
    return res


settings = get_settings()