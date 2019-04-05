#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : reload_module.py
@Author: ChenXinqun
@Date  : 2019/3/7 17:44
'''
import os
import sys
from importlib import reload
from crawlframe.utils import logger


mtimes = {}


def reload_module():
    # 遍历module
    for module in sys.modules.values():
        # 获取文件名
        file_name = getattr(module, '__file__', None)
        if not (file_name and os.path.isfile(file_name)):
            continue
        if file_name[-4:] in ('.pyc', '.pyo', '.pyd'):
            file_name = file_name[:-1]
        try:
            # 获取文件的最后修改时间
            new_mtime = os.stat(file_name).st_mtime
        except OSError:
            continue
        # 缓存最后修改时间
        old_mtime = mtimes.get(module)
        if old_mtime is None:
            mtimes[module] = new_mtime
        # 比对最后修改时间, 如果大于缓存
        elif old_mtime < new_mtime:
            try:
                # 重载模块
                reload(module)
                msg = '%s %s %s %s %s %s' % ('old_mtime', old_mtime, 'new_mtime ', new_mtime, 'reload', module)
                if hasattr(logger, 'crawlframe_logger'):
                    logger.crawlframe_logger.warning(msg)
                else:
                    print(msg)
                # 并更新最后修改时间缓存
                mtimes[module] = new_mtime
            except ImportError:
                pass