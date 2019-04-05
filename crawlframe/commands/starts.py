#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : start.py
@Author: ChenXinqun
@Date  : 2019/1/19 11:59
'''
import os
import sys
import pickle
from subprocess import Popen
from importlib import import_module

from crawlframe import configs
from crawlframe.utils.timers import BaseTime
from crawlframe.engine.crawl import input_checkout
from crawlframe.configs import (
    _CACHE_ENV,
    _CHDIR_ENV,
    _STOP_SIGNALS_ENV,
    _CRAWLFRAME_DIR as frame_dir
)


def start(args=None):
    if args is None:
        args = sys.argv[1:]

    # 检查参数
    input_checkout(args[0])
    # 引擎目录
    engine_dir = os.path.join(frame_dir, 'engine')
    # crawl脚本
    crawl_file = os.path.join(engine_dir, 'crawl.py')
    # settings实例
    settings = configs.get_settings()
    # 为模块赋值.
    configs.settings = settings
    # 提取工作路径
    chdir = os.getenv(_CHDIR_ENV) or settings.SPIDER_CHDIR or os.getcwd()
    if os.path.isdir(chdir):
        # 设置工作路径环境变量
        os.environ[_CHDIR_ENV] = chdir
    # 设置缓存目录
    cache_dir = settings.CRAWLFRAME_CACHE_DIR or os.path.join(chdir, 'cache')
    if not os.path.exists(cache_dir):
        # 创建缓存目录
        os.makedirs(cache_dir)
    # 设置停止信号环境变量
    os.environ.setdefault(_STOP_SIGNALS_ENV, 'flase')
    # 设置缓存文件环境变量
    os.environ.setdefault(_CACHE_ENV, cache_dir)
    # APP容器
    app_list = []
    # 根据命令不同, 分别使用不同方式提取APP
    if args[0] == 'project':
        for spiders in settings.INSTALLED_SPIDER:
            if isinstance(spiders, dict):
                for mod, sp in spiders.items():
                    spider_mod = import_module(mod)
                    spider_obj = getattr(spider_mod, sp)
                    app_list.append('app:%s' % spider_obj.name)
    elif 'app:' in args[0]:
        app_list.append(args[0])
    # 进程容器
    p_dict = {}
    pid_dict = {}
    # 开启子进程, 并装进容器.
    for app in app_list:
        p = Popen(['python', crawl_file, app])
        p_dict[app] = p
        pid_dict[app] = p.pid

    # 将子进程ID信息保存(stop脚本用得上)
    if len(pid_dict) > 0:
        data_file = os.path.join(cache_dir, '%s_data' % os.getpid())
        with open(data_file, 'wb') as f:
            pickle.dump(pid_dict, f)

    for a, pid in pid_dict.items():
        # 创建子进程stop信号文件(stop脚本用)
        stop_file = os.path.join(cache_dir, '%s_stop' % pid)
        with open(stop_file, 'w', encoding='utf-8') as f:
            f.write('false')
            f.flush()
        # 提示启动成功.
        print('%s %s pid:%s start' % (BaseTime.time_now().strftime('%Y-%m-%d %H:%M:%S'), a, pid))


if __name__ == '__main__':
    start()