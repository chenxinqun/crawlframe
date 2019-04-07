#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : signals.py
@Author: ChenXinqun
@Date  : 2019/1/22 11:08
'''

import os
import time
from crawlframe import configs
from crawlframe.utils import logger
# 停止信号. 当需要停止进程时, 修改此模块此变量的值为True,
# 所有收到信号的程序, 将进入温和停止(完成任务后再退出)流程.


# 爬虫停止信号
CRAWLFRAME_STOP_SIGNALS = lambda: os.environ.setdefault(configs._STOP_SIGNALS_ENV, 'flase')


def _log_and_rm_file(msg, signal_file):
    logger.crawlframe_logger.war(msg)
    if os.path.isfile(signal_file) and os.path.exists(signal_file):
        os.remove(signal_file)
        logger.crawlframe_logger.info('"%s" signal file remove!' % signal_file)


def received_stop_signals(i, mod=None):
    '''

    :param mod: 一个module对象
    :param interval: 时间间隔
    :return:
    '''
    signal = signal_file = ''
    pid = os.getpid()
    cache_dir = configs.settings.CRAWLFRAME_CACHE_DIR or os.getenv(configs._CACHE_ENV) or ''
    '''
    通过pid在cache文件夹中, 找到以pid命名的对应cache文件, 并读取其值, 接收相应的停止信号.
    如果停止信号为true, 则将改信号写入环境变量. 并修改
    '''
    if os.path.isdir(cache_dir):
        files = os.listdir(cache_dir)
        for file in files:
            if file == '%s_stop' % pid:
                signal_file = os.path.join(cache_dir, file)

    if os.path.isfile(signal_file) and os.path.exists(signal_file):
        with open(signal_file, 'r', encoding='utf-8') as f:
            signal = f.read()

    if isinstance(signal, str) and signal == 'true':
        os.environ[configs._STOP_SIGNALS_ENV] = 'true'
        os.environ['CRAWLFRAME_RELOAD_SIGNALS'] = 'stop'
        configs.settings.CRAWLFRAME_STOP_SIGNALS = True
        configs.settings.CRAWLFRAME_RELOAD_SIGNALS = 'stop'
        return
    else:
        with open(signal_file, 'w', encoding='utf-8') as f:
            f.write('%d:%d' % (i, time.time()))
            f.flush()

