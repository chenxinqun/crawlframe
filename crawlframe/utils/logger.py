#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : logger.py
@Author: ChenXinqun
@Date  : 2019/1/19 12:18
'''
import os
import logging
from time import time
from functools import wraps
from logging.handlers import RotatingFileHandler
from crawlframe import configs
from crawlframe.utils.timers import BaseTime


_log_title = lambda: '=== PID:%s ===>' % (os.getpid())


# 日志模块
class BaseLogger:
    _data = {}
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = object.__new__(cls)
        return cls.instance

    def __init__(self, log_name: str, log_file: str=None, log_path=None, log_level: str=None,
                 formatter: tuple or None=None, maxBytes: int= 50 * 1024 * 1024,
                 sh_off: bool=False, sh_level: str=None, file_extension=None):
        '''
        :param log_name: 日志名
        :param log_file: 基于log_name 拼接的文件路径. 默认值为/当前工作路径/log_name.log.
        :param log_path: 基于log_name 拼接的文件路径. 默认值为/当前工作路径/log_name.log.
        :param log_level: 日志级别, 有DEBUG, INFO, WARNING, ERROR, CRITICAL. 默认值为DEBUG
        :param formatter: 日志格式. 默认值为None
        :param maxBytes: 日志文件大小, 单位是字节, 默认值为50M.
        :param sh_off: 是否关闭shell日志(默认不关闭)
        '''

        self.level = log_level or 'DEBUG'
        self.formatter = formatter
        self.maxBytes = maxBytes
        self.sh_off = sh_off
        self.sh_level = sh_level or 'DEBUG'
        self.file_extension = file_extension or '.log'
        if isinstance(self.file_extension, str) and not self.file_extension.startswith('.'):
            self.file_extension = '.%s' % self.file_extension
        self.name = log_name
        self.path = log_path
        if self.path is None and log_file is None:
            self.path = os.getcwd()
        elif self.path is None and log_file:
            self.path = '/'.join('/'.join(log_file.split('\\')).split('/')[:-1])
        # 设置file日志
        self.file = log_file if log_file else os.path.join(self.path, '%s%s' % (self.name, self.file_extension))

        # 初始化日志
        self.logger = self._data.get(self.file) or logging.getLogger(self.file)
        self.logger.setLevel(self.level.upper())
        self._data.setdefault(self.file, self.logger)

        self.logger.propagate = False

        # 设置sheel日志(sheel日志默认'DEBUG'级别)
        if not self.sh_off:
            self.sh = logging.StreamHandler()
            self.sh.setLevel(self.sh_level.upper())

        # 用自带的按大小切割handler(线程安全)
        self.fh = RotatingFileHandler(self.file, mode='a', maxBytes=self.maxBytes, backupCount=10, encoding='utf-8')

        self.fh.setLevel(self.level.upper())

        # 注册日志格式
        fmt = None
        if isinstance(self.formatter, tuple):
            fmt = logging.Formatter(*self.formatter)
        elif isinstance(self.formatter, dict):
            fmt = logging.Formatter(**self.formatter)
        if fmt is not None:
            if not self.sh_off:
                self.sh.setFormatter(fmt)
            self.fh.setFormatter(fmt)

        # 如果此handlers不存在, 注册日志
        if not self.logger.hasHandlers():
            if not self.sh_off:
                self.logger.addHandler(self.sh)
            self.logger.addHandler(self.fh)

    def __str__(self):
        return '<{}.{} object at {} path:"{}" file:"{}">'.format(
            self.__class__.__module__, self.__class__.__name__, self.name, self.path, self.file
        )

    __repr__ = __str__

    def debug(self, message, *args):
        self.logger.debug(message, *args)

    def info(self, message, *args):
        self.logger.info(message, *args)

    def warning(self, message, *args):
        self.logger.warning(message, *args)

    def war(self, message, *args):
        self.warning(message, *args)

    def error(self, message, *args):
        self.logger.error(message, *args)

    def err(self, message, *args):
        self.error(message, *args)

    def exception(self, message, *args):
        self.logger.exception(message, *args)

    def exc(self, message, *args):
        self.exception(message, *args)

    def critical(self, message, *args):
        self.logger.critical(message, *args)

    def cri(self, message, *args):
        self.critical(message, *args)

    def do_log(self, level, msg):
        level_log = self.__getattribute__(level)
        level_log(msg)


class CrawlLogger(BaseLogger):
    def __init__(self, log_name: str, log_path=None, log_level='DEBUG', file_extension=None, **kwargs):
        '''
        :param log_name: 必传参数.
        :param spider: 一个爬虫实例或者爬虫类.
        :param kwargs: BaseLogger所需的关键词参数
        '''
        _log_name = '%s_spider_log' % log_name
        _path = log_path or configs.settings.LOGGER_PATH
        formatter = configs.settings.SPIDER_LOGGER_FORMATTER
        _extension = file_extension or configs.settings.LOGGER_FILE_EXTENSION
        super().__init__(
            _log_name, log_path=_path, formatter=formatter, log_level=log_level.upper(),
            file_extension=_extension, **kwargs
        )


class SlowLogger(BaseLogger):
    def __init__(self, slow_type, spider_name):
        log_name = '%s_slow_log' % slow_type
        if spider_name:
            log_name = '%s_%s_slow_log' % (spider_name, slow_type)
        if slow_type == 'request':
            log_file = configs.settings.SLOW_LOGGER_REQUEST_FILE
            log_path = configs.settings.SLOW_LOGGER_REQUEST_PATH or configs.settings.LOGGER_PATH
        elif slow_type == 'query':
            log_file = configs.settings.SLOW_LOGGER_QUERY_FILE
            log_path = configs.settings.SLOW_LOGGER_QUERY_PATH or configs.settings.LOGGER_PATH
        else:
            log_file = None
            log_path = configs.settings.LOGGER_PATH
        _extension = configs.settings.LOGGER_FILE_EXTENSION or '.log'
        super().__init__(log_name=log_name, log_path=log_path, log_file=log_file, file_extension=_extension)


def _get_slow_log(slow_type, request=None):
    if slow_type == 'request' and request:
        # 慢日志不关闭时才创建日志对象, 节省内存开销.
        if not configs.settings.SLOW_LOGGER_REQUEST_OFF:
            logger = SlowLogger(slow_type='request', spider_name=request.spider_name)
            return logger

    elif slow_type == 'query':
        # 慢日志不关闭时才创建日志对象, 节省内存开销.
        if not configs.settings.SLOW_LOGGER_QUERY_OFF:
            logger = SlowLogger(slow_type='request', spider_name=str(os.getpid()))
            return logger


# 慢日志(记录慢的进程_or_函数_or_查询)
def slow_log(slow_type, threshold, log=None, slow_off=True, log_title=None, timezone=None):
    '''
    接收两个参数, 装饰在views函数上(配置路由时的views_func)
    或者 装饰在数据库查询函数上(目前仅apps.models.postgresql.DbModel.select)
    :param slow_type: 慢日志类型('request' or 'query'), request类型则记录请求耗时, query类型则记录数据库查询耗时.
    :param threshold: 慢日志阈值(超过这个值才记录日志)
    :param slow_off: 慢日志是否关闭, 为True表示关闭, 为Flase表示不关闭)
    :param log: 记录慢日志的日志对象
    :param log_title: 记录日志时的日志头.
    :return: 返回原函数运行结果
    '''

    def record_log(func):
        @wraps(func)
        def warp(*args, **kwargs):
            # 计算运行时间
            time1 = time()
            result = func(*args, **kwargs)
            time2 = time()
            timer = time2 - time1
            # 只有slow_off 为Flase的时候, 才开启慢日志记录
            if not slow_off:
                if timer > threshold:
                        if log_title is None:
                            title = _log_title()
                        else:
                            title = log_title
                        if timezone is None:
                            tz = 'Asia/Shanghai'
                        else:
                            tz = timezone
                        msg_fmt = '[{} start:{} end:{} timer:{} {}]'
                        start = '%s_%s ' % (tz, BaseTime.stamp_to_date(time1, tz).strftime(configs.settings.LOGGER_DATE_FORMAT))
                        end = '%s_%s ' % (tz, BaseTime.stamp_to_date(time2, tz).strftime(configs.settings.LOGGER_DATE_FORMAT))
                        if slow_type == 'request':
                            request = args[1]
                            _log = log
                            if _log is None:
                                _log = _get_slow_log(slow_type, request)
                            if isinstance(_log, BaseLogger):
                                _log.debug(msg_fmt.format(title, start, end, timer, 'request:%s' % request))
                        elif slow_type == 'query':
                            _log = log
                            if _log is None:
                                _log = _get_slow_log(slow_type)
                            if isinstance(_log, BaseLogger):
                                execute_sql = args[0].get_query(args[1])
                                _log.debug(msg_fmt.format(title, start, end, timer, 'sql:<%s>' % execute_sql))
            return result
        return warp
    return record_log