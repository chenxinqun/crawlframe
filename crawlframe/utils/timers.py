#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : timers.py
@Author: ChenXinqun
@Date  : 2019/1/19 12:20
'''
import time
from asyncio import (
    sleep,
    events,
    run_coroutine_threadsafe
)
from pytz import timezone
from datetime import datetime


# 时间处理基类
class BaseTime:
    @staticmethod
    def time_now(tz: str='Asia/Shanghai'):
        '''返回指定时区的当前时间的datetime对象. 默认为上海时区'''
        # 生成时区对象
        timez = timezone(tz)
        # 获取该时区的当前时间
        return datetime.now(timez)

    @staticmethod
    def target_time(tm: int, target_tz: str, local_tz: str='Asia/Shanghai'):
        '''将Unix时间戳转换成任意时区时间的datetime对象'''
        # 将时间戳转换成带本地时区信息的datetime对象
        date = BaseTime.stamp_to_date(tm, local_tz)
        # 转换成目标时区
        return date.astimezone(BaseTime.time_zone(target_tz))

    @staticmethod
    def timestamp(time_obj: datetime or time.struct_time=None):
        '''获取任意时间日期的Unix时间戳'''
        # 如果是datetime对象
        if isinstance(time_obj, datetime):
            result = time_obj.timestamp()
        # 如果是struct_time对象, 且有传时间格式
        elif isinstance(time_obj, time.struct_time):
            result = time.mktime(time_obj)
        # 不是以上情况, 默认返回当前时间的Unix时间戳.
        else:
            result = time.time()
        return result

    @staticmethod
    def str_to_date(tm_str, tm_fmt, dt=True):
        '''默认返回datetime对象, dt=Flase 返回struct_time对象'''
        if dt:
            result = datetime.strptime(tm_str, tm_fmt)
        else:
            result = time.strptime(tm_str, tm_fmt)
        return result

    @staticmethod
    def stamp_to_date(tm, tz: str=None, dt=True):
        '''时间戳转换为时间对象. tz为指定时区信息. dt为True返回datetime对象, dt为False返回struct_time对象(不包含时区信息)'''
        # 时间戳只取前10位.
        tm = int(str(tm)[:10])
        if dt:
            result = datetime.fromtimestamp(tm)
            if isinstance(tz, str):
                result = BaseTime.date_zone(result, tz)
        else:
            result = time.localtime(tm)
        return result

    @staticmethod
    def date_zone(date_obj: datetime, tz: str):
        '''将datetime对象, 包装成带时区的datetime对象'''
        return BaseTime.time_zone(tz).localize(date_obj)

    @staticmethod
    def time_zone(tz: str):
        '''返回一个pytz时区对象'''
        return timezone(tz)

    @staticmethod
    def time_to_str(tm, tm_fmt: str):
        '''时间转换成字符串'''
        if isinstance(tm, datetime):
            return tm.strftime(tm_fmt)
        elif isinstance(tm, time.struct_time):
            return time.strftime(tm_fmt, tm)
        elif isinstance(tm, int) or isinstance(tm, float):
            return datetime.fromtimestamp(tm).strftime(tm_fmt)
        elif isinstance(tm, str) and tm.isdigit():
            return time.strftime(tm_fmt, time.localtime(int(tm)))
        else:
            raise TypeError('%s "tm" parameter error!' % tm)


async def async_sleep(seconds):
    await sleep(seconds)


def task_sleep(seconds=1):
    loop = events.get_event_loop()
    future = run_coroutine_threadsafe(async_sleep(seconds), loop)
    try:
        future.result(timeout=seconds + 1)
    except:
        future.cancel()
