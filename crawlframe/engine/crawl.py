#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : crawl.py
@Author: ChenXinqun
@Date  : 2019/1/19 12:20
'''

'''
启动程序用
支持工程级别的启动.
要控制哪些APP不启动, 需要写在配置中.
需要实现以下事情.
由start脚本调用.接收参数(app:name, project:name).
根据参数加载相应的配置, 生成配置对象.
主进程是监护进程, 子进程才进行工作.
子进程内部只用协程
子进程用subprocess启动
'''
import warnings
warnings.filterwarnings('ignore')
import gc
import os
import sys
import traceback
try:
    if 'win' not in sys.platform:
        import asyncio
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except:
    pass
from asyncio import (
    wait,
    sleep,
    events,
    get_event_loop
)

from importlib import (
    import_module,
    reload
)

from crawlframe import configs
from crawlframe.utils import logger
from crawlframe.utils.errors import InputError
from crawlframe.utils.reload_module import reload_module
from crawlframe.engine.spiders_container import SpidersContainer
from crawlframe.cores.downloader import MainDownloader
from crawlframe.utils.signals import (
    received_stop_signals,
    CRAWLFRAME_STOP_SIGNALS
)


def input_checkout(args_in, input_type='start'):
    if input_type == 'start':
        if not (args_in.startswith('app:') or args_in == 'project'):
            raise InputError(input_type)
    elif input_type == 'stop':
        if not (args_in.isdigit() or args_in.startswith('app:') or args_in == 'project'):
            raise InputError(input_type)


async def _logger_war(msg=None):
    if hasattr(logger, 'crawlframe_logger'):
        await logger.crawlframe_logger.warning(msg)


async def while_func(func, slp=1, args=None, loop=None):
    if loop is None:
        loop = events.get_event_loop()
    i = 1
    while True:
        if hasattr(func, '__self__'):
            msg_name = func.__self__.__class__
        else:
            msg_name = func
        if isinstance(getattr(configs.settings, 'CRAWLFRAME_RELOAD_SIGNALS', False), str) and getattr(configs.settings, 'CRAWLFRAME_RELOAD_SIGNALS') == 'stop':
            print(
                'configs.settings.CRAWLFRAME_RELOAD_SIGNALS.stop ' +
                'pid:%s ,%s thread stop!' % (os.getpid(), msg_name.__name__)
            )
            return

        if hasattr(configs.settings, 'CRAWLFRAME_STOP_SIGNALS') and configs.settings.CRAWLFRAME_STOP_SIGNALS:
            if msg_name.__name__ != 'reload_app':
                _logger_war(
                    'configs.settings.CRAWLFRAME_STOP_SIGNALS ' +
                    'pid:%s ,%s thread stop!' % (os.getpid(), msg_name.__name__)
                )
                return

        if CRAWLFRAME_STOP_SIGNALS() == 'true':
            if msg_name.__name__ != 'reload_app':
                _logger_war(
                    'signals.CRAWLFRAME_STOP_SIGNALS ' +
                    'pid:%s ,%s thread stop!' % (os.getpid(), msg_name.__name__)
                )
                return

        if os.getenv(configs._STOP_SIGNALS_ENV, '') == 'true':
            if msg_name.__name__ != 'reload_app':
                _logger_war(
                    'os.environ[%s]' % configs._STOP_SIGNALS_ENV +
                    'pid:%s ,%s thread stop!' % (os.getpid(), msg_name.__name__)
                )
                return

        try:
            if func.__name__ == 'received_stop_signals':
                await loop.run_in_executor(None, func, i)
            elif func.__name__ == reload_module.__name__:
                await loop.run_in_executor(None, func)
            elif func.__name__ == 'reload_app':
                func(*args)
            elif func.__name__ == 'guard':
                await func(i, *args)
            elif func.__name__ == 'crawler':
                await func(*args, i)
            else:
                func()
        except Exception as e:
            if hasattr(logger, 'crawlframe_error'):
                logger.crawlframe_error.exception('%s error, ' % func.__name__)
                logger.crawlframe_error.critical(str(traceback.format_exc()))
            else:
                print(e)
                traceback.print_exc()
        i += 1
        if not func.__name__ == 'guard':
            await sleep(slp)


async def guard(i, container, func):
    try:
        os.environ.pop(configs._SURVIVE_SIGNALS_ENV)
    except:
        pass
    if configs.settings.CRAWLFRAME_SURVIVE_SWITCH:
        if hasattr(configs.settings, configs._SURVIVE_SIGNALS_ENV) and isinstance(
                getattr(configs.settings, configs._SURVIVE_SIGNALS_ENV), int):
            # 执行一定请求数, 重启爬虫(可配置, 默认1000)
            if container.count >= getattr(configs.settings, configs._SURVIVE_SIGNALS_ENV):
                os.environ[configs._SURVIVE_SIGNALS_ENV] = 'true'
                os.environ[configs._STOP_SIGNALS_ENV] = 'true'
                configs.settings.CRAWLFRAME_STOP_SIGNALS = True
                configs.settings.CRAWLFRAME_RELOAD_SIGNALS = True
                return
    return await func(container)


async def crawler(spider_run, i):
    return await spider_run()


async def operation(app_name, loop=None):
    # 获取settings模块
    settings_mod_name = os.getenv(configs._SETTINGS_ENV)
    if isinstance(settings_mod_name, str):
        settings_mod = import_module(settings_mod_name)
        settings = configs.get_settings(settings_mod)
        configs.settings = settings
        crawlframe_logger = logger.CrawlLogger('crawlframe_logger', log_level='info')
        crawlframe_error = logger.CrawlLogger('crawlframe_error', log_level='error')
        logger.crawlframe_logger = crawlframe_logger
        logger.crawlframe_error = crawlframe_error
        configs.settings.CRAWLFRAME_RELOAD_SIGNALS = False
        spiders_container = SpidersContainer()
        if spiders_container.count > 0:
            spiders_container.count = 0
        for spiders in settings.INSTALLED_SPIDER:
            if isinstance(spiders, dict):
                for mod, sp in spiders.items():
                    spider_mod = import_module(mod)
                    spider_obj = getattr(spider_mod, sp)
                    if spider_obj.name == app_name:
                        spider_obj._container_ = spiders_container
                        spiders_container[spider_obj.name] = spider_obj
                        for conf in configs.settings.__dict__:
                            if hasattr(spider_obj, conf):
                                configs.settings.__dict__[conf] = getattr(spider_obj, conf)
                            elif hasattr(spider_obj, conf.lower()):
                                configs.settings.__dict__[conf] = getattr(spider_obj, conf.lower())

        from crawlframe.utils import pools
        from crawlframe.utils import queues
        reload(pools)
        reload(queues)
        pools.Pool.timeout = configs.settings.TASKS_RUN_TIMEOUT
        pools.task_pools = pools.get_task_pools()
        pools.task_pools.pool = pools.task_pools.new()
        queues.task_queues = queues.get_task_queues()
        queues.task_queues.task = queues.task_queues.new()
        queues.task_queues.next = queues.task_queues.new()
        from crawlframe.cores import crawlers
        from crawlframe.cores import downloader
        reload(crawlers)
        reload(downloader)
        from crawlframe.utils import middle
        reload(middle)
        middle.middleware = middle.get_middleware()
        spider = spiders_container[app_name]()
        middle.install_spider_middle(spider)
        download = MainDownloader()
        spider._task_queues = queues.task_queues
        download._task_queues = queues.task_queues
        download._task_pools = pools.task_pools
        if loop is None:
            loop = events.get_event_loop()
        task_list = [
            loop.create_task(while_func(received_stop_signals, slp=configs.settings.RECEIVED_STOP_SIGNALS_INTERVAL)),
            loop.create_task(while_func(crawler,args=(
                spider.run, ), loop=loop)),
            loop.create_task(while_func(guard,args=(
                spiders_container, download.run, ), loop=loop)),
        ]
        if settings.SPIDER_RELOAD:
            task_list.append(loop.create_task(while_func(reload_module, slp=60)))
        await wait(task_list)


def crawl_app(args=None):
    '''只启动APP'''
    if args is None:
        args = sys.argv[1:]
    input_checkout(args[0])
    chdir = os.getenv(configs._CHDIR_ENV) or ''
    if os.path.isdir(chdir):
        os.chdir(chdir)
        os.environ[configs._CACHE_ENV] = os.path.join(chdir, 'cache')
    app_name = args[0].split(':')[1] if 'app:' in args[0] else None
    if app_name:
        loop = get_event_loop()
        loop.run_until_complete(loop.create_task(operation(app_name, loop)))



if __name__ == '__main__':

    crawl_app()