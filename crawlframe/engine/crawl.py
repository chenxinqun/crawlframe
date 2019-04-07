#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : crawl.py
@Author: ChenXinqun
@Date  : 2019/1/19 12:20
'''

import warnings
warnings.filterwarnings('ignore')
import gc
import os
import sys
import traceback
try:
    import asyncio
    if 'win' not in sys.platform:
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


def _logger_war(msg=None):
    if hasattr(logger, 'crawlframe_logger'):
        return logger.crawlframe_logger.warning(msg)


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


async def guard(i, spider, download_run):
    obj = download_run.__self__
    count = getattr(spider, 'count', None) or obj._count
    max_survive = getattr(configs.settings, configs._SURVIVE_SIGNALS_ENV)
    if configs.settings.CRAWLFRAME_SURVIVE_SWITCH:
        if isinstance(max_survive, int) and count >= max_survive:
            for i in range(spider.task_len()):
                try:
                    spider._task_queues.task.get_nowait()
                except:
                    pass
                try:
                    spider._task_queues.next.get_nowait()
                except:
                    pass
            # 执行一定请求数, 重启爬虫(可配置, 默认1000)
            configs.settings.CRAWLFRAME_STOP_SIGNALS = True
            obj._count = 0
            spider.count = 0
            return
    return await download_run(spider)


async def crawler(spider_run, i):
    spider = spider_run.__self__
    count = getattr(spider, 'count', None) or 0
    max_survive = getattr(configs.settings, configs._SURVIVE_SIGNALS_ENV)
    if configs.settings.CRAWLFRAME_SURVIVE_SWITCH:
        if isinstance(max_survive, int) and count >= max_survive:
            for i in range(spider.task_len()):
                try:
                    spider._task_queues.task.get_nowait()
                except:
                    pass
                try:
                    spider._task_queues.next.get_nowait()
                except:
                    pass
            # 执行一定请求数, 重启爬虫(可配置, 默认1000)
            configs.settings.CRAWLFRAME_STOP_SIGNALS = True
            spider.count = 0
            return
    return await spider_run()


async def operation(app_name, loop=None, rel=False):
    # 获取settings模块
    settings_mod_name = os.getenv(configs._SETTINGS_ENV)
    if isinstance(settings_mod_name, str):
        settings_mod = import_module(settings_mod_name)
        settings = configs.get_settings(settings_mod)
        configs.settings = settings
        reload(logger)
        crawlframe_logger = logger.CrawlLogger('crawlframe_logger', log_level='info')
        crawlframe_error = logger.CrawlLogger('crawlframe_error', log_level='error')
        logger.crawlframe_logger = crawlframe_logger
        logger.crawlframe_error = crawlframe_error
        if rel:
            _logger_war('CRAWLFRAME RELOAD')
        configs.settings.CRAWLFRAME_RELOAD_SIGNALS = False
        spiders_container = SpidersContainer()
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
        app = spiders_container.get(app_name)
        if not app:
            raise NameError('The spider app: %s not found!' % app_name)
        spider = app()
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
                spider, download.run, ), loop=loop)),
        ]
        if settings.SPIDER_RELOAD:
            task_list.append(loop.create_task(while_func(reload_module, slp=60)))
        await wait(task_list)
        del spider
        del download
        del task_list
        del middle.middleware
        del pools.task_pools
        del queues.task_queues
        del spiders_container
        gc.collect()


import time
def crawl_app(args=None, rel=False):
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
        if 'win' in sys.platform:
            loop = asyncio.ProactorEventLoop()
            asyncio.set_event_loop(loop)
        else:
            loop = get_event_loop()
        run = lambda: loop.run_until_complete(operation(app_name, loop, rel))
        while 1:
            reload_signals = os.environ.get('CRAWLFRAME_RELOAD_SIGNALS') or \
                             getattr(configs.settings, 'CRAWLFRAME_RELOAD_SIGNALS', None)
            stop_signals = os.environ.get(configs._STOP_SIGNALS_ENV) or ''
            if isinstance(reload_signals, str) and reload_signals == 'stop':
                break
            if stop_signals == 'true':
                break
            configs.settings.CRAWLFRAME_STOP_SIGNALS = False
            configs.settings.CRAWLFRAME_RELOAD_SIGNALS = False
            run()
            gc.collect()
            time.sleep(1)
        loop.close()
        del loop
    sys.exit()


if __name__ == '__main__':

    crawl_app()