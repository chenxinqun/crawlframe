#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : __init__.py
@Author: ChenXinqun
@Date  : 2019/1/18 16:26
'''


__all__ = [
    '__title__',
    '__description__',
    '__url__',
    '__version__',
    '__author__',
    '__author_email__',
    '__license__',
    '__copyright__',
    'cores',
    'utils',
    'cmd'
]


from .__version__ import (
    __title__,
    __description__,
    __url__,
    __version__,
    __author__,
    __author_email__,
    __license__,
    __copyright__
)


from crawlframe.configs import Settings
from crawlframe.utils.pools import Pool
from crawlframe.utils.pools import task_pools
from crawlframe.utils.queues import task_queues
from crawlframe.cores.parsers.parser import HtmlParser
from crawlframe.cores.parsers.parcel import ParcelItems
from crawlframe.cores.crawlers import BaseCrawler as Crawler
from crawlframe.cores.requests.requester import HttpRequest as Requester
from crawlframe.cores.requests.requester import FromRequest as FromRequester
from crawlframe.cores.downloader.download import HttpDownload as Downloader
from crawlframe.cores.middlewares.spider import SpiderMiddleware as SpiderMiddle
from crawlframe.cores.middlewares.request import RequestMiddleware as RequestMiddle
from crawlframe.cores.middlewares.response import ResponseMiddleware as ResponseMiddle