#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : default_settings.py
@Author: ChenXinqun
@Date  : 2019/1/18 16:50
'''
# 爬虫工作路径
SPIDER_CHDIR = None

# 爬虫框架缓存文件夹
CRAWLFRAME_CACHE_DIR = None

# 代码修改后是否自动重载(默认为False)
SPIDER_RELOAD = False

# 任务队列长度
TASKS_QUEUE_MAXSIZE = 10
# 任务运行超时时间
TASKS_RUN_TIMEOUT = 60
# 加入队列超时时间(秒)
TASKS_PUT_TIMEOUT = 60
# 获取队列超时时间(毫秒)
TASKS_GET_TIMEOUT = 1
# 是否同步爬虫
CRAWLFRAME_SYNC_TASKS = False
# 执行多少个次请求重启爬虫(并非精准停止, 实际执行过程中, 有可能超出这个数才会重启)
CRAWLFRAME_SURVIVE_MAX = 1000

# 重启开关
CRAWLFRAME_SURVIVE_SWITCH = False

# 线程总数
TASKS_THREAD_MAXSIZE = 20

# 当队列为空时请求时间的间隔(毫秒).最大支持1000, 也就是1秒.通常情况下不要修改这个值.
TASKS_GET_INTERVAL = 10

# 接收停止信号的时间间隔(S)
RECEIVED_STOP_SIGNALS_INTERVAL = 10

# 全局日志时间模板(设置了之后, 会对慢日志与默认的爬虫日志模板生效.)
LOGGER_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# 全局日志文件拓展名(设置了之后, 会对慢日志与爬虫日志同时生效.)
LOGGER_FILE_EXTENSION = '.log'

# 全局日志保存路径, 如果不指定则默认为工程根目录.
LOGGER_PATH = None

# 爬虫框架日志模板, 是一个字典, 包含了日志模板的两个关键词参数, fmt与datefmt.
# 也可以是一个元组, 下标0是fmt, 下标1是datefmt.
SPIDER_LOGGER_FORMATTER = dict(
    fmt='GMT+8:%(asctime)s PID:%(process)d TID:%(thread)d TNAME:%(threadName)s MSG:%(message)s',
    datefmt=LOGGER_DATE_FORMAT,
)


# request慢请求日志是否关闭
SLOW_LOGGER_REQUEST_OFF = True
# request慢日志的消息头.
SLOW_LOGGER_REQUEST_TITLE = None
# request慢日志文件保存路径, 如果不指定则默认为LOGGER_PATH
SLOW_LOGGER_REQUEST_PATH = None
# request慢日志文件显示的时间, 所用到的时区, 如果不指定,则默认为上海时区.
SLOW_LOGGER_REQUEST_TIMEZONE = None
# request慢日志文件路径(必须是绝对路径, 前缀路径最好是SLOW_LOGGER_REQUEST_PATH), 一般不需要指定. 不指定时,
# 日志文件为SLOW_LOGGER_REQUEST_PATH /spider_name_request_slow.log
SLOW_LOGGER_REQUEST_FILE = None
# request慢请求日志阈值(秒)
SLOW_LOGGER_REQUEST_THRESHOLD = 10

# query慢请求日志是否关闭
SLOW_LOGGER_QUERY_OFF = True
# query慢日志的消息头.
SLOW_LOGGER_QUERY_TITLE = None
# query慢日志文件保存路径, 如果不指定则默认为LOGGER_PATH
SLOW_LOGGER_QUERY_PATH = None
# query慢日志文件显示的时间, 所用到的时区, 如果不指定,则默认为上海时区.
SLOW_LOGGER_QUERY_TIMEZONE = None
# query慢日志文件路径(必须是绝对路径, 前缀路径最好是SLOW_LOGGER_QUERY_PATH), 一般不需要指定. 不指定时,
# 日志文件为SLOW_LOGGER_QUERY_PATH /os.getpid()_query_slow.log
SLOW_LOGGER_QUERY_FILE = None
# query慢查询日志阈值(秒)
SLOW_LOGGER_QUERY_THRESHOLD = 0.1

# 需要安装的APP
# 配置为一个列表, 列表内的元素是字典,
# 每一个字典的key都必须是能指向爬虫模块(BaseCrawler子类所在的模块)的字符串.
# value则是爬虫类的类名.
# 如: INSTALLED_SPIDER = [{'crawlframe.cores.crawlers': 'BaseCrawler'}]
INSTALLED_SPIDER = []


'''
需要安装的中间件
配置为一个列表, 列表内的元素是一个字典.
字典的key, 是一个能指向中间件模块
(即SpiderMiddleware, RequestMiddleware, ResponseMiddleware子类所在的模块)的字符串.
字典value也是一个列表, 
列表内的元素必须是二元素元组
元组下标0的值, 是中间件模块内具体的中间件类名, 
元组下标1的值, 则是一个int型, 范围为1-100. 表示中间件执行的优先级(数字越小, 越早执行)
如: 
INSTALLED_MIDDLEWARE = [
        {
            'crawlframe.cores.middlewares.request': [
                ('RequestMiddleware3', 30),
                ('RequestMiddleware1', 10),
                ('RequestMiddleware2', 20),
        ],
    }
]
'''
INSTALLED_MIDDLEWARE = []


