#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : requester.py
@Author: ChenXinqun
@Date  : 2019/2/13 10:21
'''
from logging import Logger
from crawlframe.cores.requests import BaseRef
from crawlframe.utils.logger import BaseLogger


class HttpRequest(BaseRef):
    def __init__(self, url, callback=None, method='GET', headers=None,
                 cookies=None, encoding='utf-8', log=None, meta=None, timeout=None, spider_name=None, **kwargs):
        '''
        一个发送请求的预处理对象
        不指定回调函数时, 只做存储对象用. 用于存储一些发送网络请求所必须的参数.
        如果指定了回调函数, 则变身为一个请求处理函数. 具体请求逻辑, 请在回调函数中实现.
        回调函数用callback关键字参数传参, 必须为一个callable对象.
        :param url: 请求的URL
        :param callback: 回调函数
        :param method: 请求方法, 默认'GET'
        :param headers: 请求头
        :param cookies: 请求cookie
        :param encoding: 字符集编码
        :param log: 可以是一个log object 则此log object最好是crawlframe.utils.logger.BaseLogger及其子类的instance.
        当然也可以是logging.Logger及其子类的instance.
        如果log object合法的话, request会增加一个self.log属性. 如果不合法, 会抛出一个TypeError.
        :param meta: 一些额外的参数, 用以拓展程序. 建议传一个字典
        :param timeout: 超时时间. int s
        :param spider_name: 爬虫名, 建议传Crawler类的name
        (一般而言, 只有指定的callback对象不是Crawler类的方法时, 才需要传此参数).
        不传则默认为callback.__self__.name
        :param kwargs: 可能用到的拓展参数
        '''
        self._encoding = encoding
        self.method = str(method).upper()
        self.cookies = cookies
        self.timeout = timeout
        self._set_url(url)
        self._set_headers(headers)
        self._set_meta(meta)
        self._set_callback(callback)
        self._set_spider_name(callback, spider_name)
        if log is not None:
            self._set_log(log)
        self.kwargs = kwargs

    def __str__(self):
        return '<{}.{} {} {} callback:{}>'.format(self.__class__.__module__, self.__class__.__name__, self.method, self.url, self.callback)

    __repr__ = __str__

    def _set_url(self, url):
        if not isinstance(url, str):
            raise TypeError('Request url must be str or unicode, got %s:' % type(url).__name__)
        self.url = url
        if ':' not in self.url:
            raise ValueError('Missing scheme in request url: %s' % self.url)

    def _set_callback(self, callback):
        if callback is not None and not callable(callback):
            raise TypeError('callback must be a callable, got %s' % type(callback).__name__)
        self.callback = callback

    def _set_headers(self, headers):
        if headers is not None and not isinstance(headers, dict):
            raise TypeError('headers must be a headers, got %s' % type(headers).__name__)
        self.headers = headers

    def _set_spider_name(self, callback, spider_name):
        self.spider_name = spider_name
        if not self.spider_name:
            self.spider_name = callback.__self__.name

    def _set_log(self, log):
        if isinstance(log, BaseLogger):
            self.log = log
        elif isinstance(log, Logger):
            self.log = log
        elif isinstance(log, object):
            self.log = log
        else:
            raise TypeError('log must be string '
                            'or "crawlframe.utils.logger.BaseLogger" instance '
                            'or "logging.Logger" instance!')

    def _set_meta(self, meta):
        if meta is not None and not isinstance(meta, dict):
            raise TypeError('headers must be a headers, got %s' % type(meta).__name__)
        self.meta = meta

    def __call__(self):
        # 如果回调函数是一个callable对象自动调用回调函数
        if callable(self.callback):
            return self.callback(self)


class FromRequest(HttpRequest):

    def __init__(self, url, callback=None, method='POST', data=None, headers=None,
                 cookies=None, log=None, encoding='utf-8', meta=None, spider_name=None, **kwargs):
        '''
        继承自Spider.Base.BaseDownload.Request
        :param url: 请求URL
        :param method: 请求方法,必须为'POST'
        :param data: post请求所需的FromData
        :param headers: 请求头
        :param cookies: 必传来自于get请求或cookie池的cookies,
        如果是连贯的get => post 请求, headers的UserAgent请用与get请求相同的UserAgent
        :param encoding: 字符集编码
        :param log: 可以是一个log object 则此log object最好是crawlframe.utils.logger.BaseLogger及其子类的instance.
        当然也可以是logging.Logger及其子类的instance.
        如果log object合法的话, request会增加一个self.log属性. 如果不合法, 会抛出一个TypeError.
        :param meta: 一些额外的参数, 用以拓展程序. 建议传一个字典
        :param spider_name: 爬虫名, 建议传Crawler类的name
        (一般而言, 只有指定的callback对象不是Crawler类的方法时, 才需要传此参数).
        不传则默认为callback.__self__.name
        :param kwargs: 未来可能用到的拓展参数
        '''
        super(FromRequest, self).__init__(
            url, callback=callback, method=method,
            headers=headers, cookies=cookies, log=log, encoding=encoding, meta=meta,
            spider_name=spider_name, **kwargs
        )
        # 设置data属性
        self._set_data(data)

    def _set_data(self, data):
        if data is not None and not isinstance(data, dict):
            raise TypeError('data must be a dict, got %s' % type(data).__name__)
        self.data = data