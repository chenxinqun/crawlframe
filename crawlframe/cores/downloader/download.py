#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : download.py
@Author: ChenXinqun
@Date  : 2019/2/13 10:17
'''
from requests import api
from crawlframe import configs
from crawlframe.utils.logger import slow_log
from crawlframe.utils.middle import install_middle
from crawlframe.cores.requests.requester import HttpRequest
from crawlframe.cores.requests.response import CrawlResponse


class BaseHttpDownload:
    def __init__(self):
        '''
        实例化不需要传参.
        实例化之后, 会获得两个属性.
        Reqt, 一个 requests.api.request对象, 用于发送请求, 也是下载器的核心主体.
        Resp, 一个crawlframe.cores.requests.response.CrawlResponse对象, 封装好的下载器返回值.
        '''
        self._request = api.request
        self.response = CrawlResponse

    @property
    def need_params(self):
        return dict(
            url=None,
            method='GET',
            headers=None,
            cookies=None,
            proxies=None,
            verify=None,
            timeout=180,
        )

    # 参数过滤
    @staticmethod
    def filter_params(need_param=None, params=None):
        if not isinstance(need_param, dict):
            raise TypeError(
                "The 'need_param' must be dict, got %s" % type(need_param).__name__
            )
        if not isinstance(params, dict):
            raise TypeError(
                "The 'params' must be dict, got %s" % type(params).__name__
            )
        give_up_params = []
        for param in need_param:
            need_param[param] = params.get(param) or need_param[param]
        for param in need_param:
            if need_param[param] is None:
                give_up_params.append(param)
        for param in give_up_params:
            need_param.pop(param)
        return need_param

    def _set_headers(self, reqt):
        '''子类必须实现此方法'''
        raise NotImplementedError

    def _set_proxies(self, reqt):
        '''子类必须实现此方法'''
        raise NotImplementedError

    # 慢请求日志装饰器
    @slow_log(
        'request', configs.settings.SLOW_LOGGER_REQUEST_THRESHOLD,
        slow_off=configs.settings.SLOW_LOGGER_REQUEST_OFF,
        log_title=configs.settings.SLOW_LOGGER_REQUEST_TITLE,
        timezone=configs.settings.SLOW_LOGGER_REQUEST_TIMEZONE
    )
    def request(self, reqt):
        params = self.filter_params(need_param=self.need_params, params=reqt.__dict__)
        r = self._request(**params)
        r.raise_for_status()
        return r

    @install_middle('request')
    def download(self, reqt):
        reps = self.request(reqt)
        return reps

    @install_middle('response')
    def result(self, resp, reqt):
        return self.response(resp, reqt)


class HttpDownload(BaseHttpDownload):
    def __init__(self):
        super(HttpDownload, self).__init__()

    def _set_headers(self, reqt):
        '''子类覆写此方法, 或者通过中间件定义headers'''
        if not hasattr(reqt, 'headers') or not isinstance(reqt.headers, dict):
            reqt.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}

    def _set_proxies(self, reqt):
        '''子类覆写此方法, 或者通过中间件定义proxies'''
        if not hasattr(reqt, 'proxies') or not isinstance(reqt.proxies, dict):
            reqt.proxies = None

    def __call__(self, reqt):
        if not isinstance(reqt, HttpRequest):
            raise TypeError('the reqt must be %s instance, got %s!' % (HttpRequest, type(reqt).__name__))
        self._set_headers(reqt)
        self._set_proxies(reqt)
        # 发起请求
        resp = self.download(reqt)
        # 打包响应体
        response = self.result(resp, reqt)
        return response