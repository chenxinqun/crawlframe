#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : response.py
@Author: ChenXinqun
@Date  : 2019/1/19 12:06
'''
import lxml.etree as etree
from bs4 import BeautifulSoup
from requests.models import Response as response

from crawlframe.cores.requests import BaseRef
from crawlframe.cores.requests.requester import HttpRequest


class CrawlResponse(BaseRef):
    def __init__(self, resp, request):
        '''
        会继承request的所有属性, 并且新增request属性, resp属性, text属性, status_code属性.
        并替换request.cookies为response.cookies. 如果想要查看request属性, 请访问self.request.cookies.
        :param resp: 一个 requests.models.Response 对象
        :param request: 一个Spider.Base.BaseDownload.Request 对象
        '''
        self._set_request(request)
        self._set_attribute()
        self._set_resp(resp)
        self.cookies = self.resp.cookies
        self.html = self.resp.text
        self.status_code = self.resp.status_code
        self._set_bs4_obj()
        self._set_xpath_obj()

    def __str__(self):
        return '<{}.{} {} {} {}>'.format(self.__class__.__module__, self.__class__.__name__, self.method, self.status_code, self.url)

    __repr__ = __str__

    def _set_request(self, request):
        if request is not None and not isinstance(request, HttpRequest):
            raise TypeError('data must be a %s, got %s' % (HttpRequest.__class__, type(request).__name__))
        self.request = request

    def _set_attribute(self):
        if hasattr(self.request, '__dict__'):
            self.__dict__.update(self.request.__dict__)

    def _set_resp(self, resp):
        if resp is not None and not isinstance(resp, response):
            raise TypeError('data must be a %s, got %s' % (response.__class__, type(resp).__name__))
        self.resp = resp

    def _set_bs4_obj(self):
        try:
            self.bs4_obj = BeautifulSoup(self.html, 'lxml')
        except AttributeError:
            raise AttributeError(
                "Response.resp not available, this resp must be 'requests.models.Response' instance!"
            )
        except Exception as e:
            raise e

    def _set_xpath_obj(self):
        try:
            self.xpath_obj = etree.HTML(self.html)
        except AttributeError:
            raise AttributeError(
                "Response.resp not available, this resp must be 'requests.models.Response' instance!"
            )
        except Exception as e:
            raise e