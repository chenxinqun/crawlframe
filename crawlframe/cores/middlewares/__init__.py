#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : __init__.py
@Author: ChenXinqun
@Date  : 2019/1/18 16:26
'''
'''
中间件含三种, spider, request, response.
分别可以对爬虫实例, 请求实例与响应实例做一些额外操作.
因此一些记录日志, 限制请求频率等等功能, 可以单独写在中间件里面, 以保持程序逻辑的纯粹性.
同时spider中间件可以给spider增加一些方法, 或者替换原有方法, 在不修改原代码的基础上,修改爬虫逻辑
因此可以用来给spider打补丁.
 '''