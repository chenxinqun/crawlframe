#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : __init__.py
@Author: ChenXinqun
@Date  : 2019/1/18 16:26
'''
import time
import weakref
from collections import defaultdict

live_refs = defaultdict(weakref.WeakKeyDictionary)


# 弱引用对象
class BaseRef(object):

    __slots__ = ()

    def __new__(cls, *args, **kwargs):
        obj = object.__new__(cls)
        live_refs[cls][obj] = time.time()
        return obj