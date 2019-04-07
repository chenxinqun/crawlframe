#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : spiders_container.py
@Author: ChenXinqun
@Date  : 2019/2/19 14:16
'''
from collections import UserDict


class SpidersContainer(UserDict):
    def __missing__(self, key):
        if isinstance(key, str):
            raise KeyError(key)
        return self[str(key)]

    def __contains__(self, key):
        return str(key) in self.data

    def __setitem__(self, key, item):
        self.data[str(key)] = item

    def __getattr__(self, key):
        return self.data[str(key)]