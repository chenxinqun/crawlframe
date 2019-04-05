#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : errors.py
@Author: ChenXinqun
@Date  : 2019/1/22 16:31
'''


class InputError(BaseException):
    def __init__(self, input_type='start'):
        if input_type == 'start':
            must_be = '"app:name" or "project"'
        elif input_type == 'stop':
            must_be = '"pid" or "app:name" or "project"'
        else:
            must_be = ''
        err = 'InputError: Input args must be %s!' % must_be
        super().__init__(self, err)