#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : __init__.py
@Author: ChenXinqun
@Date  : 2019/1/18 16:26
'''


# 解析基类, 适用于单字段解析.
class BaseParse:
    '''
    子类继承调用super方法后,
    调用 self.register 方法,
    传入子类新增的方法名,
    即可注册子类新增方法,
    使方法自动运行
    '''
    def __init__(self):
        # 设置启动方法, 默认为'run', 如无特殊需要, 子类不要修改
        self.__run = 'run'  # self.__run: str
        # 设置方法数组(一个方法名列表, 默认为空, 由子类注册进来)
        self.__func_array = []    # self.__func_array: List[str]

    def register(self, func_name: str):
        '''注册新方法(子类有新增方法, 则在子类__init__方法中调用此方法, 传入新增方法名, 注册此方法)'''
        self.__func_array.append(func_name)

    def set_run(self, run_name: str):
        '''设置启动方法(如无特殊需要, 子类不要调用此方法)'''
        self.__run = run_name

    def get_run_name(self):
        '''获取run方法'''
        return self.__run

    def get_func_array(self):
        '''获取注册方法列表'''
        return self.__func_array

    def run(self, params: dict):
        '''
        启动方法, 指定为run方法.子类中如果更换了启动方法, 需要将启动方法的方法名赋值给 __run 变量
        :param params: 一个字典
        :return: 返回整个实例的运行结果
        '''
        for func in self.__func_array:
            if not isinstance(func, str):
                raise ValueError('__func_array value must str')
            if not hasattr(self, func):
                raise AttributeError('"%s" object has not attribute "%s"' % (self.__class__.__name__, func))
            obj = getattr(self, func)
            if not callable(obj):
                raise RuntimeError(obj, 'is not callable')
            result = obj(params)
            if result:
                return result

    def __call__(self, params: dict):
        '''
        让类函数化,作为实例的唯一对外接口
        :param params: 一个字典
        :return: 返回rum方法的运行结果
        '''
        if not isinstance(self.__run, str):
            raise TypeError('paser_obj must str')
        if not hasattr(self, self.__run):
            raise AttributeError('"%s" object has not attribute "%s"' % (self.__class__.__name__, self.__run))
        obj = getattr(self, self.__run)
        if not callable(obj):
            raise RuntimeError(obj, 'is not callable')
        return obj(params)
