#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : parcel.py
@Author: ChenXinqun
@Date  : 2019/2/13 11:32
'''


# 一个打包数据的基类, 要与解析基类配合使用
class BaseParcel:
    def init_config(self, config: dict):
        '''
        初始化实例属性, 每个子类的__init__方法, 必须调用此方法.
        :param config: 一个字典, 键是解析数据所需字段, 值是一个callable对象, 即解析这个字段的对象.
        :return: None
        '''
        conf = config  # : Dict(str, callable)
        self.__dict__ = conf

    def __call__(self, params: dict):
        '''自动调用可调用的实例属性'''
        for item in self.__dict__:
            if callable(self.__dict__[item]):
                self.__dict__[item] = self.__dict__[item](params)


class Parcel(BaseParcel):

    @staticmethod
    def filrt_call_conf(config, modules):
        '''
        加工配置字典, 将配置字典中键所对应的字符串值, 加工成一个可调用对象值.
        :param config: 配置字典, 键是数据库所需的字段, 值是 解析这个字段的类或者方法的名字.
        :param modules: 一个python模块对象.
        :return: 返回加工后的配置字典, 只包含可调用对象的键值对. 如果某个键值对的可调用对象不存在, 则会被过滤掉.
        '''
        for item in config:
            config[item] = Parcel._filrt_callble(modules, config[item])
        result = {}
        for item in config:
            if callable(config[item]):
                result[item] = config[item]
        return result

    @staticmethod
    def _filrt_callble(modules, call_name):
        '''
        :param modules: 一个python模块对象
        :param call_name: 一个string, 是想要导入的模块成员名.
        :return: 返回模块中的可调用对象, 或者None
        '''
        if isinstance(call_name, str):
            if hasattr(modules, call_name):
                obj = getattr(modules, call_name)
                if type(obj) is type(object):
                    instance = obj()
                    if callable(instance):
                        return instance
                elif callable(obj):
                    return obj


class ParcelItems(Parcel):
    def __init__(self, conf: dict, mod: object):
        '''
        :param config: 一个字典, 键是数据库所需字段的键, 值是解析这个字段的类(必须是可调用类), 或者函数的名字, string型.
        :param module: 一个python模块对象.
        '''
        self.mod = mod
        # 加工配置字典
        self.conf = self.filrt_call_conf(conf, self.mod)
        # 初始化配置
        self.init_config(self.conf)
