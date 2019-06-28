#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : cache.py
@Author: ChenXinqun
@Date  : 2019/1/19 12:02
'''
from redis import StrictRedis as Redis


class BaseRedis:
    def __init__(self, conf: dict, cluster=False):
        '''
        :param conf: 连接配置.
        普通版:
        conf = dict(
            host='127.0.0.1',  # 必须参数
            port=6379,  # 必须参数
            db=6,  # 必须参数
            # max_connections=10,
            # decode_responses=True,
        )
        集群版:
        conf = dict(
            startup_nodes=[
                dict(
                    host='127.0.0.1',  # 必须参数
                    port=6379,  # 必须参数
                ),
            ],
            # decode_responses=True,  # 非必须参数
        )
        :param cluster: 要连接的是不是redis集群
        '''
        self.redis = Redis(**conf)

    # 删除键
    def delete_name(self, name):
        return self.redis.delete(name)

    # 键是否存在
    def exists_name(self, name):
        return self.redis.exists(name)

    # 是否需要返回bytes
    def need_tytes(self, value, need_bytes, encoding='utf-8') -> str or bytes or None:
        if isinstance(value, bytes):
            if need_bytes:
                result = value
            else:
                result = value.decode(encoding=encoding)
        else:
            if need_bytes and isinstance(value, str):
                result = value.encode(encoding=encoding)
            else:
                result = value
        return result

    # 获取string
    def str_get(self, key, need_bytes=False, encoding='utf-8') -> str or bytes or None:
        value = self.redis.get(key)
        return self.need_tytes(value, need_bytes, encoding)

    # 设置string
    def str_set(self, key, value, overtime=60):
        return self.redis.set(key, value, overtime)

    # 不存在则设置
    def setnx(self, key, value, overtime=60):
        if self.redis.setnx(key, value):
            return self.expire(key, overtime)
        else:
            return False

    # 更新过期时间
    def expire(self, name, overtime=60):
        return self.redis.expire(name, overtime)

    # 获取hash单个键的值
    def hash_get(self, name, key, need_bytes=False, encoding='utf-8') -> str or bytes or None:
        value = self.redis.hget(name, key)
        return self.need_tytes(value, need_bytes, encoding)

    # 获取hash所有键值
    def hash_getall(self, name, need_bytes=False, encoding='utf-8') -> dict:
        value = self.redis.hgetall(name)
        result_dict = {}
        if isinstance(value, dict):
            for k, v in value.items():
                k = self.need_tytes(k, False)
                v = self.need_tytes(v, need_bytes, encoding)
                result_dict[k] = v
        return result_dict

    # 获取hash指定键的值
    def hash_hmget(self, name, keys: list, need_bytes=False, encoding='utf-8') -> list:
        value = self.redis.hmget(name, keys)
        result = []
        for va in value:
            val = self.need_tytes(va, need_bytes, encoding)
            result.append(val)
        return result

    # 设置hash单个键值
    def hash_set(self, name, key, value):
        return self.redis.hset(name, key, value)

    # 设置hash多个键值
    def hash_hmset(self, name, mapping: dict):
        return self.redis.hmset(name, mapping)

    # 删除hash单个键值
    def hash_del(self, name, key):
        return self.redis.hdel(name, key)

    # 弹出集合中的值
    def sets_pop(self, name, need_bytes=False, encoding='utf-8') -> str or bytes or None :
        value = self.redis.spop(name)
        return self.need_tytes(value, need_bytes, encoding)

    # 增加到集合
    def sets_add(self, name, value):
        return self.redis.sadd(name, value)

    # 是否存在于集合
    def sets_exists(self, name, value) -> bool:
        return self.redis.sismember(name, value)

    # 从集合中删除
    def sets_del(self, name, value):
        return self.redis.srem(name, value)

    # 集合长度
    def sets_len(self, name) -> int:
        return self.redis.scard(name)

    # 加入list头部(与rpop配合, 实现先进先出队列)
    def list_lpush(self, name, value):
        return self.redis.lpush(name, value)

    # 加入list底部(基于lpush与rpop配合实现先进先出队列, 此方法用于先进先出队列的插队到最前面)
    def list_rpush(self, name, value):
        return self.redis.rpush(name, value)

    # 插队到指定的下标(会替换原值)
    def list_lset(self, name, value, index):
        return self.redis.lset(name, index, value)

    # 从list底部弹出(与lpush配合, 实现先进先出队列)
    def list_rpop(self, name, need_bytes=False, encoding='utf-8') -> str or bytes or None:
        value = self.redis.rpop(name)
        return self.need_tytes(value, need_bytes, encoding)

    # list长度
    def list_len(self, name) -> int:
        return self.redis.llen(name)