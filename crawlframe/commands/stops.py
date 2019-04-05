#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : stop.py
@Author: XinQun
@Date  : 2019/1/19 12:01
@Company: 宏数科技
'''

'''
停止爬虫程序用
所有爬虫程序启动之后, 线程(协程)会释放一个工作状态的信号.标识是工作中还是空闲中.
所有的协程, 都会检测一个stop的信号参数, stop为true的时候, 就不会再继续爬取任务.
基于以上机制, 程序实现以下功能:
接收参数(pid) 如不传参, 默认为all, 从程序运行的pid文件列表中读取所有pid.
默认是温和停止.即等待所有任务完成后才停止
加一个参数, -9 可以强制kill程序
'''
import os
import sys
import pickle
import psutil
from time import sleep
from crawlframe import configs
from crawlframe.configs import _CHDIR_ENV
from crawlframe.engine.crawl import input_checkout


_sleep_tm = 1


def stop(args=None):
    if args is None:
        args = sys.argv[1:]
    input_checkout(args[0], input_type='stop')
    # 获取配置文件
    settings = configs.get_settings()
    # 获取工作路径
    chdir = os.getenv(_CHDIR_ENV) or settings.SPIDER_CHDIR or os.getcwd()
    # 获取缓存路径
    cache_dir = settings.CRAWLFRAME_CACHE_DIR or os.path.join(chdir, 'cache')
    # 缓存文件容器
    cache_files = {}
    if os.path.isdir(cache_dir):
        files = os.listdir(cache_dir)
        # 获取缓存文件
        for file in files:
            file_absdir = os.path.join(cache_dir, file)
            if '_data' in file:
                cache_files.setdefault('data', [])
                cache_files['data'].append(file_absdir)
            elif '_stop' in file:
                cache_files.setdefault('stop', [])
                cache_files['stop'].append(file_absdir)
    # 如果是pid
    if args[0].isdigit() and isinstance(cache_files.get('stop'), list):
        for file in cache_files.get('stop'):
            if file.startswith(str(args[0])):
                with open(file, 'w', encoding='utf-8') as f:
                    f.write('true')
                    f.flush()
    project_pid_list = []
    if isinstance(cache_files.get('data'), list):
        for file in cache_files.get('data'):
            with open(file, 'rb') as f:
                project_pid_list.append(pickle.load(f, encoding='utf-8'))

    pid_stop_dict = {}
    app_stop_dict = {}
    for pid_dict in project_pid_list:
        if isinstance(pid_dict, dict):
            for app, pid in pid_dict.items():
                pid_stop_dict[str(pid)] = False
                app_stop_dict[app] = pid
                if args[0] == app or args[0] == 'project':
                    with open(os.path.join(cache_dir, '%s_stop' % pid), 'w', encoding='utf-8') as f:
                        f.write('true')
                        f.flush()
    app_stop = False
    if cache_files.values():
        if args[0].isdigit():
            while not app_stop:
                if not psutil.pid_exists(int(args[0])):
                    print('pid:%s is not exists!' % args[0])
                    break
                app_stop = True
                sleep(_sleep_tm)
            for file in cache_files.get('stop', []):
                if file == os.path.join(cache_dir, '%s_stop' % args[0]):
                    if os.path.exists(file):
                        os.remove(file)
                        print('File "%s" remove!' % file)
        elif args[0] == 'project':
            while not all(pid_stop_dict.values()):
                for pid_dict in project_pid_list:
                    if isinstance(pid_dict, dict):
                        for app, pid in pid_dict.items():
                            if not psutil.pid_exists(pid):
                                pid_stop_dict[str(pid)] = True
                                print('%s pid:%s not found!' % (args[0], pid))
                            else:
                                print(print('%s pid:%s is exists!' % (args[0], pid)))
                        sleep(_sleep_tm / 10)
                sleep(_sleep_tm)

            for file in cache_files.get('stop', []):
                if os.path.exists(file):
                    os.remove(file)
                    print('File "%s" remove!' % file)
            for file in cache_files.get('data', []):
                if os.path.exists(file):
                    os.remove(file)
                    print('File "%s" remove!' % file)
        elif args[0].startswith('app:'):
            while not app_stop:
                for app, pid in app_stop_dict.items():
                    if app == args[0] and not psutil.pid_exists(pid):
                        app_stop = True
                        print('%s pid:%s is not exists!' % (app, pid))
                        break
                    sleep(_sleep_tm / 10)
                sleep(_sleep_tm)

            for file in cache_files.get('stop', []):
                if file == os.path.join(cache_dir, '%s_stop' % app_stop_dict.get(args[0])):
                    if os.path.exists(file):
                        os.remove(file)
                        print('File "%s" remove!' % file)
    else:
        print('Cache File Not Found!')



