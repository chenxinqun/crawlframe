#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : cmd.py
@Author: ChenXinqun
@Date  : 2019/1/22 17:48
'''
import sys
import getopt

from crawlframe.commands import stop
from crawlframe.commands import start


def print_info():
    print(
        'start <project or app:app_name>\n'
        'stop <project or pid or app:app_name>\n'
        '-s <project or app:app_name> --start=<project or app:app_name>\n'
        '-p <project or pid or app:app_name> --stop=<pid or all>\n'
    )


def mian(args=None):
    if args is None:
        args = sys.argv[1:]
    if len(args) < 2:
        sys.exit(print_info())
    try:
        opts, args = getopt.getopt(args, 's:p:', ['start=', 'stop='])
    except getopt.GetoptError:
        sys.exit(print_info())
    if len(args) > 1:
        if args[0] == 'start':
            return start(args[1:])
        elif args[0] == 'stop':
            return stop(args[1:])
    for opt, arg in opts:
        if opt in ('-s', '--start'):
            return start(arg)
        elif opt in ('-p', '--stop'):
            return stop(arg)


if __name__ == '__main__':
    mian()