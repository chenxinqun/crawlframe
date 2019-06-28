#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
@File  : setup.py
@Author: ChenXinqun
@Date  : 2019/5/25 17:21
'''
import sys
import pathlib
from setuptools import (
    setup,
    find_packages
)

if sys.version_info < (3, 5, 3):
    raise RuntimeError("crawlframe 1.x requires Python 3.5.3+")

here = pathlib.Path(__file__).parent


def read(f):
    return (here / f).read_text('utf-8').strip()


about = {}

version = (here / 'crawlframe' / '__version__.py').read_text('utf-8')

exec(version, about)

install_requires = [
    'bs4',
    'pytz',
    'lxml',
    'redis',
    'psutil',
    'psycopg2',
    'requests',
]

tests_require = [
    'pytest-httpbin==0.0.7',
    'pytest-cov',
    'pytest-mock',
    'pytest-xdist',
    'PySocks>=1.5.6, !=1.5.7',
    'pytest>=2.8.0'
]

args = dict(
    long_description='\n\n'.join((read('README.md'), read('CHANGES.md'))),
    long_description_content_type='text/markdown',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Development Status :: 5 - Production/Stable',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Framework :: AsyncIO',
    ],
    name=about['__title__'],
    version=about['__version__'],
    description=about['__description__'],
    author=about['__author__'],
    author_email=about['__author_email__'],
    url=about['__url__'],
    maintainer=about['__maintainer__'],
    maintainer_email=about['__maintainer_email__'],
    license=about['__license__'],
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    python_requires='>=3.5.3',
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'crawlf = crawlframe.cmd:mian',
            'crawlframe = crawlframe.cmd:mian'
        ]
    },
    tests_require=tests_require,
    include_package_data=True,
)

setup(**args)
