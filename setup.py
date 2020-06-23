#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    setup.py
    ~~~~~~~~

    An airflow job operator that executes a task as a Kubernetes job on a cluster,
given a job yaml configuration or an image uri.

    :copyright: (c) 2020 by zav.
    :license: see LICENSE for more details.
"""

import codecs
import os
import re
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

setup(
    name="zthreading",
    version="0.1.0",
    description="A collection of wrapper classes for event broadcast and task management for python (Python Threads or Asyncio).",
    long_description="Please see readme.md",
    classifiers=[],
    author="Zav Shotan",
    author_email="",
    url="https://github.com/LamaAni/zthreading.py",
    packages=["zthreading"],
    platforms="any",
    license="LICENSE",
    install_requires=[],
    python_requires=">=3.6",
    include_package_data=True,
)
