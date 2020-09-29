#!/usr/bin/env python

from setuptools import setup

setup(
    name="timestampmath",
    version="0.0.1",
    description="Compare timestamp columns to find difference, minimum or maximum.",
    author="Adam Hooper",
    author_email="adam@adamhooper.com",
    url="https://github.com/CJWorkbench/timestampmath",
    packages=[""],
    py_modules=["loadurl"],
    install_requires=["numpy~=1.19", "cjwmodule~=2.0"],
    extras_require={"tests": ["pytest~=5.4", "pandas~=1.1"]},
)
