#!/usr/bin/env python
from setuptools import setup

#https://dzone.com/articles/executable-package-pip-install

setup(
    name="tap-brightpearl",
    version="0.1.0",
    description="Singer.io tap for extracting Brightpearl data",
    author="Adilson",
    url="http://github.com/Zookal/tap-brightpearl",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_brightpearl"],
    install_requires=[
        "singer-python==5.4.1",
        "requests==2.24.0",
    ],
    extras_require={
        'dev': [
            'pylint',
            'ipdb',
            'requests==2.24.0',
            'nose',
        ]
    },
    entry_points="""
    [console_scripts]
    tap-brightpearl=tap_brightpearl:main
    """,
    packages=["tap_brightpearl"],
    package_data = {
        "tap_brightpearl": ["schemas/*.json"]
    },
    include_package_data=True,
)
