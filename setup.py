#! /usr/bin/env python

from setuptools import setup

setup(
    name                 = 'py_threader',
    version              = '0.0.1',
    description          = 'A simple task threader',
    url                  = 'https://github.com/Contatta/py-threader',
    author               = 'Contatta',
    packages             = ['py_threader'],
    include_package_data = True
)

#python setup.py install
#python setup.py develop (to symlink to source)