# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('readme.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='sqlutils',
    version='0.0.2',
    description='Simple pyodbc helper methods',
    long_description=readme,
    author='Jeremy Axmacher',
    author_email='jeremy@obsoleter.com',
    url='https://github.com/obsoleter/sqlutils',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    py_modules=['sqlutils'],
)
