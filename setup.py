#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer


requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-spark',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Apache Spark plugin for Intake',
    url='https://github.com/ContinuumIO/intake-spark',
    maintainer='Martin Durant',
    maintainer_email='mdurant@anaconda.com',
    license='BSD',
    py_modules=['intake_spark'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.yaml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
