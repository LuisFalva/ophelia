#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from pip.download import PipSession
    from pip.req import parse_requirements
except ImportError as ie:
    from pip._internal.download import PipSession
    from pip._internal.req import parse_requirements

from setuptools import setup, find_packages
from os import path


if __name__ == "__main__":
    here = path.abspath(path.dirname(__file__))

    with open(path.join(here, 'README.md')) as f:
        readme = f.read()

    requirements = [str(ir.req) for ir in parse_requirements('requirements.txt', session=PipSession())]
    test_requirements = [str(ir.req) for ir in parse_requirements('requirements_test.txt', session=PipSession())]

    description = '''
    Ophelia is an spark miner AI engine that builds data mining & ml pipelines with PySpark.
    '''

    setup(name='Ophelia',
          version='0.1.dev0',
          description=description,
          long_description=readme,
          author='Luis Vargas',
          author_email='falva.luis@gmail.com',
          url='https://github.com/Vendetta-Gentleman-Club/ophelia',
          download_url='https://github.com/Vendetta-Gentleman-Club/ophelia/tarball/0.0.1',
          license='Free for non-commercial use',
          keywords='Ophelia',
          install_requires=requirements,
          extras_require={
              'tests': test_requirements,
          },
          classifiers=[
              'Development Status :: 1 - Planning',
              'Intended Audience :: Developers',
              'Intended Audience :: Science/Research',
              'Intended Audience :: Financial and Insurance Industry',
              'License :: Free for non-commercial use',
              'Programming Language :: Python :: 3',
              'Programming Language :: Python :: 3.6',
              'Topic :: Software Development :: Libraries',
              'Topic :: Software Development :: Libraries :: Python Modules',
              'Topic :: Office/Business :: Financial :: Investment',
              'Topic :: Scientific/Engineering :: Artificial Intelligence',
              'Topic :: Scientific/Engineering :: Information Analysis'
          ],
          packages=find_packages())
