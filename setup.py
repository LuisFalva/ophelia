#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup
import os
import pip

__version__ = "0.1.0"
pip_major = int(pip.__version__.split(".")[0])

if pip_major < 10:
    from pip.download import PipSession
    from pip.req import parse_requirements
elif pip_major < 20:
    from pip._internal.download import PipSession
    from pip._internal.req import parse_requirements
else:
    from pip._internal.network.session import PipSession
    from pip._internal.req.req_file import parse_requirements


if __name__ == "__main__":
    here = os.path.abspath(os.path.dirname(__file__))

    with open(os.path.join(here, 'README.md')) as f:
        readme = f.read()

    def parser_req(requirements_file):
        session = PipSession()
        parse_req = (lambda file: list(parse_requirements(file, session=session)))
        return [str(ir.requirement) for ir in parse_req(requirements_file)]

    requirements = parser_req('requirements.txt')
    test_requirements = parser_req('requirements_test.txt')
    dev_requirements = parser_req('requirements_dev.txt')

    description = '''
    Ophelia is an spark miner AI engine that builds data mining & ml pipelines with PySpark.
    '''

    setup(name='ophelia',
          version=__version__,
          description=description,
          long_description=readme,
          long_description_content_type='text/markdown',
          author='Luis Vargas',
          author_email='falvaluis@gmail.com',
          url='https://https://github.com/LuisFalva/ophelia',
          license='Free for non-commercial use',
          keywords='ophelia',
          packages=['ophelia'],
          package_dir={'ophelia': 'ophelia'},
          include_package_data=True,
          python_requires='>=3.6',
          install_requires=requirements,
          extras_require={
              'tests': test_requirements,
              'dev': dev_requirements,
          },
          classifiers=[
              'Development Status :: 1 - Planning',
              'Intended Audience :: Developers',
              'Intended Audience :: Science/Research',
              'Intended Audience :: Financial and Insurance Industry',
              'License :: Free for non-commercial use',
              'Programming Language :: Python :: 3',
              'Programming Language :: Python :: 3.7',
              'Topic :: Software Development :: Libraries',
              'Topic :: Software Development :: Libraries :: Python Modules',
              'Topic :: Office/Business :: Financial :: Investment',
              'Topic :: Scientific/Engineering :: Artificial Intelligence',
              'Topic :: Scientific/Engineering :: Information Analysis'
          ])
