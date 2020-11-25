from setuptools import setup
from setuptools import find_packages

long_description = '''
Ophelia description
'''

requirements = [
    "pyspark==3.0.0",
    "numpy==1.19.1"
]

requirements_test = [
    'pytest',
    'pytest-pep8',
    'pytest-xdist',
    'flaky',
    'pytest-cov'
]

setup(name='Ophelia',
      version='0.0.1',
      description='Artificial assistant for machine learning applications in Spark',
      long_description=long_description,
      author='Luis Vargas',
      author_email='luis.vargasfavero@gmail.com',
      url='https://github.com/Vendetta-Gentleman-Club/ophelia',
      download_url='https://github.com/Vendetta-Gentleman-Club/ophelia/tarball/0.0.1',
      license='Free for non-commercial use',
      install_requires=requirements,
      extras_require={
          'tests': requirements_test,
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
