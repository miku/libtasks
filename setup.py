#!/usr/bin/env python

""" 
libtasks implements a few luigi tasks in the library context.

Luigi: https://github.com/spotify/luigi

Documentation and examples can be found under: https://github.com/miku/libtasks
"""

from setuptools import setup

classifiers = """
Development Status :: 3 - Alpha
Intended Audience :: Education
Intended Audience :: Developers
Intended Audience :: Information Technology
License :: OSI Approved :: MIT License
Programming Language :: Python
Topic :: Text Processing :: General
"""

setup(name='libtasks',
      version='0.0.1.4',
      description='Reusable tasks and task templates for a library context.',
      long_description=__doc__,
      classifiers=filter(None, classifiers.split('\n')),
      author='Martin Czygan',
      author_email='martin.czygan@gmail.com',
      url='https://github.com/miku/libtasks',
      packages=['libtasks'],
      # run pip install numpy beforehand
      install_requires=['marcx>=0.1.7', 'luigi>=1.0.3', 'pymarc>=2.8', 
                        'pandas>=0.12.0', 'requests>=1.2.3'])
