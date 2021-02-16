# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError, DistutilsExecError, DistutilsPlatformError

import setuptools
import sys


cext = setuptools.Extension(
    "pyignite._cutils",
    sources=[
        "./cext/cutils.c"
    ],
    include_dirs=["./cext"]
)

if sys.platform == 'win32':
    ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError, IOError, ValueError)
else:
    ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)


class BuildFailed(Exception):
    pass


class ve_build_ext(build_ext):
    # This class allows C extension building to fail.

    def run(self):
        try:
            build_ext.run(self)
        except DistutilsPlatformError:
            raise BuildFailed()

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except ext_errors:
            raise BuildFailed()


def run_setup(with_binary=True):
    if with_binary:
        kw = dict(
            ext_modules=[cext],
            cmdclass=dict(build_ext=ve_build_ext),
        )
    else:
        kw = dict()

    setuptools.setup(
        name='pyignite',
        version='0.4.0',
        python_requires='>=3.6',
        author='The Apache Software Foundation',
        author_email='dev@ignite.apache.org',
        description='Apache Ignite binary client Python API',
        url='https://github.com/apache/ignite-python-thin-client',
        packages=setuptools.find_packages(),
        install_requires=[
            "attrs==18.1.0"
        ],
        tests_require=[
            'pytest==3.6.1',
            'pytest-cov==2.5.1',
            'teamcity-messages==1.21',
            'psutil==5.6.5',
            'jinja2==2.11.3'
        ],
        setup_requires=[
            'pytest-runner==4.2'
        ],
        extras_require={
            'docs': [
                'wheel==0.36.2',
                'Sphinx==1.7.5',
                'sphinxcontrib-fulltoc==1.2.0'
            ],
        },
        classifiers=[
            'Programming Language :: Python',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3 :: Only',
            'Intended Audience :: Developers',
            'Topic :: Database :: Front-Ends',
            'Topic :: Software Development :: Libraries :: Python Modules',
            'License :: OSI Approved :: Apache Software License',
            'Operating System :: OS Independent',
        ],
        **kw
    )


try:
    run_setup()
except BuildFailed:
    BUILD_EXT_WARNING = ("WARNING: The C extension could not be compiled, "
                         "speedups are not enabled.")
    print('*' * 75)
    print(BUILD_EXT_WARNING)
    print("Failure information, if any, is above.")
    print("I'm retrying the build without the C extension now.")
    print('*' * 75)

    run_setup(False)

    print('*' * 75)
    print(BUILD_EXT_WARNING)
    print("Plain python installation succeeded.")
    print('*' * 75)
