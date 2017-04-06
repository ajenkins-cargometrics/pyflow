import os

from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as f:
    long_description = f.read()

setup(
    name='pyflow-swf',
    version='1.0',
    author='Adam Jenkins',
    author_email='adam@thejenkins.org',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 2.7',
    ],
    packages=['pyflow'],
    url='https://github.com/ajenkins-cargometrics/pyflow',
    description='Python Workflow Library built on the AWS SWF service',
    long_description=long_description,
    install_requires=[
        'boto3',
        'attrs'
    ],
    setup_requires=['pytest-runner'],
    tests_require=[
        'pytest>=3.0',
    ]
)
