from setuptools import setup

setup(
    name='pyflow-swf',
    version='2.0',
    author='Cargometrics',
    author_email='ajenkins@cargometrics.com',
    packages=['pyflow'],
    url='https://github.com/ajenkins-cargometrics/pyflow',
    description='Python Workflow Library built on the AWS SWF service',
    install_requires=[
        'boto3',
        'attrs'
    ],
    setup_requires=['pytest-runner'],
    tests_require=[
        'pytest>=3.0',
    ]
)
