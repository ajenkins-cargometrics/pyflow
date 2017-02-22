from setuptools import setup

setup(
    name='pyflow',
    version='1.0',
    author='Cargometrics',
    author_email='ajenkins@cargometrics.com',
    packages=['pyflow'],
    url='http://www.cargometrics.com',
    description='Python Workflow Library',
    install_requires=[
        'boto3'
    ],
    test_suite='nose.collector',
    tests_require=[
        'nose>=1.3.4',
    ]
)
