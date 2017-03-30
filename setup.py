from setuptools import setup

setup(
    name='pyflow',
    version='2.0',
    author='Cargometrics',
    author_email='ajenkins@cargometrics.com',
    packages=['pyflow'],
    url='http://www.cargometrics.com',
    description='Python Workflow Library',
    install_requires=[
        'boto3',
        'attrs'
    ],
    setup_requires=['pytest-runner'],
    tests_require=[
        'pytest',
    ]
)
