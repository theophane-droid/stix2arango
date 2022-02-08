from distutils.core import setup

setup(
    name='stix2arango',
    version='1.0.1',
    author='theophanedroid',
    description='stix2arango is a python package to convert stix2 to arangoDB',
    packages=['stix2arango',],
    license='MIT license',
    long_description=open('README.md').read(),
    install_requires=[
        "stix2 >= 3.0.1",
        "pyArango >= 1.3.5",
        "flask >= 2.0.2",
        "pyopenssl >= 22.0.0",
        "flask_login >= 0.5.0"
    ]
)