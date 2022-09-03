import setuptools
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='nice-spider',
    version='0.0.1',
    author='minusli',
    author_email='minusli@foxmail.com',
    url='',
    description='easy nice spider: download & handle',
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(include=["nicespider"]),
    install_requires=[
        "pytest",
        "requests",
        "nice-sql==0.0.5"
    ],
    entry_points={},
    license="Apache License 2.0"
)
