from setuptools import setup

from jdbc_to_s3 import __version__

VERSION = __version__

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='jdbc-to-s3',
    version=VERSION,
    packages=['jdbc_to_s3'],
    url='https://github.com/pandologic-algo/jdbc-to-s3',
    download_url='https://github.com/pandologic-algo/jdbc-to-s3/archive/v{}.tar.gz'.format(VERSION),
    license='MIT License',
    author='Pandologic',
    author_email='ebrill@pandologic.com',
    description='pyspark simple API for JDBC reading table and saving in S3 location',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ],
    python_requires='>=3.6',
    install_requires=[
                        'pyspark==2.4.4'
                    ]
)