from setuptools import setup, find_packages

from spark_jdbc_handler import __version__

VERSION = __version__

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='spark-jdbc-handler',
    version=VERSION,
    packages=find_packages(),
    url='https://github.com/pandologic-algo/spark-jdbc-handler',
    download_url='https://github.com/pandologic-algo/spark-jdbc-handler/archive/v{}.tar.gz'.format(VERSION),
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
                        'pyspark==2.4.4',
                        'python-dotenv==0.14.0'
                    ]
)