from setuptools import setup

with open("README.md", 'r') as f:
    long_description = f.read()

setup(
   name='nc2parquet',
   version='0.1',
   description='Module to convert Argo netCDF database to parquet',
   license="GNU GPLv3",
   long_description=long_description,
   author='Enrico Milanese',
   author_email='enrico.milanese@whoi.edu'
)
