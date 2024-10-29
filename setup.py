from setuptools import setup

with open("README.md", 'r') as f:
    long_description = f.read()

def parse_requirements(filename):
    with open(filename, 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name='argo2parquet',
    version='0.1',
    description='Module to convert Argo netCDF database to parquet',
    license="GNU GPLv3",
    long_description=long_description,
    author='Enrico Milanese',
    author_email='enrico.milanese@whoi.edu',
    packages=['argo2parquet'],
    install_requires=parse_requirements('requirements.txt'),
    entry_points={
        'console_scripts': [
            'argo2parquet = argo2parquet.main:main',
        ],
    },
)
