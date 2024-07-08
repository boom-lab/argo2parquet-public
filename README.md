## Argo2Parquet

Argo2Parquet is a python script to bulk convert Argo data from the Global Data Assembly Centers (GDAC) from their original [NetCDF](https://www.unidata.ucar.edu/software/netcdf/) format to the [parquet](https://parquet.apache.org/) format.

NB: the development is ongoing and in its infant stage, more complete documentation will be added as developing and testing proceed.

### Table of Contents
1. [Requirements](#requirements)
2. [Running the code](#running-the-code)
3. [Log](#log)
4. [TODO](#TODO)

### Requirements
* Python 3.9.10

Python packages:
* argopy 0.1.15
* gsw 3.6.18
* numpy 1.26.4
* pandas 2.2.2
* pyarrow 16.1.0
* xarray 2024.5.0
* cartopy 0.23.0
* matplotlib 3.9.0

For AWS examples:
* boto3 1.34.140

### Running the code
Go through the Examples 1, 2, and 3.

### Log
* 2024.07.08: added access to AWS S3, examples 1, 2, 3
* 2024.07.03: metadata are converted to parquet, argo files can be downloaded in parallel, various tests
* 2024.06.12: Argo2Parquet moved to its own repository

### TODO
This is a list of features that we think would be useful and we might implement some day (in no particular order):
* add print to external log file for errors during download from Argo servers
* add print to external log file for errors during conversion
* return separate lists of downloaded files and failed downloads
* update download to `imap_unordered`
* ~incorporate creation of metadata file during conversion of profiles~
* ~parallelize files download in argo_tools~
* update conversion to use shallow copy when concataneting pandas dataframes
