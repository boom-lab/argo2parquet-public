## Argo2Parquet

Argo2Parquet is a suite of python scripts and notebooks to bulk-convert Argo data from the Global Data Assembly Centers (GDAC) from their original [NetCDF](https://www.unidata.ucar.edu/software/netcdf/) format to the [parquet](https://parquet.apache.org/) format.

NB: the development is ongoing and in its infant stage, more complete documentation will be added as developing and testing proceed.

### Table of Contents
1. [Requirements](#requirements)
2. [Running the code](#running-the-code)
3. [MATLAB access](#matlab-access)
4. [Log](#log)
5. [TODO](#TODO)

### Requirements
* Python 3.9.10

##### Both cluster/Poseidon and AWS S3 examples
Python packages:
* argopy 0.1.15
* gsw 3.6.18
* numpy 1.26.4
* pandas 2.2.2
* pyarrow 16.1.0
* xarray 2024.5.0
* cartopy 0.23.0
* matplotlib 3.9.0

##### For cluster/Poseidon examples
Examples with 'Poseidon' or 'cluster' in their name store or access data in WHOI's server. You should be able to easily adapt these examples for your own server or local machine by changing the paths accordingly, as the code itself does not require any credentials (the notebooks are meant to run directly on the server).

If you want to exectue them on Poseidon, you will need access to both WHOI's network (i.e. VPN or on-site Eduroam) and Boom's lab shared storage on the server.

##### For AWS examples
You will need to install also `boto3 (1.34.140)`. At this time the bucket with the data for the examples is public, so there is no need to set up your AWS S3 credentials. This might change in the future.

### Running the code
The `notebooks` folder contains a series of notebooks. `to_parquet` notebooks show how to convert the Argo profiles to parquet. The Examples 1 to 4 show how to access the data. 

### MATLAB access
The folder `matlab` contains an example to access the Argo Core and BGC parquet databases with MATLAB. 

The most recent version of MATLAB is recommended, as the tools to access parquet databases are fairly recent and keeps being updated. The code has been tested on MATLAB R2024a. Versions R2022a and more recent might be compatible, while older ones should not.

Note that the databases needs to be provided separately at the moment. [Reach out](enrico.milanese@whoi.edu) for a link to download them.

### Log
* 2024.08.21: dask conversion dedicated notebook and tools, moved notebooks to dedicated folder
* 2024.08.08: speed tests for different partitioning
* 2024.08.05: added filtering tools for MATLAB
* 2024.08.03: conversion and read now doable with Dask; speed tests added
* 2024.07.30: coiled-AWS example added for self-managing aws access
* 2024.07.23: added Argo-phy download, time checking through index files
* 2024.07.08: added access to AWS S3, examples 1, 2, 3
* 2024.07.03: metadata are converted to parquet, argo files can be downloaded in parallel, various tests
* 2024.06.12: Argo2Parquet moved to its own repository

### TODO
This is a list of features that we think would be useful and we might implement some day (in no particular order):

* ~add parquet schemas to this repo~
* switch all paths to `pathlib`
* add print to external log file for errors during download from Argo servers
* add print to external log file for errors during conversion
* return separate lists of downloaded files and failed downloads
* update download to `imap_unordered`
* ~incorporate creation of metadata file during conversion of profiles~
* ~parallelize files download in argo_tools~
* update conversion to use shallow copy when concataneting pandas dataframes
