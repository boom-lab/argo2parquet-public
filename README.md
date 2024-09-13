## argo2parquet

argo2parquet bulk-convert Argo data from the Global Data Assembly Centers (GDAC) from their original [NetCDF](https://www.unidata.ucar.edu/software/netcdf/) format to the [parquet](https://parquet.apache.org/) format.

It converts profile files only, i.e. `<PLATFORM_NUMBER>_prof.nc` for Argo Core and `<PLATFORM_NUMBER>_Sprof.nc` for Argo BGC. Metadata for these files are stored

NB: the development is ongoing and in its infant stage, more complete documentation will be added as developing and testing proceed.

### Table of Contents
1. [Installation and usage](#installation)
2. [Reading parquet database](#reading-parquet-database)
3. [Contact](#contact)

### Installation and usage
From the root directory, install it locally as:

``` sh
pip install .
```

And to execute it: 
`argo2parquet [-h] [-d DOWNLOAD] [-c CONVERT] [--gdac_index GDAC_INDEX] [--db_nc DB_NC] [--db_parquet DB_PARQUET] [--db DB]` 

The code is meant to be used on an HPC machine, both for storage and for perfomance. It uses `dask`, which allows for parallelized lazy operations and larger-than-memory data management. It is currently set to use up to 10 workers and up to 30 threads.

Note that it will download a mirror of all the profile files to your machine before converting them. Arguments can be specified (see `main.py`) to only download or only convert the datsets, and to select the Core (`phy`) or BGC (`bgc`) dataset only. When downloading the datasets, if previous files are already present, it does not download them unless a newer version is present in the GDAC. When converting the datasets, the each dataset is always converted whole, i.e. even if any parquet version has already been created, argo2parquet will not update that but create a new one. This allows for better compression, indexing, optimized file size for later reading and delayed operations.

The original datasets are downloaded to the folder `data/GDAC/dac/` using the same path structure as in the GDAC. The parquet datasets are stored to `data/parquet/`. All folders are generated automatically if not already present.

During the tests that I have run, it took approximately these times:
* Downloading Argo Core profile files: 2.5 hours
* Downloading Argo BGC profile files: 30 mins
* Converting Argo Core to parquet: 22 mins
* Converting Argo BGC to parquet: 25 mins

The resulting databases are 13 GB (Core) and 7.2 GB (BGC) large.


#### HPC parameters

At the moment, `argo_convert.py` contains hardcoded values for the number of workers and threads that Dask uses when converting the database. The values are `nw=18` and `nw=9` respectively for the Core and BGC databases. I found that fewer workers are better for the BGC database, as it requires more memory during conversion. I also normally reserve 100GB of memory in the HPC cluster, thus I impose a memory limit of 5.5GB and 11GB per worker for the two cases, as it seems that the 'auto' setting from Dask sets lower memory limits than available (and the conversion crashes).


### Reading parquet database

The `notebooks` folder contains example of how to read the parquet databases.
The notebooks also report reading times, which might not match what you get as they depend on the machine that the runs the notebooks, of course. Most of the times reported were obtained on WHOI's HPC cluster.

### Contact

For any questions or bugs, you can open an issue on github or reach out to me at [enrico.milanese@whoi.edu](mailto:enrico.milanese@whoi.edu).
