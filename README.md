## argo2parquet

argo2parquet bulk-convert Argo data from the Global Data Assembly Centers (GDAC) from their original [NetCDF](https://www.unidata.ucar.edu/software/netcdf/) format to the [parquet](https://parquet.apache.org/) format.

It converts profile files only, i.e. `<PLATFORM_NUMBER>_prof.nc` for Argo Core and `<PLATFORM_NUMBER>_Sprof.nc` for Argo BGC. Metadata for these files are stored

NB: the development is ongoing and in its infant stage, more complete documentation will be added as developing and testing proceed.

### Table of Contents
1. [Installation and usage](#installation)
2. [Reading parquet database](#reading-parquet-database)
3. [Contact](#contact)

### Installation and usage
From the root directory, execute:

``` sh
pip install .
```

You can then call `main.py`. 

The code is meant to be used on an HPC machine, both for storage and for perfomance. It uses `dask`, which allows for parallelized lazy operations and larger-than-memory data management. It is currently set to use up to 10 workers and up to 30 threads.

Note that it will download a mirror of all the profile files to your machine before converting them. Arguments can be specified (see `main.py`) to only download or only convert the datsets, and to select the Core (`phy`) or BGC (`bgc`) dataset only. When downloading the datasets, if previous files are already present, it does not download them unless a newer version is present in the GDAC. When converting the datasets, the each dataset is always converted whole, i.e. even if any parquet version has already been created, argo2parquet will not update that but create a new one. This allows for better compression, indexing, optimized file size for later reading and delayed operations.

The original datasets are downloaded to the folder `data/GDAC/dac/` using the same path structure as in the GDAC. The parquet datasets are stored to `data/parquet/`. All folders are generated automatically if not already present. 

### Reading parquet database

The `notebooks` folder contains example of how to read the parquet databases.

### Contact

For any questions or bugs, you can open an issue on github or reach out to me at [enrico.milanese@whoi.edu](mailto:enrico.milanese@whoi.edu).
