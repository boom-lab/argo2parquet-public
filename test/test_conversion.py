#!/usr/bin/env python3

## @file test_conversion.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Thu 24 Oct 2024

##########################################################################
import argo2parquet.argo_tools as at
import dask
from dask.distributed import Client
from dask.distributed import performance_report
from distributed.diagnostics import MemorySampler
from argo2parquet.daskTools import daskTools
import time
from pathlib import Path
import glob
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import xarray as xr
##########################################################################

def test_bgc_data_mode():

    out_dir = "./data/parquet/bgc_single_Sprof_file/"
    schema_fname = "../schemas/ArgoBGC_schema.metadata"
    chunk_size = 1
    ref_dir = "./data/ref/single_Sprof_file/"
    flist = glob.glob(ref_dir + "*")

    client = Client(
        n_workers=1,
        threads_per_worker=1,
        processes=True,
        memory_limit="10GB",
    )

    ms = MemorySampler()

    with ms.sample("single_prof_file"):
        with performance_report(filename="test_bgc_data_mode.html"):

            # converting to parquet
            daskConverter = daskTools(
                db_type = "BGC",
                out_dir = out_dir,
                flist = flist,
                schema_path = schema_fname,
                chunk = chunk_size,
            )
            daskConverter.convert_to_parquet()

    ms.plot()
    client.shutdown()

    #loading original netCDF file
    argo_file = flist[0]
    ds = xr.open_dataset(argo_file, engine="argo")

    #loading created parquet file
    schema = pq.read_schema(schema_fname)
    parquet_ds = pq.ParquetDataset(out_dir, schema=schema)
    parquet_df = parquet_ds.read().to_pandas()

    for j in range(ds.sizes["N_PROF"]):
        parquet_prof_rows = parquet_df[ parquet_df["N_PROF"]==j ]

        for p in range(ds.sizes["N_PARAM"]):
            param_name = ds["PARAMETER"].isel(N_CALIB=0,N_PROF=j,N_PARAM=p).values

            param_name = str(param_name).strip()
            parquet_dm_name = param_name + "_DATA_MODE"

            # check that all entries of <PARAM>_DATA_MODE for a given profile N_PROF match the netCDF file entries
            all_dm_ok = ( parquet_prof_rows[parquet_dm_name] == ds["PARAMETER_DATA_MODE"].isel(N_PROF=j,N_PARAM=p) ).all()

            if not all_dm_ok:
                print("Test failed.")
                break

        if not all_dm_ok:
            print("Test failed.")
            break

    if all_dm_ok:
        print("Test successful.")

    return
