#!/usr/bin/env python3

## @file argo_convert.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 03 Sep 2024

##########################################################################
import argo2parquet.argo_tools as at
import dask
from dask.distributed import Client
from argo2parquet.daskTools import daskTools
import time
##########################################################################

def argo_convert( flists, db_names, outdir_parquet, schema_path):

    flist_phy = flists[0]
    flist_bgc = flists[1]

    for k in range(len(db_names)):

        db_name = db_names[k]
        print("Converting " + db_name + " database...")
        schema_fname = schema_path+"Argo" + db_name.upper() + "_schema.metadata"
        print("Schema file for " + db_name + " database: " + schema_fname)

        if db_name=="phy":
            flist = flist_phy
            nw = 30
            tw = 1
        elif db_name=="bgc":
            flist = flist_bgc
            nw = 10
            tw = 10

        client = Client(
            n_workers=nw,
            threads_per_worker=tw,
            processes=True,
            memory_limit="auto"
        )

        start_time = time.time()
        daskConverter = daskTools(
            db_type = db_name.upper(),
            out_dir = outdir_parquet+db_name,
            flist = flist,
            schema_path = schema_fname,
            chunk = 1000
        )

        daskConverter.convert_to_parquet()

        end_time = time.time()

        print(f"Converted in: {(end_time-start_time):.4f} seconds")

        client.shutdown()

    return wmos_fp_phy, wmos_fp_bgc

##########################################################################

if __name__ == "__main__":
    argo_download()
