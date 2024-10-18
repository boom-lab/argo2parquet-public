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
from argo2parquet.generateSchema import generateSchema
import time
from pathlib import Path
##########################################################################

def argo_convert( flists, metadata, db_names, outdir_parquet, schema_path):

    flist_phy = flists[0]
    flist_bgc = flists[1]

    metadata_phy = metadata[0]
    metadata_bgc = metadata[1]

    for k in range(len(db_names)):

        start_time = time.time()

        db_name = db_names[k]
        print("Converting " + db_name + " database...")
        genSchema = generateSchema(outdir=schema_path, db=db_name)
        schema_fname = genSchema.schema_fname
        print("Schema file for " + db_name + " database: " + schema_fname)

        if db_name=="phy":
            flist = flist_phy
            metadata = metadata_phy
            nw = 18
            tw = 1
            memlim = "5.5GB"
        elif db_name=="bgc":
            flist = flist_bgc
            metadata = metadata_bgc
            nw = 9
            tw = 1
            memlim = "11GB"

        print("(nw, tw)")
        print( (nw, tw) )

        # convert metadata
        if len(metadata) > 0:
            metadata_dir = outdir_parquet + "metadata/"
            Path(metadata_dir).mkdir(parents = True, exist_ok = True)
            parquet_filename = metadata_dir + "Argo" + db_name.upper() + "_metadata.parquet"
            metadata.to_parquet(parquet_filename)
            print("Metadata stored to " + str(parquet_filename) + ".")

            client = Client(
            n_workers=nw,
            threads_per_worker=tw,
            processes=True,
            memory_limit=memlim,
        )

        chunksize = 1000

        daskConverter = daskTools(
            db_type = db_name.upper(),
            out_dir = outdir_parquet+db_name,
            flist = flist,
            schema_path = schema_fname,
            chunk = chunksize,
        )

        daskConverter.convert_to_parquet()

        client.shutdown()

        elapsed_time = time.time() - start_time
        print("Time to convert " + db_name + " database: " + str(elapsed_time))

##########################################################################

if __name__ == "__main__":
    argo_download()
