#!/usr/bin/env python3

## @file argo_download.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 03 Sep 2024

##########################################################################
import pandas as pd
# ignore pandas "educational" performance warnings
from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
import argo2parquet.argo_tools as at
import copy
import time
##########################################################################

def argo_download(gdac_path, outdir_nc, db_names, dryrun_flag):

    if dryrun_flag:
        nproc = 1
    else:
        nproc = 36

    wmos_fp_phy = []
    wmos_fp_bgc = []
    metadata_phy = []
    metadata_bgc = []

    for k in range(len(db_names)):
        start_time = time.time()

        db_name = db_names[k]
        print("Database " + db_name + "...")

        wmos, metadata, wmos_fp = at.argo_gdac(
            gdac_path=gdac_path,
            dataset=db_name,
            save_to=outdir_nc,
            download_individual_profs=False,
            skip_downloads=False,
            dryrun=dryrun_flag,
            overwrite_profiles=True,
            NPROC=nproc,
            verbose=True,
            checktime=True
        )

        if db_name=="phy":
            wmos_fp_phy = copy.deepcopy(wmos_fp)
            metadata_phy = metadata
        elif db_name=="bgc":
            wmos_fp_bgc = copy.deepcopy(wmos_fp)
            metadata_bgc = metadata

        print("done.")
        elapsed_time = time.time() - start_time
        print("Time to donwload " + db_name + " database: " + str(elapsed_time))

    return wmos_fp_phy, wmos_fp_bgc, metadata_phy, metadata_bgc


##########################################################################

if __name__ == "__main__":
    argo_download()
