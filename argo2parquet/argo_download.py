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
##########################################################################

def argo_download(gdac_path, outdir_nc, db_names, dryrun_flag):

    if dryrun_flag:
        nproc = 1
    else:
        nproc = 36

    wmos_fp_phy = []
    wmos_fp_bgc = []

    for k in range(len(db_names)):
        db_name = db_names[k]
        print("Database " + db_name + "...")

        wmos, df2, wmos_fp = at.argo_gdac(
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
        elif db_name=="bgc":
            wmos_fp_bgc = copy.deepcopy(wmos_fp)

        print("done.")

    return wmos_fp_phy, wmos_fp_bgc


##########################################################################

if __name__ == "__main__":
    argo_download()
