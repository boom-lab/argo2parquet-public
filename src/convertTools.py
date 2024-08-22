#!/usr/bin/env python3

## @file converter.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 21 Aug 2024

##########################################################################
import pandas as pd
import xarray as xr
import numpy as np
import multiprocessing
from pathlib import Path
import gc
import os
import warnings
import argo_tools as at

# ignore pandas "educational" performance warnings
from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

# importing iterators
# * `islice` returns selected elements from iterable
# * `batched` split the iterable object into tuples of prescribed length _n_ (if
#   `length(iterable)%n~=0`, the last tuple is shorter than _n_).
import sys
import itertools
from itertools import islice

if sys.version_info >= (3, 12):
    from itertools import batched
else:
    try:
        from more_itertools import batched
    except ImportError:
        def batched(iterable, chunk_size):
            iterator = iter(iterable)
            while chunk := tuple(islice(iterator, chunk_size)):
                yield chunk
##########################################################################

class convertTools():

    """class convertTools:
    original methods to convert the argo database to parquet format using
    python multiprocessing package
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, db_type=None, out_dir=None, flist=None, metadata_table = None, metadata_dir=None, single_process = None):
        """Constructor

        Arguments:
        db_type     -- "PHY" for physical Argo, "BGC" for biogeochemical Argo
        out_dir     -- destination directory for the converted database
        flist       -- list of paths to Argo files to be converted
        metadata_dir -- path to store metadata to
        """

        if db_type is None:
            print("No database type provided, remember to provide the variables you desire to read.")
        elif db_type not in ["PHY", "BGC"]:
            raise ValueError("db_type can only take values PHY or BGC.")
        
        self.db_type = db_type;

        if flist is None:
            print("No file lists provided, remember to assign one to self.flist before triggering the database conversion.")
        else:
            self.flist = flist

        if metadata_table is None:
            print("No metadata provided, remember to assign one to self.metadata_table before triggering the database conversion.")
        else:
            self.metadata_table = metadata_table

        if out_dir is None:
            self.out_dir = './ArgoParquet_mp/'
        else:
            self.out_dir = out_dir
        Path(self.out_dir).mkdir(parents= True, exist_ok= True)

        if metadata_dir is None:
            self.metadata_dir = self.out_dir + 'metadata/'
        else:
            self.metadata_dir = metadata_dir
        Path(self.metadata_dir).mkdir(parents= True, exist_ok= True)

        if single_process is None:
            self.single_process = False;
        else:
            self.single_process = single_process;

        self.__assign_vars()
        self.VARS = sorted(self.VARS)

        self.MAXPROC = 20

        pass

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

#------------------------------------------------------------------------------#
## Set up parallel environment and call conversion methods

    def convert(self):

        # converting metadata
        metadata_dir = self.metadata_dir
        parquet_filename = metadata_dir + "test_metadata.parquet"
        self.metadata_table.to_parquet(parquet_filename)
        print(str(parquet_filename) + " stored.")

        # converting profiles
        flist = self.flist
        print("Processing " + str(len(flist)) + " files.")

        if not self.single_process:
            nc_size_per_pqt = 40 # Empirically, 40 MB of average .nc file size gives in-memory sizes between 100-330 MB, which is what Dask recommens
            NPROC, chunks,size_per_proc = self.poolParams(nc_size_per_pqt)

        # fixing max nb of processes to prevent bottleneck likely due to I/O on disk queing operations and filling up the memory
        MAXPROC = self.MAXPROC
        if size_per_proc > 300:
            MAXPROC = 20

        if NPROC > MAXPROC and not self.single_process:
            print("Estimated number of processors might create bottleneck issues. Forcing to use " + str(MAXPROC) + " processors at a time.")
            # force to use at most MAXPROC processes, by looping over chunks
            full_loops = NPROC//MAXPROC  #nb of loops to use at most MAXPROC
            RESPROC = NPROC%MAXPROC   #nb of residual processors after the loops

            i_start = 0
            i_end   = 0
            failed_files = []
            for full_loop in range(full_loops):
                i_start = MAXPROC*full_loop
                i_end   = MAXPROC*(full_loop+1)
                pool_obj = multiprocessing.Pool(processes=MAXPROC)
                failed_files.append( pool_obj.starmap(self.xr2pqt, [(rank, chunk, full_loop) for rank, chunk in enumerate(chunks[i_start:i_end])] ) )
                pool_obj.close()

            # multiprocessing across residual processor pool with NPROC<MAXPROC
            if RESPROC > 0:
                pool_obj = multiprocessing.Pool(processes=RESPROC)
                failed_files.append( pool_obj.starmap(self.xr2pqt, [(rank, chunk, full_loop+1) for rank, chunk in enumerate(chunks[(i_end+1):])] ) )
                pool_obj.close()

        elif NPROC > 1 and not single_process:
            failed_files = []
            pool_obj = multiprocessing.Pool(processes=NPROC)
            failed_files.append( pool_obj.starmap(self.xr2pqt, [(rank, chunk, 0) for rank, chunk in enumerate(chunks)] ) )
            pool_obj.close()

        else:
            failed_files = self.xr2pqt(0,flist,0)

        failed = []
        for f in failed_files:
            for g in f:
                if len(g) > 0:
                    for h in g:
                        failed.append(h)
                        print(h)

        self.failed = failed
        print('Files that encountered an error and were not converted:')
        print(self.failed)

#------------------------------------------------------------------------------#
## Converting the netCDF files
    def xr2pqt(self,rank,files_list,loop_id):
        """ Read Argo file into xarray, convert to pandas table, store in parquet

        Arguments:
        rank -- processor number
        files_list -- list of files processed by processor
        loop_id -- current loop number (for this processor)

        Returns:
        argo_file_fail -- list of files that failed somewhere in the conversion

        Exceptions:
        if the Argo file cannot be read, the file name is printed to screen
        """

        df_list = []
        df_memory = 0
        counter = 0
        rank_str = "#" + str(rank) + ": "
        nb_files = len(files_list)
        argo_file_fail = []
        for argo_file in files_list:
            counter += 1
            if counter%10==0:
                print(rank_str + "processing file " + str(counter) + " of " + str(nb_files))

            try:
                ds = xr.load_dataset(argo_file, engine="argo") #loading into memory the profile
            except:
                print(rank_str + 'Failed on ' + str(argo_file))
                argo_file_fail.append(argo_file)
                continue

            # some floats don't have all the vars specified in VARS
            invars = list(set(self.VARS) & set(list(ds.data_vars)))
            df = ds[invars].to_dataframe()
            # df = ds.to_dataframe()

            if not df.empty:
                # for col in VARS:
                #     if col not in invars:
                #         df[col] = np.nan #ensures that all parquet files have all the VARS as columns
                # df_memory += df.memory_usage().sum()/(10**6) # tracking memory usage (in MB)
                df_list.append(df)

                df = None
                ds = None
                del df
                del ds
                gc.collect()

        # store to parquet once a large enough dataframe has been created
        print(rank_str + "Storing to parquet...")

        try:
            df_list = pd.concat(df_list, axis=0) # it automatically adds NaNs where needed
        except:
            print(rank_str + 'Could not concatenate pandas dataframes')
            print(rank_str + 'Failed on ' + str(argo_file) + '. Caution: more files might be affected.')
            print(rank_str + 'Data frames list:')
            print(df_list)
            return argo_file_fail

        df_memory = df_list.memory_usage().sum()/(1024**2)
        if df_memory < 1e3:
            print(rank_str + "In-memory filesize: " + "{:.2f}".format(df_memory) + " MB")
        else:
            print(rank_str + "In-memory filesize: " + "{:.2f}".format(df_memory/1024) + " GB")

        parquet_filename = self.out_dir + "test_profiles_levels_" + str(rank) + "_" + str(loop_id) + ".parquet"
        df_list.to_parquet(parquet_filename)
        print(rank_str + str(parquet_filename) + " stored.")

        df_list = None
        del df_list
        gc.collect()

        return argo_file_fail


#------------------------------------------------------------------------------#
## Set up parameter for parallel processing
    def poolParams(self,nc_size_per_pqt):
        flist = self.flist

        size_flist = []

        for f in flist:
            try:
                f_size = os.path.getsize(f)/1024**2
                size_flist.append( f_size ) #size in MB
            except:
                if not os.path.isfile(f):
                    gdac_root = 'https://usgodae.org/pub/outgoing/argo/dac/'
                    fpath = os.path.join( *f.split(os.path.sep)[-3:] )
                    response = at.get_func( gdac_root + fpath )
                    if response.status_code == 404:
                        print('File ' + f + ' returned 404 error from URL ' + str(gdac_root+fpath) + '. Skipping it.')
                    else:
                        print('File ' + f + ' likely present at URL ' + str(gdac_root+fpath) + '. You might want to check why it is not in the local drive.')
                else:
                    print('File ' + f + ' seems to exist in the local drive: not sure what is going on here.')
                continue

        size_tot = sum(size_flist)
        NPROC = int(np.ceil(size_tot/nc_size_per_pqt))
        size_per_proc = size_tot/NPROC

        print('')
        print('Size per processor (MB)')
        print(size_per_proc)
        print('')

        ids_sort = np.argsort(np.array(size_flist))

        chunks_ids = []
        x = np.copy(ids_sort)

        for j in range(NPROC):
            chunk_ids = []
            chunk_size = 0
            while ((chunk_size<size_per_proc) and (len(x) > 0)):
                if len(chunk_ids)%2 == 0:
                    chunk_ids.append(x[-1])
                    x = x[:-1]
                else:
                    chunk_ids.append(x[0])
                    x = x[1:]
                chunk_size = sum(np.asarray(size_flist)[chunk_ids])
            print(chunk_size)
            chunks_ids.append(chunk_ids)

        if len(x) > 0:
            warnings.warn(str(len(x)) + " files have not been assigned to a processor.")

        print('')
        chunks=[]
        skip_proc = 0
        total_memory = 0
        for j,chunk_ids in enumerate(chunks_ids):
            print('Size in processor ' + str(j) + ' (MB):')
            size_proc = sum(np.asarray(size_flist)[chunk_ids])
            total_memory += size_proc
            print(size_proc)
            if size_proc == 0:
                skip_proc += 1
                continue
            chunk = [flist[k] for k in chunk_ids]
            chunks.append(chunk)

        NPROC -= skip_proc

        print('')
        print("Using " + str(NPROC) + " processors")

        return NPROC, chunks, size_per_proc

#------------------------------------------------------------------------------#
## Select PHY or BGC variables
    def __assign_vars(self):
        """ Select variables in target Argo database"""

        if self.db_type == "PHY":
            self.VARS = [
                'PLATFORM_NUMBER',
                'TEMP',
                'PRES',
                'LONGITUDE',
                'PRES_ADJUSTED',
                'LATITUDE',
                'TEMP_QC',
                'TEMP_ADJUSTED_QC',
                'PSAL',
                'PRES_ADJUSTED_ERROR',
                'JULD',
                'CYCLE_NUMBER',
                'TEMP_ADJUSTED',
                'TEMP_ADJUSTED_ERROR',
                'PSAL_ADJUSTED_QC',
                'PSAL_QC',
                'PRES_ADJUSTED_QC',
                'PSAL_ADJUSTED',
                'PSAL_ADJUSTED_ERROR',
                'PRES_QC',
                'N_PROF',
                'N_LEVELS'
            ]

        elif self.db_type == "BGC":
            self.VARS = [
                'LATITUDE',
                'LONGITUDE',
                'JULD',
                'CYCLE_NUMBER',
                'PLATFORM_NUMBER',
                'N_PROF',
                'N_LEVELS',
                'PRES',
                'PRES_QC',
                'PRES_ADJUSTED',
                'PRES_ADJUSTED_QC',
                'PRES_ADJUSTED_ERROR',
                'TEMP',
                'TEMP_QC',
                'TEMP_dPRES',
                'TEMP_ADJUSTED',
                'TEMP_ADJUSTED_QC',
                'TEMP_ADJUSTED_ERROR',
                'PSAL',
                'PSAL_QC',
                'PSAL_dPRES',
                'PSAL_ADJUSTED',
                'PSAL_ADJUSTED_QC',
                'PSAL_ADJUSTED_ERROR',
                'DOXY',
                'DOXY_QC',
                'DOXY_dPRES',
                'DOXY_ADJUSTED',
                'DOXY_ADJUSTED_QC',
                'DOXY_ADJUSTED_ERROR',
                'BBP',
                'BBP_QC',
                'BBP_dPRES',
                'BBP_ADJUSTED',
                'BBP_ADJUSTED_QC',
                'BBP_ADJUSTED_ERROR',
                'BBP470',
                'BBP470_QC',
                'BBP470_dPRES',
                'BBP470_ADJUSTED',
                'BBP470_ADJUSTED_QC',
                'BBP470_ADJUSTED_ERROR',
                'BBP532',
                'BBP532_QC',
                'BBP532_dPRES',
                'BBP532_ADJUSTED',
                'BBP532_ADJUSTED_QC',
                'BBP532_ADJUSTED_ERROR',
                'BBP700',
                'BBP700_QC',
                'BBP700_dPRES',
                'BBP700_ADJUSTED',
                'BBP700_ADJUSTED_QC',
                'BBP700_ADJUSTED_ERROR',
                'TURBIDITY',
                'TURBIDITY_QC',
                'TURBIDITY_dPRES',
                'TURBIDITY_ADJUSTED',
                'TURBIDITY_ADJUSTED_QC',
                'TURBIDITY_ADJUSTED_ERROR',
                'CP',
                'CP_QC',
                'CP_dPRES',
                'CP_ADJUSTED',
                'CP_ADJUSTED_QC',
                'CP_ADJUSTED_ERROR',
                'CP660',
                'CP660_QC',
                'CP660_dPRES',
                'CP660_ADJUSTED',
                'CP660_ADJUSTED_QC',
                'CP660_ADJUSTED_ERROR',
                'CHLA',
                'CHLA_QC',
                'CHLA_dPRES',
                'CHLA_ADJUSTED',
                'CHLA_ADJUSTED_QC',
                'CHLA_ADJUSTED_ERROR',
                'CDOM',
                'CDOM_QC',
                'CDOM_dPRES',
                'CDOM_ADJUSTED',
                'CDOM_ADJUSTED_QC',
                'CDOM_ADJUSTED_ERROR',
                'NITRATE',
                'NITRATE_QC',
                'NITRATE_dPRES',
                'NITRATE_ADJUSTED',
                'NITRATE_ADJUSTED_QC',
                'NITRATE_ADJUSTED_ERROR',
                'BISULFIDE',
                'BISULFIDE_QC',
                'BISULFIDE_dPRES',
                'BISULFIDE_ADJUSTED',
                'BISULFIDE_ADJUSTED_QC',
                'BISULFIDE_ADJUSTED_ERROR',
                'PH_IN_SITU_TOTAL',
                'PH_IN_SITU_TOTAL_QC',
                'PH_IN_SITU_TOTAL_dPRES',
                'PH_IN_SITU_TOTAL_ADJUSTED',
                'PH_IN_SITU_TOTAL_ADJUSTED_QC',
                'PH_IN_SITU_TOTAL_ADJUSTED_ERROR',
                'DOWN_IRRADIANCE',
                'DOWN_IRRADIANCE_QC',
                'DOWN_IRRADIANCE_dPRES',
                'DOWN_IRRADIANCE_ADJUSTED',
                'DOWN_IRRADIANCE_ADJUSTED_QC',
                'DOWN_IRRADIANCE_ADJUSTED_ERROR',
                'DOWN_IRRADIANCE380',
                'DOWN_IRRADIANCE380_QC',
                'DOWN_IRRADIANCE380_dPRES',
                'DOWN_IRRADIANCE380_ADJUSTED',
                'DOWN_IRRADIANCE380_ADJUSTED_QC',
                'DOWN_IRRADIANCE380_ADJUSTED_ERROR',
                'DOWN_IRRADIANCE412',
                'DOWN_IRRADIANCE412_QC',
                'DOWN_IRRADIANCE412_dPRES',
                'DOWN_IRRADIANCE412_ADJUSTED',
                'DOWN_IRRADIANCE412_ADJUSTED_QC',
                'DOWN_IRRADIANCE412_ADJUSTED_ERROR',
                'DOWN_IRRADIANCE443',
                'DOWN_IRRADIANCE443_QC',
                'DOWN_IRRADIANCE443_dPRES',
                'DOWN_IRRADIANCE443_ADJUSTED',
                'DOWN_IRRADIANCE443_ADJUSTED_QC',
                'DOWN_IRRADIANCE443_ADJUSTED_ERROR',
                'DOWN_IRRADIANCE490',
                'DOWN_IRRADIANCE490_QC',
                'DOWN_IRRADIANCE490_dPRES',
                'DOWN_IRRADIANCE490_ADJUSTED',
                'DOWN_IRRADIANCE490_ADJUSTED_QC',
                'DOWN_IRRADIANCE490_ADJUSTED_ERROR',
                'DOWN_IRRADIANCE555',
                'DOWN_IRRADIANCE555_QC',
                'DOWN_IRRADIANCE555_dPRES',
                'DOWN_IRRADIANCE555_ADJUSTED',
                'DOWN_IRRADIANCE555_ADJUSTED_QC',
                'DOWN_IRRADIANCE555_ADJUSTED_ERROR',
                'UP_IRRADIANCE',
                'UP_IRRADIANCE_QC',
                'UP_IRRADIANCE_dPRES',
                'UP_IRRADIANCE_ADJUSTED',
                'UP_IRRADIANCE_ADJUSTED_QC',
                'UP_IRRADIANCE_ADJUSTED_ERROR',
                'UP_IRRADIANCE380',
                'UP_IRRADIANCE380_QC',
                'UP_IRRADIANCE380_dPRES',
                'UP_IRRADIANCE380_ADJUSTED',
                'UP_IRRADIANCE380_ADJUSTED_QC',
                'UP_IRRADIANCE380_ADJUSTED_ERROR',
                'UP_IRRADIANCE412',
                'UP_IRRADIANCE412_QC',
                'UP_IRRADIANCE412_dPRES',
                'UP_IRRADIANCE412_ADJUSTED',
                'UP_IRRADIANCE412_ADJUSTED_QC',
                'UP_IRRADIANCE412_ADJUSTED_ERROR',
                'UP_IRRADIANCE443',
                'UP_IRRADIANCE443_QC',
                'UP_IRRADIANCE443_dPRES',
                'UP_IRRADIANCE443_ADJUSTED',
                'UP_IRRADIANCE443_ADJUSTED_QC',
                'UP_IRRADIANCE443_ADJUSTED_ERROR',
                'UP_IRRADIANCE490',
                'UP_IRRADIANCE490_QC',
                'UP_IRRADIANCE490_dPRES',
                'UP_IRRADIANCE490_ADJUSTED',
                'UP_IRRADIANCE490_ADJUSTED_QC',
                'UP_IRRADIANCE490_ADJUSTED_ERROR',
                'UP_IRRADIANCE555',
                'UP_IRRADIANCE555_QC',
                'UP_IRRADIANCE555_dPRES',
                'UP_IRRADIANCE555_ADJUSTED',
                'UP_IRRADIANCE555_ADJUSTED_QC',
                'UP_IRRADIANCE555_ADJUSTED_ERROR',
                'DOWNWELLING_PAR',
                'DOWNWELLING_PAR_QC',
                'DOWNWELLING_PAR_dPRES',
                'DOWNWELLING_PAR_ADJUSTED',
                'DOWNWELLING_PAR_ADJUSTED_QC',
                'DOWNWELLING_PAR_ADJUSTED_ERROR'
            ]

        else:
            print("Variables list to read from Argo files not provided.")

        return

##########################################################################

if __name__ == '__main__':
    test = converter()
