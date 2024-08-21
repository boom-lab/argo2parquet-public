#!/usr/bin/env python3

## @file daskTools.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 20 Aug 2024

##########################################################################
import dask
import dask.dataframe as dd
import pyarrow.parquet as pq
import pandas as pd
import xarray as xr
import numpy as np
# ignore pandas "educational" performance warnings
from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
##########################################################################

class daskTools():

    """class daskTools:
    methods to convert the argo database to parquet format
    and that use the dask module
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, db_type=None, out_dir=None, flist=None, schema_path='../schemas', chunk=2000):
        """Constructor

        Arguments:
        db_type     -- "PHY" for physical Argo, "BGC" for biogeochemical Argo
        out_dir     -- destination directory for the converted database
        flist       -- list of paths to Argo files to be converted
        schema_path -- path to ArgoPHY_schema.metadata and ArgoBGC_schema.metadata
        chunk       -- number of files processed at a time by dask
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

        if out_dir is None:
            self.out_dir = './ArgoParquet/'
        else:
            self.out_dir = out_dir

        self.chunk = chunk

        if schema_path is None:
            self.schema_path = '../schemas/Argo' + self.db_type + '_schema.metadata'
        else:
            self.schema_path = schema_path + 'Argo' + self.db_type + '_schema.metadata'
        self.__translate_pq_to_pd()

        self.__assign_vars()
        self.VARS = sorted(self.VARS)

        pass

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

#------------------------------------------------------------------------------#
## Delayed function to read an Argo profile into a dataframe with a prescribed
## schema
    @dask.delayed
    def read_argo(self,argo_file,VARS,partition_on_time=False):
        """ Read Argo file into dataframe

        Arguments:
        argo_file -- path to file
        VARS      -- variables to include in target dataframe
        partition_on_time -- bool to add field to schema to partition
                             in subfolders on time

        Returns:
        df -- dataframe

        Exceptions:
        if the Argo file cannot be read, the file name is printed to screen
        and returns an empty dataframe for compatibility when dask gathers
        returns from multiple workers
        """

        try:
            ds = xr.open_dataset(argo_file, engine="argo") #loading into memory the profile
            invars = list(set(self.VARS) & set(list(ds.data_vars)))
            df = ds[invars].to_dataframe()
            df = df.reset_index() #flatten dataframe
        except:
            print('Failed on ' + str(argo_file))
            # create empty dataframe
            df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in self.pd_dict.items()})

        #ensures that all data frames have all the columns and in the same order; it creates NaNs where not present
        df=df.reindex( columns=self.VARS )

        # enforcing dtypes otherwise to_parquet() gives error when appending
        df = df.astype(self.pd_dict)

        # adding JULD_D variable for partitioning in time
        # if partition_on_time:
        #     df['JULD_D'] = df['JULD'].dt.floor('30D')

        return df

#------------------------------------------------------------------------------#
## Performs conversion
    def convert_to_parquet(self,flist=None, out_dir=None, chunk=None):
        """Performs conversion by building compute graph and triggering
        operations

        Arguments:
        flist    -- list of paths to files to convert
        out_dir   -- output directory for the parquet database
        chunk    -- number of files processed at a time
        """

        if flist is None:
            flist = self.flist
        if out_dir is None:
            out_dir = self.out_dir
        if chunk is None:
            chunk = self.chunk

        for j in range( int(np.ceil(len(flist)/chunk)) ):
            initchunk = j*chunk
            endchunk = (j+1)*chunk
            if endchunk > len(flist):
                endchunk = len(flist)

            df = [ self.read_argo(file,self.pd_dict,self.VARS) for file in flist[initchunk:endchunk] ]
            df = dd.from_delayed(df) # creating unique df from list of df
            df = df.repartition(partition_size="300MB")

            name_function = lambda x: f"Argo{self.db_type}_dask_{j}_{x}.parquet"

            # to_parquet() triggers execution of lazy functions
            if j>0:
                append_db = True # append to pre-existing partition
            else:
                append_db = False
                df.to_parquet(
                    out_dir,
                    engine="pyarrow",
                    name_function = name_function,
                    append = append_db,
                    write_metadata_file = True,
                    write_index=False
                )

        print("stored.")

#------------------------------------------------------------------------------#
## Convert parquet schema to pandas
    def __translate_pq_to_pd(self):
        """Convert parquet schema to pandas schema

        Generates:
        pd_dict -- schema for pandas dataframe
        """

        schema = pq.read_schema(self.schema_path)

        pd_types = []

        for d in schema.types:
            pd_types.append(d.to_pandas_dtype())
            pd_dict = dict(zip(schema.names,pd_types))

        self.pd_dict = pd_dict


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
    test = daskTools()
