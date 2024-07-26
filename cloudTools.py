#!/usr/bin/env python3

## @file cloudTools.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Fri 26 Jul 2024

##########################################################################
import coiled
import dask.dataframe as dd
##########################################################################

class cloudTools():

    """
    class cloudTools: methods to set up Coiled-AWS cluster for oceanographic
    database and retrieve dask dataframe
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, setup_cluster=True):

        if not setup_cluster: pass

        # set up coiled cluster
        self.launch_coiled()

        pass

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

#------------------------------------------------------------------------------#
## Set up coiled cluster. By design choice all parameters are hardcoded: ideally
## the user does not need to set them up
    def launch_coiled(self):

        self.cluster = coiled.Cluster(
            name="read-parquet-demo",
            n_workers=1,  # simple test on a 20~MB file, no need for parallel per se
            region="us-east-1",  # Start workers in same region as data to minimize costs
            compute_purchase_option="spot_with_fallback", # spot instances (cheaper), if none available: fall back to normal on-demand
            use_best_zone = True, # pick best availability zone within specified region (should reduce costs)
            idle_timeout="20 minutes", # cluster shut downs if idle for this time
            workspace="enrico" # linked to WHOI's AWS
        )

        client = self.cluster.get_client()

#------------------------------------------------------------------------------#
## Close down coiled cluster
    def close_coiled(self):
        self.cluster.shutdown()

#------------------------------------------------------------------------------#
## Import data into Dask Dataframe and load them into virtual memory
    def load_data(self,cols=None,filters=None):

        # validating input
        if cols is None:
            if filters is None:
                raise("Safeguard: No filtering by columns or rows provided. This would load to large a dataset and execution is interrupted.")
            else:
                cols = []
        if filters is None:
            filters = []

        ## importing data into Dask dataframe
        ddf = dd.read_parquet(
            "s3://argo-experimental/pqt/data/ArgoPHY0357.parquet", # 20 MB file
            engine="pyarrow",
            storage_options={"anon": True, "use_ssl": True},
            columns = cols,
            filters = filters
        )

        ## computing memory usage of a dask dataframe partition
        def partition_memory_usage(ddf_partition):
            return ddf_partition.memory_usage(deep=True).sum()

        ## estimating dataframe size
        ddf_size = ddf.map_partitions(partition_memory_usage).compute().sum() / (1024**3) #GB

        if ddf_size > 10:
            raise("Safeguard: Dataset too large for current implementation (>10 GB). Interrupting execution.")
        else:
            self.data_frame = ddf.persist()

##########################################################################

if __name__ == '__main__':
    test = cloudTools()
