#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# !pip install dask-yarn


# In[ ]:


# !conda install -c conda-forge dask-yarn
# import sys
# sys.version


# In[ ]:


#tag::ex_yarn_deployment[]

from dask_yarn import YarnCluster
from dask.distributed import Client

# Create a cluster where each worker has two cores and eight GiB of memory
cluster = YarnCluster(
    environment='your_environment.tar.gz',
    worker_vcores=2,
    worker_memory="4GiB")

# Scale out to num_workers such workers
cluster.scale(num_workers)

# Connect to the cluster
client = Client(cluster)
#end::ex_yarn_deployment[]


# In[ ]:





# In[ ]:


#tag::ex_slurm_deployment[]
from dask_jobqueue import SLURMCluster
from dask.distributed import Client

cluster = SLURMCluster(
    queue='regular',
    account="slurm_caccount",
    cores=24,
    memory="500 GB"
)
cluster.scale(jobs=SLURM_JOB_COUNT)  # ask for N jobs from SLURM

client = Client(cluster)

cluster.adapt(minimum_jobs=10, maximum_jobs=100)  # auto-scale between 10 and 100 jobs
cluster.adapt(maximum_memory="10 TB")  # or use core/memory limits
#end::ex_slurm_deployment[]


# In[ ]:





# In[ ]:


#tag::ex_s3_minio_rw[]
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq

minio_storage_options = {
    "key": MINIO_KEY,
    "secret": MINIO_SECRET,
    "client_kwargs": {
        "endpoint_url": "http://ENDPOINT_URL",
        "region_name": 'us-east-1'
    },
    "config_kwargs": {"s3": {"signature_version": 's3v4'}},
}

df.to_parquet(f's3://s3_destination/{filename}',
              compression="gzip",
              storage_options=minio_storage_options,
              engine = "fastparquet")


df = dd.read_parquet(
    f's3://s3_source/', 
    storage_options=minio_storage_options, 
    engine="pyarrow"
)
#end::ex_s3_minio_rw[]


# In[ ]:





# In[ ]:


# !pip install fugue
# ! pip install  antlr==4.10.1
# !pip install antlr4-python3-runtime==4.10.1


# In[ ]:





# In[ ]:





# In[ ]:


# !pip install fugue
# !pip install fugue[sql]
# !pip install s3fs
# !conda  install aiohttp


# In[ ]:





# In[ ]:





# In[ ]:


from fugue import transform


# In[ ]:


from fugue_notebook import setup
try:
    setup()
except:
    pass


# In[ ]:





# In[ ]:


import dask.dataframe as dd


# In[ ]:


url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-01.parquet'
df = dd.read_parquet(url)


# In[ ]:


#tag::ex_fugue_SQL[]
from fugue_notebook import setup

setup(is_lab=True)

get_ipython().run_line_magic('%fsql', 'dask')
tempdf = SELECT VendorID, AVG(total_amount) AS average_fare FROM df GROUP BY VendorID

SELECT *
FROM tempdf
ORDER BY average_fare DESC
LIMIT 5
PRINT
#end::ex_fugue_SQL[]


# In[ ]:





# In[ ]:


from fugue_notebook import setup
setup(is_lab=True)
url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-01.parquet'
df = dd.read_parquet(url)


# In[ ]:


get_ipython().run_cell_magic('fsql', 'dask', 'tempdf = SELECT VendorID, AVG(total_amount) AS average_fare FROM df GROUP BY VendorID\n\nSELECT *\nFROM tempdf\nORDER BY average_fare DESC\nLIMIT 5\nPRINT\n')


# In[ ]:





# In[ ]:


#tag::ex_postgres_dataframe[]
df = dd.read_sql_table('accounts', 'sqlite:///path/to/your.db',
                 npartitions=10, index_col='id')  
#end::ex_postgres_dataframe[]


# In[ ]:


#tag::ex_basic_logging[]
from dask.distributed import Client

client = Client()
client.log_event(topic = "custom_events", msg = "hello world")
client.get_events("custom_events")
#end::ex_basic_logging[]


# In[ ]:





# In[ ]:





# In[ ]:


import numpy as np
from datetime import datetime
import dask.distributed
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client, LocalCluster


# In[ ]:


# just sanity closing older client
# client.close()
cluster = LocalCluster()  # Launches a scheduler and workers locally
client = Client(cluster)  # Connect to distributed cluster and override default


# In[ ]:


#tag::ex_distributed_logging[]

from dask.distributed import Client, LocalCluster

client = Client(cluster)  # Connect to distributed cluster and override default

d = {'x': [3.0, 1.0, 0.2], 'y': [2.0, 0.5, 0.1], 'z': [1.0, 0.2, 0.4]}
scores_df = dd.from_pandas(pd.DataFrame(data=d), npartitions=1)

def compute_softmax(partition, axis = 0):
    """ computes the softmax of the logits
    :param logits: the vector to compute the softmax over
    :param axis: the axis we are summing over
    :return: the softmax of the vector
    """
    if partition.empty:
        return
    import timeit
    x = partition[['x', 'y', 'z']].values.tolist()
    start = timeit.default_timer()
    axis = 0
    e = np.exp(x - np.max(x))
    ret = e / np.sum(e, axis=axis)
    stop = timeit.default_timer()
    # partition.log_event("softmax", {"start": start, "x": x, "stop": stop})
    dask.distributed.get_worker().log_event("softmax", {"start": start, "input": x, "stop": stop})
    return ret

scores_df.apply(compute_softmax, axis=1, meta=object).compute()
client.get_events("softmax")
#end::ex_distributed_logging[]


# In[ ]:





# In[ ]:


# client.get_worker_logs()


# In[ ]:


# plain version of the function
def compute_softmax(x, axis = 0):
    """ computes the softmax of the logits
    :param logits: the vector to compute the softmax over
    :param axis: the axis we are summing over
    :return: the softmax of the vector
    """
    import timeit
    start = timeit.default_timer()
    axis = 0
    e = np.exp(x - np.max(x))
    ret = e / np.sum(e, axis=axis)
    stop = timeit.default_timer()
    dask.distributed.get_worker().log_event("softmax", {"start": start, "x": x, "stop": stop})
    return ret

scores_df.apply(compute_softmax, axis = 1, meta = object).compute()


# In[ ]:





# In[ ]:


client.close()

