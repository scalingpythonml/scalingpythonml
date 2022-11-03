#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# !pip install dask-yarn


# In[ ]:


# !conda install -c conda-forge dask-yarn
# import sys
# sys.version


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





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
#end::ex_yarn_deployment


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
#end::ex_slurm_deployment


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
#end::ex_s3_minio_rw


# In[ ]:





# In[ ]:


get_ipython().system('pip install fugue')


# In[ ]:


# ! pip install  antlr==4.10.1


# In[ ]:


from fugue_notebook import setup
try:
    setup()
except:
    pass


# In[ ]:


from fugue import transform


# In[ ]:


get_ipython().system('pip install fugue')
get_ipython().system('pip install fugue[sql]')


# In[ ]:


get_ipython().system('pip install s3fs')


# In[ ]:


get_ipython().system('conda  install aiohttp')


# In[ ]:


# !conda install requests
# !pip install antlr4-python3-runtime
# !pip install antlr4-python3-runtime==4.10


# In[ ]:


# !pip install fugue
# !pip install fugue[sql]
# !pip install s3fs
# !conda  install aiohttp


# In[ ]:


# import antlr4
from fugue import transform


# In[1]:


from fugue_notebook import setup
setup()


# In[ ]:





# In[2]:


setup()


# In[ ]:





# In[4]:


import dask.dataframe as dd


# In[5]:


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
#end::ex_fugue_SQL


# In[ ]:





# In[8]:


setup(is_lab=True)


# In[14]:


get_ipython().run_cell_magic('fsql', 'dask', 'tempdf = SELECT VendorID, AVG(total_amount) AS average_fare FROM df GROUP BY VendorID\n\nSELECT *\nFROM tempdf\nORDER BY average_fare DESC\nLIMIT 5\nPRINT\n')


# In[ ]:




