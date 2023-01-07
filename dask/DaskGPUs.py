#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Dask local GPU
import dask


# In[ ]:


#tag::dask_local_gpu[]
from dask_cuda import LocalCUDACluster
from dask.distributed import Client
#NOTE: The resources= flag is important, by default the LocalCUDACluster *does not* label any resources which can make
# porting your code to a cluster where some workers have GPUs and some not painful.
cluster = LocalCUDACluster(resources={"GPU": 1})
client = Client(cluster)
#end::dask_local_gpu[]


# In[ ]:


cluster


# In[ ]:


def how_many_gpus(x):
    import torch
    return torch.cuda.device_count(); 


# In[ ]:


#tag::ex_submit_gpu[]
future = client.submit(how_many_gpus, 1, resources={'GPU': 1})
#end::ex_submit_gpu[]


# In[ ]:


client.gather(future)


# In[ ]:


#tag::ex_annotate_gpu[]
with dask.annotate(resources={'GPU': 1}):
    future = client.submit(how_many_gpus, 1)
#end::ex_annotate_gpu[]


# In[ ]:




