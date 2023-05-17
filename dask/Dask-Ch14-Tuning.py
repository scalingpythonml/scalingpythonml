#!/usr/bin/env python
# coding: utf-8

# In[1]:


from dask_jobqueue import SLURMCluster
from dask.distributed import Client


# In[2]:


#tag::ex_deploy_SLURM_by_hand[]
from dask_jobqueue import SLURMCluster
from dask.distributed import Client

cluster = SLURMCluster(
    n_workers = 4,
    queue='regular',
    account=account_name,
    job_directives_skip=['--mem', 'another-string'],
    job_script_prologue = ['/your_path/pre_run_script.sh', 'source venv/bin/activate'],
    cores=2,
    memory="25 GB",
    walltime='04:00:00',
    interface='ib0'
)
cluster.scale(10)
client = client(cluster)
#end::ex_deploy_SLURM_by_hand


# In[ ]:





# In[ ]:


#tag::ex_monitor_distributed_threads[]
import threading, sys, time

main_thread = threading.get_ident()

def print_frame(frame, event):
    print(frame, event)
    return print_frame

current_frame = sys._getframe().f_trace = print_frame

def print_frame():
    frame = sys._current_frames()[main_thread]
    print frame
    
def print_worker_config():
    return dask.config.get("distributed.worker.use-file-locking")
#end::ex_monitor_distributed_threads


# In[ ]:





# In[64]:


from dask.distributed import Client
from dask.distributed import LocalCluster
cluster = LocalCluster(n_workers=10) 
client = Client(cluster)
client


# In[65]:


import dask.array as da
import numpy as np


# In[ ]:





# In[ ]:


#tag::ex_generate_performance_report[]
from dask.distributed import performance_report

with performance_report(filename="computation_report.html"):
    gnarl = da.random.beta(1,2, size=(10000, 10000, 10), chunks=(1000,1000,5))
    x = da.random.random((10000, 10000, 10), chunks=(1000, 1000, 5))
    y = (da.arccos(x)*gnarl).sum(axis=(1,2))
    y.compute()
#end::ex_generate_performance_report


# In[ ]:


#tag::ex_get_task_stream[]
from dask.distributed import get_task_stream

with get_task_stream() as ts:
    gnarl = da.random.beta(1,2, size=(100, 100, 10), chunks=(100,100,5))
    x = da.random.random((100, 100, 10), chunks=(100, 100, 5))
    y = (da.arccos(x)*gnarl).sum(axis=(1,2))
    y.compute()
history = ts.data

#display the task stream data as dataframe
history_frame = pd.DataFrame(history, columns = ['worker','status','nbytes', 'thread', 'type', 'typename', 'metadata', 'startstops', 'key'])

#plot task stream
ts.figure
#end::ex_get_task_stream


# In[ ]:





# In[ ]:


#tag::ex_memory_sampler[]
from distributed.diagnostics import MemorySampler
from dask_kubernetes import KubeCluster
from distributed import Client

cluster = KubeCluster()
client = Client(cluster)

ms = MemorySampler()

#some gnarly compute
gnarl = da.random.beta(1,2, size=(100, 100, 10), chunks=(100,100,5))
x = da.random.random((100, 100, 10), chunks=(100, 100, 5))
y = (da.arccos(x)*gnarl).sum(axis=(1,2))
    
with ms.sample("memory without adaptive clusters"):
    y.compute()

#enable adaptive scaling
cluster.adapt(minimum=0, maximum=100) 

with ms.sample("memory with adaptive clusters"):
    y.compute()

#plot the differences
ms.plot(align=True, grid=True)

#end::ex_memory_sampler


# In[ ]:





# In[ ]:





# In[ ]:


cluster.close()


# In[ ]:


client.close()

