#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from dask_jobqueue import SLURMCluster
from dask.distributed import Client


# In[ ]:





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
#end::ex_monitor_distributed_threads[]


# In[ ]:





# In[ ]:


from dask.distributed import Client
from dask.distributed import LocalCluster
cluster = LocalCluster(n_workers=10) 
client = Client(cluster)
client


# In[ ]:


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
#end::ex_generate_performance_report[]


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
#end::ex_get_task_stream[]


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

#end::ex_memory_sampler[]


# In[ ]:


#tag::ex_hpc_infinite_workers[]
from dask_jobqueue import SLURMCluster
from dask import delayed
from dask.distributed import Client

#we give walltime of 4 hours to the cluster spawn
#each Dask workers are told they have 5 min less than that for Dask to manage
#we tell workers to stagger their start and close in a random interval of 5min
#some workers will die, but others will be staggered alive, avoiding loss of job

cluster = SLURMCluster(    
    walltime="04:00:00",
    cores=24,
    processes=6
    memory="8gb",
    #args passed directly to worker
    worker_extra_args=["--lifetime", "235m", "--lifetime-stagger", "5m"],
    #path to the interpreter that you want to run the batch submission script
    shebang='#!/usr/bin/env zsh',
    #path to desired python runtime if you have a separate one
    python='~/miniconda/bin/python'
)

client = Client(cluster)

#end::ex_hpc_infinite_workers[]


# In[ ]:


#tag::ex_yarn_deployment_tuning[]

from dask_yarn import YarnCluster
from dask.distributed import Client
import logging
import os
import sys
import time

logger = logging.getLogger(__name__)

WORKER_ENV = {"HADOOP_CONF_DIR": "/data/app/spark-yarn/hadoop-conf", "JAVA_HOME": "/usr/lib/jvm/java"}

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s")

logger.info("Initialising YarnCluster")
cluster_start_time = time.time()

# say your desired conda environment for workers are located at
# /home/mkimmins/anaconda/bin/python
# similar syntax for venv and python executable
cluster = YarnCluster(
    environment='conda:///home/mkimmins/anaconda/bin/python',
    worker_vcores=2,
    worker_memory="4GiB")

logger.info("Initialising YarnCluster: done in %.4f", time.time() - cluster_start_time)

logger.info("Initialising Client")
client = Client(cluster)
logger.info("Initialising Client: done in %.4f", time.time() - client_start_time)

# Important and common misconfig is to not have node's versions match
versions = dask_client.get_versions(check=True)

#end::ex_yarn_deployment_tuning[]


# In[ ]:


#tag::ex_yarn_deployment_CLI_tuning[]
get_ipython().system('dask-yarn submit')
  --environment home/mkimmins/anaconda/bin/python   --worker-count 20   --worker-vcores 2   --worker-memory 4GiB   your_python_script.py

# Since we already deployed and ran YARN cluster,
# we replace YarnCluster(...) with from_current() to reference it
cluster = YarnCluster.from_current()
    
# This would give you YARN application ID
# application_1516806604516_0019
# status check, kill, view log of application
get_ipython().system('dask-yarn status application_1516806604516_0019')
get_ipython().system('dask-yarn kill application_1516806604516_0019')
get_ipython().system('yarn logs -applicationId application_1516806604516_0019')

#end::ex_yarn_deployment_tuning[]


# In[ ]:


#tag::ex_deploy_SLURM_by_hand[]
from dask_jobqueue import SLURMCluster
from dask.distributed import Client

def create_slurm_clusters(cores, processes, workers, memory="16GB", queue='regular', account="slurm_account", username="mkimmins"):
    cluster = SLURMCluster(
        #ensure walltime request is reasonable within your specific cluster    
        walltime="04:00:00",
        queue=queue,
        account=account,        
        cores=cores,
        processes=processes,
        memory=memory,
        worker_extra_args=["--resources GPU=1"],
        job_extra=['--gres=gpu:1'],
        job_directives_skip=['--mem', 'another-string'],
        job_script_prologue = ['/your_path/pre_run_script.sh', 'source venv/bin/activate'],
        interface='ib0',
        log_directory='dask_slurm_logs',
        python=f'srun -n 1 -c {processes} python',
        local_directory=f'/dev/{username}',
        death_timeout=300
    )
    cluster.start_workers(workers)
    return cluster

cluster = create_slurm_clusters(cores=4, processes=1, workers=4)
cluster.scale(10)
client = Client(cluster)
#end::ex_deploy_SLURM_by_hand[]


# In[ ]:


#tag::ex_slurm_deployment_tuning[]
import time
from dask import delayed
from dask.distributed import Client, LocalCluster
# Note we introduce progress bar for future execution in a distributed context here
from dask.distributed import progress
from dask_jobqueue import SLURMCluster
import numpy as np
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s")

def visit_url(i):
    return "Some fancy operation happened. Trust me."

@delayed
def crawl(url, depth=0, maxdepth=1, maxlinks=4):
    # some complicated and async job
    # refer to chapter 2 for full implementation of crawl    
    time.sleep(1)
    some_output = visit_url(url)
    return some_output

def main_event(client):
    njobs = 100
    outputs = []
    for i in range(njobs):
        # assume we have a queue of work to do
        url = work_queue.deque()
        output = crawl(url)
        outputs.append(output)

    results = client.persist(outputs)
    logger.info(f"Running main loop...")
    progress(results)

def cli():
    cluster = create_slurm_clusters(cores=10, processes=10, workers=2)
    logger.info(f"Submitting SLURM job with jobscript: {cluster.job_script()}")
    client = Client(cluster)
    main_event(client)    

if __name__ == "__main__":
    logger.info("Initialising SLURM Cluster")
    cli()
#end::ex_slurm_deployment_tuning[]


# In[ ]:





# In[ ]:


cluster.close()


# In[ ]:


client.close()

