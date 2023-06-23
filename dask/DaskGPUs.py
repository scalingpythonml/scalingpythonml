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
# porting your code to a cluster where some workers have GPUs and some not
# painful.
cluster = LocalCUDACluster(resources={"GPU": 1})
client = Client(cluster)
#end::dask_local_gpu[]


# In[ ]:


cluster


# In[ ]:


def how_many_gpus(x):
    import torch
    return torch.cuda.device_count()


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


import numpy as np
from numba import jit, guvectorize
import dask


# In[ ]:


#tag::ex_dask_submit_numba_incorrect[]
# Works in local mode, but not distributed
@dask.delayed
@guvectorize(['void(float64[:], intp[:], float64[:])'],
             '(n),()->(n)')
def delayed_move_mean(a, window_arr, out):
    window_width = window_arr[0]
    asum = 0.0
    count = 0
    for i in range(window_width):
        asum += a[i]
        count += 1
        out[i] = asum / count
    for i in range(window_width, len(a)):
        asum += a[i] - a[i - window_width]
        out[i] = asum / count


arr = np.arange(20, dtype=np.float64).reshape(2, 10)
print(arr)
print(dask.compute(delayed_move_mean(arr, 3)))
#end::ex_dask_submit_numba_incorrect[]


# In[ ]:


#tag::ex_dask_submit_numba_correct[]
@guvectorize(['void(float64[:], intp[:], float64[:])'],
             '(n),()->(n)')
def move_mean(a, window_arr, out):
    window_width = window_arr[0]
    asum = 0.0
    count = 0
    for i in range(window_width):
        asum += a[i]
        count += 1
        out[i] = asum / count
    for i in range(window_width, len(a)):
        asum += a[i] - a[i - window_width]
        out[i] = asum / count


arr = np.arange(20, dtype=np.float64).reshape(2, 10)
print(arr)
print(move_mean(arr, 3))


def wrapped_move_mean(*args):
    return move_mean(*args)


# In[ ]:


a = dask.delayed(wrapped_move_mean)(arr, 3)
#end::ex_dask_submit_numba_correct[]


# In[ ]:


a


# In[ ]:


dask.compute(a)


# In[ ]:


from blazingsql import BlazingContext
import cudf
import numpy as np
bc = BlazingContext(dask_client=client)


# In[ ]:


df = cudf.DataFrame({chr(x): cudf.Series(
    np.arange(4172, dtype="float64")) for x in range(65, 66)})

import dask_cudf

ddf = dask_cudf.from_cudf(df, npartitions=2)


# In[ ]:


# Cpu fall back
import time


# In[ ]:


from dask.distributed import Client, LocalCluster
cluster = LocalCluster(resources={})
client = Client(cluster)


# In[ ]:


cluster.adapt(minimum=1, maximum=10)


# In[ ]:


def noop(x):
    return True


test_no_gpu_future = client.submit(noop, 1)
test_gpu_future = client.submit(
    noop, 1, resources={
        'GPU': 2, 'MEMORY': 70e100})


# In[ ]:


# First make sure that the normal task has finished
client.gather(test_no_gpu_future)
# We might take some time for the task to finish
time.sleep(1)
if


# In[ ]:


client.gather(test_no_gpu_future)


# In[ ]:


test_gpu_future


# In[ ]:


test_no_gpu_future


# In[ ]:


cluster


# In[ ]:
