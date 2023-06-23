#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# In[1]:


from dask.distributed import Client

client = Client("tcp://127.0.0.1:34711")
client


# In[6]:


from time import sleep


# In[8]:


from dask.distributed import progress
futs = client.map(sleep, range(0, 100))
progress(futs)


# In[ ]:
