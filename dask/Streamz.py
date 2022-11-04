#!/usr/bin/env python
# coding: utf-8

# In[2]:


#tag::get_started_streamz[]
import dask
import dask.dataframe as dd
from streamz import Stream
from dask.distributed import Client

client = Client()
#end::get_started_streamz[]


# In[3]:


#tag::make_local_stream[]
local_stream = Stream.from_iterable(
    ["Fight",
     "Flight",
     "Freeze",
     "Fawn"])
dask_stream = local_stream.scatter()
#end::make_local_stream[]


# In[5]:


#tag::define_sink[]
dask_stream.gather().sink(print)
#end::define_sink[]
#tag::run[]
dask_stream.start()
#end::run[]


# In[6]:


import time
#time.sleep(5)


# In[7]:


#tag::make_kafka_stream[]
batched_kafka_stream = Stream.from_kafka_batched(
    topic="quickstart-events",
    dask=True, # StreamZ will call scatter internally for us
    max_batch_size = 2, # We want this to run quickly so small batches.
    consumer_params={
        'bootstrap.servers': 'localhost:9092',
        'auto.offset.reset': 'earliest', #Start from the start
        'group.id': 'my_special_streaming_app12'}, # Consumer group id, Kafka will only deliver messages once* per consumer group.
         poll_interval=0.01) #Note some sources take a string and some take a float :/
#end::make_kafka_stream[]


# In[ ]:


#tag::wc[]
local_wc_stream = (batched_kafka_stream
                   .map(lambda batch: map(lambda b: b.decode("utf-8"), batch)) # .map gives us a per batch view, starmap per elem
                   .map(lambda batch: map(lambda e: e.split(" "), batch))
                   .map(list)
                   .gather()
                   .flatten().flatten() # We need to flatten twice.
                    .frequencies()
                  ) #ideally we'd call flatten frequencies before the gather, but they don't work on DaskStream
local_wc_stream.sink(lambda x: print(f"WC {x}"))
batched_kafka_stream.start() # Start processing the stream now that we've defined our sinks.
#end::wc[]


# In[ ]:


#tag::wc_windowed[]
windowed = (batched_kafka_stream
                   .map(lambda batch: map(lambda b: b.decode("utf-8"), batch)) # .map gives us a per batch view, starmap per elem
                   .map(lambda batch: map(lambda e: e.split(" "), batch))
                   .map(list)
                   .sliding_window(3) # Last three batches, note this creates state (yay?)
                   .gather()
                   .flatten().flatten().flatten() # We need to flatten *three* times.
                    .frequencies()
                  ) #ideally we'd call flatten frequencies before the gather, but they don't work on DaskStream
windowed.sink(lambda x: print(f"WINDOWED {x}"))
#end::wc_windowed[]


# In[ ]:





# In[ ]:


time.sleep(5)


# In[ ]:




