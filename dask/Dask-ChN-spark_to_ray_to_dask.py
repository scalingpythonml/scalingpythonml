#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# In[ ]:


from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext


# In[ ]:


get_ipython().system('java -version')


# In[ ]:


import pyspark

# number_cores = 1
# memory_gb = 8
# conf = (
#     pyspark.SparkConf()
# #         .master('spark://xxx.xxx.xx.xx:7077') \
#         .setMaster('local[{}]'.format(number_cores)) \
#         .set('spark.driver.memory', '{}g'.format(memory_gb))        
# )
# sc = pyspark.SparkContext(conf=conf)
# # sqlContext = SQLContext(sc)
# sqlContext = SQLContext(sc)
# #http://localhost:4040/jobs/


# In[ ]:





# In[ ]:


import raydp
import ray 

ray.init()
spark = raydp.init_spark(
  app_name = "raydp_spark",
  num_executors = 1,
  executor_cores = 1,
  executor_memory = "4GB"
)


# In[ ]:


from ray.util.dask import ray_dask_get, enable_dask_on_ray, disable_dask_on_ray

enable_dask_on_ray()


# In[ ]:





# In[ ]:


df = spark.createDataFrame(["Anna","Bob","Sue"], "string").toDF("firstname")


# In[ ]:


df.show()


# In[ ]:


names = ["Anna", "Bob", "Liam", "Olivia", "Noah", "Emma", "Oliver", "Ava", "Elijah", "Charlotte"]
class StudentRecord:
    def __init__(self, record_id, name):
        self.record_id = record_id
        self.name = name
    def __str__(self):
        return f'StudentRecord(record_id={self.record_id},data={self.name})'
    
num_records = len(names)
student_records = [StudentRecord(i, f'{names[i]}') for i in range(num_records)] 


# In[ ]:


student_records


# In[ ]:


df = spark.createDataFrame(student_records, ['name', 'id'])


# In[ ]:


df.show()


# In[ ]:





# In[ ]:





# In[ ]:


ray_dataset = ray.data.from_spark(df)


# In[ ]:


ray_dataset.show()


# In[ ]:


from ray.util.dask import ray_dask_get, enable_dask_on_ray, disable_dask_on_ray
import dask.array as da
import dask.dataframe as dd
import numpy as np
import pandas as pd


# In[ ]:


#tag::dask_on_ray[]


# In[ ]:


import dask


# In[ ]:


enable_dask_on_ray()


# In[ ]:


ddf_students = ray.data.dataset.Dataset.to_dask(ray_dataset) 


# In[ ]:


ddf_students.head()


# In[ ]:


disable_dask_on_ray()


# In[ ]:


#end::dask_on_ray[]


# In[ ]:


from ray.util.dask import ray_dask_get


# In[ ]:


dask.config.get


# In[ ]:


dsk_config_dump = dask.config.config.get('distributed')


# In[ ]:


dsk_config_dump.get('dashboard').get('link')


# In[ ]:





# In[ ]:





# In[ ]:


# cluster.scheduler.services['dashboard'].server.address


# In[ ]:





# In[ ]:


# larger dataset


# In[ ]:


url = "https://gender-pay-gap.service.gov.uk/viewing/download-data/2021"
from pyspark import SparkFiles


# In[ ]:


spark.sparkContext.addFile(url)


# In[ ]:





# In[ ]:


df = spark.read.csv("file://"+SparkFiles.get("2021"), header=True, inferSchema= True)


# In[ ]:


df.show(3)


# In[ ]:


ray_dataset = ray.data.from_spark(df)


# In[ ]:


ray_dataset.show(3)


# In[ ]:


from dask.distributed import Client
client = Client()


# In[ ]:





# In[ ]:


ddf_pay = ray.data.dataset.Dataset.to_dask(ray_dataset) 


# In[ ]:


ddf_pay.compute()


# In[ ]:


ddf_pay.head(3)


# In[ ]:





# In[ ]:


def fillna(df):
    return df.fillna(value={"PostCode": "UNKNOWN"}).fillna(value=0)
    
new_df = ddf_pay.map_partitions(fillna)


# In[ ]:


new_df.compute()


# In[ ]:


# Since there could be an NA in the index clear the partition / division information
new_df.clear_divisions()
new_df.compute()
narrow_df = new_df[["PostCode", "EmployerSize", "DiffMeanHourlyPercent"]]


# In[ ]:


grouped_df = narrow_df.groupby("PostCode")


# In[ ]:


avg_by_postalcode = grouped_df.mean()


avg_by_postalcode.compute()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


sc.stop()


# In[ ]:


sqlContext.stop()

