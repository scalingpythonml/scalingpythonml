#!/usr/bin/env python
# coding: utf-8


import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext


get_ipython().system('java -version')


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


import raydp
import ray

ray.init()
spark = raydp.init_spark(
    app_name="raydp_spark",
    num_executors=1,
    executor_cores=1,
    executor_memory="4GB"
)


from ray.util.dask import ray_dask_get, enable_dask_on_ray, disable_dask_on_ray

enable_dask_on_ray()


df = spark.createDataFrame(["Anna", "Bob", "Sue"], "string").toDF("firstname")


df.show()


names = [
    "Anna",
    "Bob",
    "Liam",
    "Olivia",
    "Noah",
    "Emma",
    "Oliver",
    "Ava",
    "Elijah",
    "Charlotte"]


class StudentRecord:
    def __init__(self, record_id, name):
        self.record_id = record_id
        self.name = name

    def __str__(self):
        return f'StudentRecord(record_id={self.record_id},data={self.name})'


num_records = len(names)
student_records = [StudentRecord(i, f'{names[i]}') for i in range(num_records)]


student_records


df = spark.createDataFrame(student_records, ['name', 'id'])


df.show()


ray_dataset = ray.data.from_spark(df)


ray_dataset.show()


from ray.util.dask import ray_dask_get, enable_dask_on_ray, disable_dask_on_ray
import dask.array as da
import dask.dataframe as dd
import numpy as np
import pandas as pd


#tag::dask_on_ray[]
import dask

enable_dask_on_ray()
ddf_students = ray.data.dataset.Dataset.to_dask(ray_dataset)
ddf_students.head()

disable_dask_on_ray()
#end::dask_on_ray[]


from ray.util.dask import ray_dask_get


dask.config.get


dsk_config_dump = dask.config.config.get('distributed')


dsk_config_dump.get('dashboard').get('link')


# cluster.scheduler.services['dashboard'].server.address


# larger dataset


url = "https://gender-pay-gap.service.gov.uk/viewing/download-data/2021"
from pyspark import SparkFiles


spark.sparkContext.addFile(url)


df = spark.read.csv(
    "file://" +
    SparkFiles.get("2021"),
    header=True,
    inferSchema=True)


df.show(3)


ray_dataset = ray.data.from_spark(df)


ray_dataset.show(3)


from dask.distributed import Client
client = Client()


ddf_pay = ray.data.dataset.Dataset.to_dask(ray_dataset)


ddf_pay.compute()


ddf_pay.head(3)


def fillna(df):
    return df.fillna(value={"PostCode": "UNKNOWN"}).fillna(value=0)


new_df = ddf_pay.map_partitions(fillna)


new_df.compute()


# Since there could be an NA in the index clear the partition / division
# information
new_df.clear_divisions()
new_df.compute()
narrow_df = new_df[["PostCode", "EmployerSize", "DiffMeanHourlyPercent"]]


grouped_df = narrow_df.groupby("PostCode")


avg_by_postalcode = grouped_df.mean()


avg_by_postalcode.compute()


sc.stop()


sqlContext.stop()
