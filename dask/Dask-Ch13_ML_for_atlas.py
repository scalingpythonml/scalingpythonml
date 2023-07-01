#!/usr/bin/env python
# coding: utf-8


# !pip install scikeras>=0.1.8
# !pip install tensorflow>=2.3.0
# !pip install -U skorch
# !pip install torch
# !pip install torchvision
# !pip install pytorch-cpu #not sure if i need to fix this
# !pip install s3fs
# !pip install dask_kubernetes
# !pip install pyarrow
# !pip install xgboost
import seaborn as sns
from dask_sql import Context
import dask.datasets
import dask.bag as db
from dask_cuda import LocalCUDACluster
import xgboost as xgb
from sklearn.linear_model import SGDRegressor as ScikitSGDRegressor
from sklearn.linear_model import LinearRegression as ScikitLinearRegression
import dask_ml.model_selection as dcv
from sklearn.metrics import r2_score
from dask_ml.linear_model import LinearRegression
from dask_ml.model_selection import train_test_split
import matplotlib.pyplot as plt
from joblib import parallel_backend
from dask_ml.preprocessing import PolynomialFeatures
from dask_ml.preprocessing import DummyEncoder
from pandas.api.types import CategoricalDtype
from dask_ml.preprocessing import Categorizer
import numpy as np
import dask.array as da
from dask_ml.preprocessing import StandardScaler
import dask
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
get_ipython().system('pip install dask_ml')


# !pip install cloudpickle==2.1.0
# !pip install dask==2022.05.0
# !pip install distributed==2022.5.0
# !pip install lz4==4.0.0
# !pip install msgpack==1.0.3
# !pip install toolz==0.11.2
# !pip install xgboost


# https://coiled.io/blog/tackling-unmanaged-memory-with-dask/
# filename = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-01.parquet'


# when working with clusters, specify cluster config, n_workers and worker_size
client = Client(n_workers=4,
                threads_per_worker=1,
                memory_limit=0)


url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-01.parquet'
df = dd.read_parquet(url)


#tag::ex_load_nyc_taxi[]
filename = './nyc_taxi/*.parquet'
df_x = dd.read_parquet(
    filename,
    split_row_groups=2
)
#end::ex_load_nyc_taxi


df.dtypes


df.shape


df


df.head()


#tag::ex_scaling_variables[]
from dask_ml.preprocessing import StandardScaler
import dask.array as da
import numpy as np

df = dd.read_parquet(url)
trip_dist_df = df[["trip_distance", "total_amount"]]
scaler = StandardScaler()

scaler.fit(trip_dist_df)
trip_dist_df_scaled = scaler.transform(trip_dist_df)
trip_dist_df_scaled.head()

#tag::ex_scaling_variables[]


#tag::ex_categorical_variables[]
from dask_ml.preprocessing import Categorizer
from pandas.api.types import CategoricalDtype

payment_type_amt_df = df[["payment_type", "total_amount"]]

cat = Categorizer(categories={"payment_type": CategoricalDtype([1, 2, 3, 4])})
categorized_df = cat.fit_transform(payment_type_amt_df)
categorized_df.dtypes
payment_type_amt_df.head()
#tag::ex_categorical_variables[]


#tag::ex_dummy_variables[]
from dask_ml.preprocessing import DummyEncoder

dummy = DummyEncoder()
dummified_df = dummy.fit_transform(categorized_df)
dummified_df.dtypes
dummified_df.head()
#tag::ex_dummy_variables[]


poly = PolynomialFeatures(2)


categorized_df.dtypes


categorized_df.head()


payment_type_amt_df


payment_type_amt_df.compute().describe()


pickup_locations_df.dtypes


df[["PULocationID"]].head()


#tag::ex_joblib[]
from dask.distributed import Client
from joblib import parallel_backend

client = Client('127.0.0.1:8786')

X, y = load_my_data()
net = get_that_net()

gs = GridSearchCV(
    net,
    param_grid={'lr': [0.01, 0.03]},
    scoring='accuracy',
)

XGBClassifier()

with parallel_backend('dask'):
    gs.fit(X, y)
print(gs.cv_results_)
#end::ex_joblib


#tag::ex_describe_percentiles[]
import pandas as pd

pd.set_option('display.float_format', lambda x: '%.5f' % x)
df.describe(percentiles=[.25, .5, .75]).compute()
#end::ex_describe_percentiles


# tag::ex_plot_distances[]
import matplotlib.pyplot as plt
import seaborn as sns 
import numpy as np

get_ipython().run_line_magic('matplotlib', 'inline')
sns.set(style="white", palette="muted", color_codes=True)
f, axes = plt.subplots(1, 1, figsize=(11, 7), sharex=True)
sns.despine(left=True)
sns.distplot(
    np.log(
        df['trip_distance'].values +
        1),
    axlabel='Log(trip_distance)',
    label='log(trip_distance)',
    bins=50,
    color="r")
plt.setp(axes, yticks=[])
plt.tight_layout()
plt.show()
#end::ex_plot_distances


# Show that each col is a numpy ndarray. Note how array size is NaN until we call compute.
# chunk sizes compte also shows how this is parallelized.
df['trip_distance'].values.compute_chunk_sizes()


# number of rows
numrows = df.shape[0].compute()
# number of columns
numcols = df.shape[1]
print("Number of rows {} number of columns {}".format(numrows, numcols))


df['trip_duration'] = (
    df['tpep_dropoff_datetime'] -
    df['tpep_pickup_datetime']).map(
        lambda x: x.total_seconds())


df['trip_duration'].describe().compute()


duration_diff = np.abs(df['trip_duration'])


# clean up data as we see some dirty inputs
df = df[df['trip_duration'] <= 10000]
df = df[df['trip_duration'] >= 30]


df['trip_duration'].describe().compute()


# note numpy -> ddf logic is slightly different. eg df[col].values vs df[col]
# visualizing whole dataset is a different fish to fry, we are just
# showing small ones for now.
plt.hist(df['trip_duration'], bins=100)
plt.xlabel('trip_duration')
plt.ylabel('number of records')
plt.show()


df['log_trip_duration'] = np.log(df['trip_duration'])


plt.hist(df['log_trip_duration'], bins=100)
plt.xlabel('log(trip_duration)')
plt.ylabel('number of records')
plt.show()
sns.distplot(df["log_trip_duration"], bins=100)


#tag::ex_dask_random_split[]
from dask_ml.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
    df['trip_distance'], df['total_amount'])
#end::ex_dask_random_split


X_train, X_test, y_train, y_test = train_test_split(
    df[['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
       'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag',
        'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra',
        'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee']],
    df[['total_amount']])


X_train.categorize("VendorID").dtypes


#
# Start the very tedious job of enriching the dataset, pulling features and categories out
# Chain them using dask, delay materialization...
# create dummy var out of labels.
#
# We could've read it at categorical when reading the parquet as specified in dtypes.
# Or we  can do it here.
# unlike pandas, must be categorized before calling dummy.


#tag::ex_categorical_variables_alt[]
train = train.categorize("VendorID")
train = train.categorize("passenger_count")
train = train.categorize("store_and_fwd_flag")

test = test.categorize("VendorID")
test = test.categorize("passenger_count")
test = test.categorize("store_and_fwd_flag")
#end::ex_categorical_variables_alt[]


#tag::ex_datetime_dummy_alt[]
train['Hour'] = train['tpep_pickup_datetime'].dt.hour
test['Hour'] = test['tpep_pickup_datetime'].dt.hour

train['dayofweek'] = train['tpep_pickup_datetime'].dt.dayofweek
test['dayofweek'] = test['tpep_pickup_datetime'].dt.dayofweek

train = train.categorize("dayofweek")
test = test.categorize("dayofweek")

dom_train = dd.get_dummies(
    train,
    columns=['dayofweek'],
    prefix='dom',
    prefix_sep='_')
dom_test = dd.get_dummies(
    test,
    columns=['dayofweek'],
    prefix='dom',
    prefix_sep='_')

hour_train = dd.get_dummies(
    train,
    columns=['dayofweek'],
    prefix='h',
    prefix_sep='_')
hour_test = dd.get_dummies(
    test,
    columns=['dayofweek'],
    prefix='h',
    prefix_sep='_')

dow_train = dd.get_dummies(
    train,
    columns=['dayofweek'],
    prefix='dow',
    prefix_sep='_')
dow_test = dd.get_dummies(
    test,
    columns=['dayofweek'],
    prefix='dow',
    prefix_sep='_')
#end::ex_datetime_dummy_alt[]


#tag::linear_regression[]
from dask_ml.linear_model import LinearRegression
from dask_ml.model_selection import train_test_split

regr_df = df[['trip_distance', 'total_amount']].dropna()
regr_X = regr_df[['trip_distance']]
regr_y = regr_df[['total_amount']]

X_train, X_test, y_train, y_test = train_test_split(
    regr_X, regr_y)

X_train = X_train.to_dask_array(lengths=[100]).compute()
X_test = X_test.to_dask_array(lengths=[100]).compute()
y_train = y_train.to_dask_array(lengths=[100]).compute()
y_test = y_test.to_dask_array(lengths=[100]).compute()

reg = LinearRegression()
reg.fit(X_train, y_train)
y_pred = reg.predict(X_test)

r2_score(y_test, y_pred)

#end::linear_regression[]



#tag::ex_daskml_port[]
from sklearn.linear_model import LinearRegression as ScikitLinearRegression
from sklearn.linear_model import SGDRegressor as ScikitSGDRegressor

estimators = [ScikitLinearRegression(), ScikitSGDRegressor()]
run_tasks = [dask.delayed(estimator.fit)(X_train, y_train)
             for estimator in estimators]
run_tasks
#end::ex_daskml_port[]


vendor_train = dd.get_dummies(
    train,
    columns=["VendorID"],
    prefix='vi',
    prefix_sep='_')
test_train = dd.get_dummies(
    test,
    columns=["VendorID"],
    prefix='vi',
    prefix_sep='_')


#tag::ex_dask_dummy_alt[]
vendor_train = dd.get_dummies(
    train,
    columns=["VendorID"],
    prefix='vi',
    prefix_sep='_')
vendor_test = dd.get_dummies(
    test,
    columns=["VendorID"],
    prefix='vi',
    prefix_sep='_')

passenger_count_train = dd.get_dummies(
    train,
    columns=['passenger_count'],
    prefix='pc',
    prefix_sep='_')
passenger_count_test = dd.get_dummies(
    test,
    columns=['passenger_count'],
    prefix='pc',
    prefix_sep='_')
store_and_fwd_flag_train = dd.get_dummies(
    train,
    columns=['store_and_fwd_flag'],
    prefix='sf',
    prefix_sep='_')
store_and_fwd_flag_test = dd.get_dummies(
    test,
    columns=['store_and_fwd_flag'],
    prefix='sf',
    prefix_sep='_')
#end::ex_dask_dummy_alt[]


# Full list of categorical vars

vendor_train = dd.get_dummies(
    train,
    columns=["VendorID"],
    prefix='vi',
    prefix_sep='_')
vendor_test = dd.get_dummies(
    test,
    columns=["VendorID"],
    prefix='vi',
    prefix_sep='_')

passenger_count_train = dd.get_dummies(
    train,
    columns=['passenger_count'],
    prefix='pc',
    prefix_sep='_')
passenger_count_test = dd.get_dummies(
    test,
    columns=['passenger_count'],
    prefix='pc',
    prefix_sep='_')
store_and_fwd_flag_train = dd.get_dummies(
    train,
    columns=['store_and_fwd_flag'],
    prefix='sf',
    prefix_sep='_')
store_and_fwd_flag_test = dd.get_dummies(
    test,
    columns=['store_and_fwd_flag'],
    prefix='sf',
    prefix_sep='_')

# enrich the datetime into month/ hour / day, and turn it into dummy
train['Month'] = train['tpep_pickup_datetime'].dt.month
test['Month'] = test['tpep_pickup_datetime'].dt.month
# harder way to to the same thing.
# test['Month'] = (test['tpep_pickup_datetime']).map(lambda x: x.month)
train['DayofMonth'] = train['tpep_pickup_datetime'].dt.day
test['DayofMonth'] = test['tpep_pickup_datetime'].dt.day

test.groupby('DayofMonth').count().compute()


test.head()


train['Hour'] = train['tpep_pickup_datetime'].dt.hour
test['Hour'] = test['tpep_pickup_datetime'].dt.hour

train['dayofweek'] = train['tpep_pickup_datetime'].dt.dayofweek
test['dayofweek'] = test['tpep_pickup_datetime'].dt.dayofweek

train = train.categorize("Month")
test = test.categorize("Month")

train = train.categorize("DayofMonth")
test = test.categorize("DayofMonth")

train = train.categorize("dayofweek")
test = test.categorize("dayofweek")

month_train = dd.get_dummies(
    train,
    columns=['dayofweek'],
    prefix='m',
    prefix_sep='_')
month_test = dd.get_dummies(
    test,
    columns=['dayofweek'],
    prefix='m',
    prefix_sep='_')

dom_train = dd.get_dummies(
    train,
    columns=['dayofweek'],
    prefix='dom',
    prefix_sep='_')
dom_test = dd.get_dummies(
    test,
    columns=['dayofweek'],
    prefix='dom',
    prefix_sep='_')

hour_train = dd.get_dummies(
    train,
    columns=['dayofweek'],
    prefix='h',
    prefix_sep='_')
hour_test = dd.get_dummies(
    test,
    columns=['dayofweek'],
    prefix='h',
    prefix_sep='_')

dow_train = dd.get_dummies(
    train,
    columns=['dayofweek'],
    prefix='dow',
    prefix_sep='_')
dow_test = dd.get_dummies(
    test,
    columns=['dayofweek'],
    prefix='dow',
    prefix_sep='_')
# vendor_test = dd.get_dummies(test, columns=["VendorID"], prefix='vi', prefix_sep='_')


# calculate and add average speed col
train['avg_speed_h'] = 1000 * train['trip_distance'] / train['trip_duration']
test['avg_speed_h'] = 1000 * test['trip_distance'] / test['trip_duration']


fig, ax = plt.subplots(ncols=3, sharey=True)
ax[0].plot(
    train.groupby('Hour').avg_speed_h.mean().compute(),
    'bo-',
    lw=2,
    alpha=0.7)
ax[1].plot(
    train.groupby('dayofweek').avg_speed_h.mean().compute(),
    'go-',
    lw=2,
    alpha=0.7)
ax[2].plot(
    train.groupby('Month').avg_speed_h.mean().compute(),
    'ro-',
    lw=2,
    alpha=0.7)
ax[0].set_xlabel('Hour of Day')
ax[1].set_xlabel('Day of Week')
ax[2].set_xlabel('Month of Year')
ax[0].set_ylabel('Average Speed')
fig.suptitle('Average Traffic Speed by Date-part')
plt.show()


train_final = train.drop(['VendorID',
                          'passenger_count',
                          'store_and_fwd_flag',
                          'Month',
                          'DayofMonth',
                          'Hour',
                          'dayofweek'],
                         axis=1)
test_final = test.drop(['VendorID',
                        'passenger_count',
                        'store_and_fwd_flag',
                        'Month',
                        'DayofMonth',
                        'Hour',
                        'dayofweek'],
                       axis=1)
train_final = train_final.drop(
    ['tpep_dropoff_datetime', 'tpep_pickup_datetime', 'trip_duration', 'avg_speed_h'], axis=1)
test_final = test_final.drop(['tpep_dropoff_datetime',
                              'tpep_pickup_datetime',
                              'trip_duration',
                              'avg_speed_h'],
                             axis=1)
X_train = train_final.drop(['log_trip_duration'], axis=1)
Y_train = train_final["log_trip_duration"]
X_test = test_final.drop(['log_trip_duration'], axis=1)
Y_test = test_final["log_trip_duration"]


# #fin enrich / category step


#tag::ex_xgb_basic_usage[]
import xgboost as xgb
from dask_cuda import LocalCUDACluster
from dask.distributed import Client

n_workers = 4
cluster = LocalCUDACluster(n_workers)
client = Client(cluster)

dtrain = xgb.dask.DaskDMatrix(client, X_train, y_train)

booster = xgb.dask.train(
    client,
    {"booster": "gbtree", "verbosity": 2, "nthread": 4, "eta": 0.01, gamma=0,
     "max_depth": 5, "tree_method": "auto", "objective": "reg:squarederror"},
    dtrain,
    num_boost_round=4,
    evals=[(dtrain, "train")])

#end::ex_xgb_basic_usage


# Just like standard xgb Dmatrix, but note that we are explicitly passing
# in columns since we're dealing with Pandas, and that we need to give the
# colnames for xgb to know feature names


#tag::ex_xgb_train_plot_importance[]
import xgboost as xgb

dtrain = xgb.DMatrix(X_train, label=y_train, feature_names=X_train.columns)
dvalid = xgb.DMatrix(X_test, label=y_test, feature_names=X_test.columns)
watchlist = [(dtrain, 'train'), (dvalid, 'valid')]
xgb_pars = {
    'min_child_weight': 1,
    'eta': 0.5,
    'colsample_bytree': 0.9,
    'max_depth': 6,
    'subsample': 0.9,
    'lambda': 1.,
    'nthread': -1,
    'booster': 'gbtree',
    'silent': 1,
    'eval_metric': 'rmse',
    'objective': 'reg:linear'}
model = xgb.train(xgb_pars, dtrain, 10, watchlist, early_stopping_rounds=2,
                  maximize=False, verbose_eval=1)
print('Modeling RMSLE %.5f' % model.best_score)

xgb.plot_importance(model, max_num_features=28, height=0.7)

pred = model.predict(dtest)
pred = np.exp(pred) - 1
#end::ex_xgb_train_plot_importance


#tag::ex_xgb_early_stopping_and_inference[]
import xgboost as xgb
from dask_cuda import LocalCUDACluster
from dask.distributed import Client

n_workers = 4
cluster = LocalCUDACluster(n_workers)
client = Client(cluster)


def fit_model(client, X, y, X_valid, y_valid,
              early_stopping_rounds=5) -> xgb.Booster:
    Xy_valid = dxgb.DaskDMatrix(client, X_valid, y_valid)
    # train the model
    booster = xgb.dask.train(
        client,
        {"booster": "gbtree", "verbosity": 2, "nthread": 4, "eta": 0.01, gamma=0,
         "max_depth": 5, "tree_method": "gpu_hist", "objective": "reg:squarederror"},
        dtrain,
        num_boost_round=500,
        early_stopping_rounds=early_stopping_rounds,
        evals=[(dtrain, "train")])["booster"]
    return booster


def predict(client, model, X):
    predictions = xgb.predict(client, model, X)
    assert isinstance(predictions, dd.Series)
    return predictions

#end::ex_xgb_early_stopping_and_inference


#tag::dask_delayed_load[]
from skimage.io import imread
from skimage.io.collection import alphanumeric_key
from dask import delayed
import dask.array as da
import os

root, dirs, filenames = os.walk(dataset_dir)
# sample first file
imread(filenames[0])


@dask.delayed
def lazy_reader(file):
    return imread(file)


# we have a bunch of delayed readers attached to the files
lazy_arrays = [lazy_reader(file) for file in filenames]

# read individual files from reader into a dask array
# particularly useful if each image is a large file like DICOM radiographs
# mammography dicom tends to be extremely large
dask_arrays = [
    da.from_delayed(delayed_reader, shape=(4608, 5200,), dtype=np.float32)
    for delayed_reader in lazy_arrays
]
# end::dask_delayed_load


#tag::Dask_DataFrame_map_partition_inference[]
import dask.dataframe as dd
import dask.bag as db

def rowwise_operation(row, arg *):
    # row-wise compute
    return result


def partition_operation(df):
    # partition wise logic
    result = df[col1].apply(rowwise_operation)
    return result


ddf = dd.read_csv(“metadata_of_files”)
results = ddf.map_partitions(partition_operation)
results.compute()

# An alternate way, but note the .apply() here becomes a pandas apply, not
# Dask .apply(), and you must define axis = 1
ddf.map_partitions(
    lambda partition: partition.apply(
        lambda row: rowwise_operation(row), axis=1), meta=(
            'ddf', object))

# end::Dask_DataFrame_map_partition_inference


# !pip install dask_sql


#tag::Dask_sql_define_tables[]
import dask.dataframe as dd
import dask.datasets
from dask_sql import Context

# read dataset
taxi_df = dd.read_csv('./data/taxi_train_subset.csv')
taxi_test = dd.read_csv('./data/taxi_test.csv')

# create a context to register tables
c = Context()
c.create_table("taxi_test", taxi_test)
c.create_table("taxicab", taxi_df)
#end::Dask_sql_define_tables[]


# tag::Dask_sql_linear_regression[]
import dask.dataframe as dd
import dask.datasets
from dask_sql import Context

c = Context()
# define model
c.sql(
    """
CREATE MODEL fare_linreg_model WITH (
    model_class = 'LinearRegression',
    wrap_predict = True,
    target_column = 'fare_amount'
) AS (
    SELECT passenger_count, fare_amount
    FROM taxicab
    LIMIT 1000
)
"""
)

# describe model
c.sql(
    """
DESCRIBE MODEL fare_linreg_model
    """
).compute()

# run inference
c.sql(
    """
SELECT
    *
FROM PREDICT(MODEL fare_linreg_model,
    SELECT * FROM taxi_test
)
    """
).compute()
#end::Dask_sql_linear_regression[]


#tag::Dask_sql_XGBClassifier[]
import dask.dataframe as dd
import dask.datasets
from dask_sql import Context

c = Context()
# define model
c.sql(
    """
CREATE MODEL classify_faretype WITH (
    model_class = 'XGBClassifier',
    target_column = 'fare_type'
) AS (
    SELECT airport_surcharge, passenger_count, fare_type
    FROM taxicab
    LIMIT 1000
)
"""
)

# describe model
c.sql(
    """
DESCRIBE MODEL classify_faretype
    """
).compute()

# run inference
c.sql(
    """
SELECT
    *
FROM PREDICT(MODEL classify_faretype,
    SELECT airport_surcharge, passenger_count, FROM taxi_test
)
    """
).compute()
#end::Dask_sql_XGBClassifier[]


#tag::batched_operations[]
def handle_batch(batch, conn, nlp_model):
    # run_inference_here.
    conn.commit()


def handle_partition(df):
    worker = get_worker()
    conn = connect_to_db()
    try:
        nlp_model = worker.roberta_model
    except BaseException:
        nlp_model = load_model()
        worker.nlp_model = nlp_model
    result, batch = [], []
    for _, row in part.iterrows():
        if len(batch) % batch_size == 0 and len(batch) > 0:
            batch_results = handle_batch(batch, conn, nlp_model)
            result.append(batch_results)
            batch = []
        batch.append((row.doc_id, row.sent_id, row.utterance))
    if len(batch) > 0:
        batch_results = handle_batch(batch, conn, nlp_model)
        result.append(batch_results)
    conn.close()
    return result


ddf = dd.read_csv("metadata.csv”)
results = ddf.map_partitions(handle_partition)
results.compute()
#end::batched_operations
