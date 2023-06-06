from dask_kubernetes.operator import KubeCluster

cluster = KubeCluster(name='simple',
                      n_workers=1,
                      resources={
                          "requests": {"memory": "16Gi"},
                          "limits": {"memory": "16Gi"}
                      })

cluster.add_worker_group(name="highmem",
                         n_workers=0,
                         resources={
                             "requests": {"memory": "64Gi"},
                             "limits": {"memory": "64Gi"}
                         })

cluster.add_worker_group(name="gpu",
                         n_workers=0,
                         resources={
                             "requests": {"nvidia.com/gpu": "1"},
                             "limits": {"nvidia.com/gpu": "1"}
                         })
# Now you can scale these worker groups up and down as needed
cluster.scale("gpu", 5, worker_group="gpu")
# Fancy machine learning logic
cluster.scale("gpu", , worker_group="gpu")
# Or just auto-scale
cluster.adapt(minimum=1, maximum=10)

