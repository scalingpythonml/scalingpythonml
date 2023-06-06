#!/bin/bash
#tag::install_helm[]

#end::install_helm[]
#tag::install_operator[]
# Add the repo
helm repo add dask https://helm.dask.org
helm repo update
# Install the operator, you will use this to create clusters
helm install --create-namespace -n \
     dask-operator --generate-name dask/dask-kubernetes-operator
#end::install_operator[]
