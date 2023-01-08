#!/bin/bash

set -ex

# Accelerator libraries often involve a lot of native code so it's
# faster to install with conda.
conda install -c numba numba -y
conda install -c numba roctools -y
conda install -c numba cudatoolkit -y
# A lot of GPU acceleration libraries are in the rapidsai channel
# these are not installable with pip
conda install -c rapidsai cudf
conda install -c rapidsai blazingsql
