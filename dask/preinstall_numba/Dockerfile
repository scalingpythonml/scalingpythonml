# Use the Dask base image, for arm64 though we have to use custom built
# FROM ghcr.io/dask/dask
FROM holdenk/dask:latest

# arm64 channel
RUN conda config --add channels rpi
# numba and conda-forge channels
RUN conda config --add channels numba
RUN conda config --add channels conda-forge
# some CUDA specific stuff
RUN conda config --add channels rapidsai
# Accelerator libraries often involve a lot of native code so it's
# faster to install with conda.
RUN conda install numba -y
# GPU support (NV)
RUN conda install cudatoolkit -y
# GPU support (AMD)
RUN conda install roctools -y || echo "No roc tools on $(uname -a)"
# A lot of GPU acceleration libraries are in the rapidsai channel
# these are not installable with pip
RUN conda install cudf -y

