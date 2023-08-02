# We need to use l4t for the arm image but it's not cross-build so use regular cuda for amd64

FROM ubuntu:20.04 as base

FROM nvidia/cuda:11.8.0-devel-ubuntu20.04 AS base-amd64
ENV NVARCH x86_64
ARG DEBIAN_FRONTEND=noninteractive

RUN export CU_MINOR=$(nvcc --version |grep "cuda_" |cut -d "_" -f 2 |cut -d "." -f 2)

ENV PIP_EXTRA "--pre --extra-index-url https://download.pytorch.org/whl/cu11${CU_MINOR}"
RUN export PIP_EXTRA="--pre --extra-index-url https://download.pytorch.org/whl/cu11${CU_MINOR}"


#FROM nvcr.io/nvidia/l4t-base:r32.6.1 as base-arm64
FROM nvcr.io/nvidia/l4t-ml:r35.2.1-py3 as base-arm64
#FROM mdegans/l4t-base:xavier as base-arm64
#FROM nvcr.io/nvidia/l4t-tensorflow:r32.7.1-tf2.7-py3 as base-arm64
ARG DEBIAN_FRONTEND=noninteractive

ENV NVARCH sbsa
# See https://forums.developer.nvidia.com/t/numpy-throwing-illegal-instruction-core-dumped-error/184030
ENV OPENBLAS_CORETYPE ARMV8

RUN export CU_MINOR=$(nvcc --version |grep "cuda_" |cut -d "_" -f 2 |cut -d "." -f 2)

RUN mv /etc/apt/sources.list.d/nvidia-l4t-apt-source.list /etc/apt/ || echo "k"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates
RUN mv /etc/apt/nvidia-l4t-apt-source.list /etc/apt/sources.list.d || echo "k..."
RUN apt-get update && apt-get upgrade -y
ENV PIP_EXTRA "--pre --extra-index-url https://developer.download.nvidia.com/compute/redist/jp/v51/"
RUN export PIP_EXTRA="--pre --extra-index-url https://developer.download.nvidia.com/compute/redist/jp/v51/"


FROM base-${TARGETARCH}
ARG TARGETARCH
ARG DEBIAN_FRONTEND=noninteractive
ARG PYTHON_VERSION=3.8

RUN export ARG HOSTTYPE=${HOSTTYPE:-x86_64}
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates python3-dev \
    && apt install -y software-properties-common \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean



COPY setup.sh .
RUN ./setup.sh
