#/bin/bash
set -ex

docker buildx build -t holdenk/dask-extended  --platform linux/arm64,linux/amd64 --push . -f Dockerfile
docker buildx build -t holdenk/dask-extended-notebook  --platform linux/arm64,linux/amd64 --push . -f NotebookDockerfile
