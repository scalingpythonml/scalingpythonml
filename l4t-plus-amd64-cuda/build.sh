set -ex
TAG="a01082023"
CROSS_IMAGE="holdenk/cross-cuda-l4t-new:${TAG}"
RAY_CROSS_IMAGE="holdenk/ray-x86-and-l4t:${TAG}"

docker buildx build --platform=linux/arm64,linux/amd64  -t ${CROSS_IMAGE} . --push
docker buildx build --platform=linux/arm64,linux/amd64 --build-arg="BASE_IMAGE=${CROSS_IMAGE}" -t ${RAY_CROSS_IMAGE} . --push -f RayDockerfile
