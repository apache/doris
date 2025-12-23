#!/usr/bin/env bash
# Build image and run container to build Doris BE
set -euo pipefail
REPO_DIR=/home/zhisheng/doris
IMAGE_NAME=doris-builder:latest
DOCKERFILE=${REPO_DIR}/Dockerfile.doris
PROXY_HOST=${PROXY_HOST:-}
PROXY_PORT=${PROXY_PORT:-}

if [ ! -f "$DOCKERFILE" ]; then
  echo "Dockerfile not found at $DOCKERFILE"
  exit 1
fi

# Build image (may require sudo)
if ! docker image inspect "$IMAGE_NAME" >/dev/null 2>&1; then
  echo "Building Docker image $IMAGE_NAME (may ask for sudo)..."
  sudo docker build -f "$DOCKERFILE" -t "$IMAGE_NAME" "$REPO_DIR"
else
  echo "Image $IMAGE_NAME already exists"
fi

# Compose proxy env if provided
if [ -n "$PROXY_HOST" ] && [ -n "$PROXY_PORT" ]; then
  HTTP_PROXY="http://$PROXY_HOST:$PROXY_PORT"
  HTTPS_PROXY="$HTTP_PROXY"
  NO_PROXY="localhost,127.0.0.1,$PROXY_HOST"
else
  HTTP_PROXY=""
  HTTPS_PROXY=""
  NO_PROXY=""
fi

# Run container as root to avoid permission issues and build thirdparty+be
sudo docker run --rm -it \
  --user root \
  -v "$REPO_DIR":/work \
  -w /work \
  -e HTTP_PROXY="$HTTP_PROXY" -e http_proxy="$HTTP_PROXY" \
  -e HTTPS_PROXY="$HTTPS_PROXY" -e https_proxy="$HTTPS_PROXY" \
  -e NO_PROXY="$NO_PROXY" -e no_proxy="$NO_PROXY" \
  -e JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
  -e DORIS_BUILD_PYTHON_VERSION=python3 \
  $IMAGE_NAME \
  bash -lc "git config --global --add safe.directory /work && \
    git config --global http.proxy '$HTTP_PROXY' || true && \
    git config --global https.proxy '$HTTP_PROXY' || true && \
    export http_proxy='$HTTP_PROXY' https_proxy='$HTTP_PROXY' no_proxy='$NO_PROXY' && \
    if [ -x ./thirdparty/build-thirdparty.sh ]; then ./thirdparty/build-thirdparty.sh --clean --parallel \\$(nproc) 2>&1 | tee build_thirdparty.log; else ./build.sh --thirdparty 2>&1 | tee build_thirdparty.log; fi && \
    ls -lh thirdparty/installed/lib64/libaws-cpp-sdk-core.a || true && \
    rm -rf be/build_Release && export CC=/usr/bin/gcc CXX=/usr/bin/g++ DORIS_TOOLCHAIN=gcc DORIS_GCC_HOME=/usr && \
    ./build.sh --be 2>&1 | tee build_be.log"
