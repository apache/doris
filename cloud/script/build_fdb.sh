# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#!/bin/bash

set -e

# Default values
VERSION="7.1.55"
BUILD_DIR="./build_output"
DOCKER_IMAGE="foundationdb/build:centos7-latest"
REPO_URL="https://github.com/apple/foundationdb.git"
SRC_DIR="./foundationdb"
UPDATE_REPO=true

# Feature flags (default: all enabled except AWS and Azure backup)
BUILD_AWS_BACKUP=false
BUILD_AZURE_BACKUP=false
USE_JEMALLOC=true
USE_LTO=true
BUILD_DOCUMENTATION=true
WITH_ROCKSDB=true
WITH_GRPC=true
FULL_DEBUG_SYMBOLS=true

# Binding flags (default: only C and Python enabled)
BUILD_C_BINDING=true
BUILD_PYTHON_BINDING=true
BUILD_JAVA_BINDING=false
BUILD_GO_BINDING=false
BUILD_SWIFT_BINDING=false
BUILD_RUBY_BINDING=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print help message
show_help() {
    cat << EOF
Usage: $0 [OPTIONS]

Build FoundationDB using Docker.

Options:
    -v VERSION         FoundationDB version/tag to build (default: 7.1.55)
    -o BUILD_DIR       Output directory for build artifacts (default: ./build_output)
    -i DOCKER_IMAGE    Docker image to use (default: foundationdb/build:centos7-latest)
    -s SRC_DIR         Source directory (default: ./foundationdb)
    -u                 Skip updating repository

Backup Options:
    --aws              Enable AWS backup support
    --azure            Enable Azure backup support

Build Options:
    --no-jemalloc      Disable jemalloc (default: enabled)
    --no-lto           Disable Link Time Optimization (default: enabled)
    --no-debug-syms    Disable full debug symbols (default: enabled)
    --no-docs          Disable documentation (default: enabled)
    --no-rocksdb       Disable RocksDB support (default: enabled)
    --no-grpc          Disable gRPC support (default: enabled)

Binding Options (default: C and Python enabled, others disabled):
    --java             Enable Java binding
    --go               Enable Go binding
    --swift            Enable Swift binding
    --ruby             Enable Ruby binding
    --all-bindings     Enable all bindings
    --minimal-bindings Enable only C binding

Environment Variables:
    FDB_ENABLE_JAVA    Enable Java binding (true/false)
    FDB_ENABLE_GO      Enable Go binding (true/false)
    FDB_ENABLE_SWIFT   Enable Swift binding (true/false)
    FDB_ENABLE_RUBY    Enable Ruby binding (true/false)

General:
    -h                 Show this help message

Examples:
    $0                                          # Build with all features (C + Python bindings)
    $0 -v 7.3.0                                 # Build specific version
    $0 -v 7.3.0 --aws --azure                   # Enable AWS + Azure backup
    $0 --java --go --swift                      # Enable additional bindings
    $0 --all-bindings                           # Enable all bindings
    $0 --no-grpc --no-rocksdb                   # Disable specific features
    $0 --minimal-bindings                       # Only C binding
    FDB_ENABLE_JAVA=true $0                     # Enable Java via env var

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v)
            VERSION="$2"
            shift 2
            ;;
        -o)
            BUILD_DIR="$2"
            shift 2
            ;;
        -i)
            DOCKER_IMAGE="$2"
            shift 2
            ;;
        -s)
            SRC_DIR="$2"
            shift 2
            ;;
        -u)
            UPDATE_REPO=false
            shift
            ;;
        --aws)
            BUILD_AWS_BACKUP=true
            shift
            ;;
        --azure)
            BUILD_AZURE_BACKUP=true
            shift
            ;;
        --no-jemalloc)
            USE_JEMALLOC=false
            shift
            ;;
        --no-lto)
            USE_LTO=false
            shift
            ;;
        --no-debug-syms)
            FULL_DEBUG_SYMBOLS=false
            shift
            ;;
        --no-docs)
            BUILD_DOCUMENTATION=false
            shift
            ;;
        --no-rocksdb)
            WITH_ROCKSDB=false
            shift
            ;;
        --no-grpc)
            WITH_GRPC=false
            shift
            ;;
        --debug-syms)
            FULL_DEBUG_SYMBOLS=true
            shift
            ;;
        --docs)
            BUILD_DOCUMENTATION=true
            shift
            ;;
        --rocksdb)
            WITH_ROCKSDB=true
            shift
            ;;
        --grpc)
            WITH_GRPC=true
            shift
            ;;
        --java)
            BUILD_JAVA_BINDING=true
            shift
            ;;
        --go)
            BUILD_GO_BINDING=true
            shift
            ;;
        --swift)
            BUILD_SWIFT_BINDING=true
            shift
            ;;
        --ruby)
            BUILD_RUBY_BINDING=true
            shift
            ;;
        --all-bindings)
            BUILD_JAVA_BINDING=true
            BUILD_GO_BINDING=true
            BUILD_SWIFT_BINDING=true
            BUILD_RUBY_BINDING=true
            shift
            ;;
        --minimal-bindings)
            BUILD_PYTHON_BINDING=false
            shift
            ;;
        -h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}" >&2
            show_help
            exit 1
            ;;
    esac
done

# Override bindings from environment variables
if [ "$FDB_ENABLE_JAVA" = "true" ]; then
    BUILD_JAVA_BINDING=true
fi
if [ "$FDB_ENABLE_GO" = "true" ]; then
    BUILD_GO_BINDING=true
fi
if [ "$FDB_ENABLE_SWIFT" = "true" ]; then
    BUILD_SWIFT_BINDING=true
fi
if [ "$FDB_ENABLE_RUBY" = "true" ]; then
    BUILD_RUBY_BINDING=true
fi

# Build CMAKE options string
CMAKE_OPTIONS=""

# Base options
CMAKE_OPTIONS+=" -DCMAKE_BUILD_TYPE=Release"
CMAKE_OPTIONS+=" -DFDB_RELEASE=ON"
# CMAKE_OPTIONS+=" -DFORCE_ALL_COMPONENTS=ON"

# Optional features
if [ "$USE_JEMALLOC" = true ]; then
    CMAKE_OPTIONS+=" -DUSE_JEMALLOC=ON"
else
    CMAKE_OPTIONS+=" -DUSE_JEMALLOC=OFF"
fi

if [ "$USE_LTO" = true ]; then
    CMAKE_OPTIONS+=" -DUSE_LTO=ON"
else
    CMAKE_OPTIONS+=" -DUSE_LTO=OFF"
fi

if [ "$FULL_DEBUG_SYMBOLS" = true ]; then
    CMAKE_OPTIONS+=" -DFULL_DEBUG_SYMBOLS=ON"
fi

if [ "$BUILD_DOCUMENTATION" = true ]; then
    CMAKE_OPTIONS+=" -DBUILD_DOCUMENTATION=ON"
fi

if [ "$WITH_ROCKSDB" = true ]; then
    CMAKE_OPTIONS+=" -DWITH_ROCKSDB=ON"
fi

if [ "$WITH_GRPC" = true ]; then
    CMAKE_OPTIONS+=" -DWITH_GRPC=ON"
fi

# Backup options
if [ "$BUILD_AWS_BACKUP" = true ]; then
    CMAKE_OPTIONS+=" -DBUILD_AWS_BACKUP=ON"
fi

if [ "$BUILD_AZURE_BACKUP" = true ]; then
    CMAKE_OPTIONS+=" -DBUILD_AZURE_BACKUP=ON"
fi

# Binding options
if [ "$BUILD_C_BINDING" = true ]; then
    CMAKE_OPTIONS+=" -DBUILD_C_BINDING=ON"
fi

if [ "$BUILD_PYTHON_BINDING" = true ]; then
    CMAKE_OPTIONS+=" -DBUILD_PYTHON_BINDING=ON"
fi

if [ "$BUILD_JAVA_BINDING" = true ]; then
    CMAKE_OPTIONS+=" -DBUILD_JAVA_BINDING=ON"
fi

if [ "$BUILD_GO_BINDING" = true ]; then
    CMAKE_OPTIONS+=" -DBUILD_GO_BINDING=ON"
fi

if [ "$BUILD_SWIFT_BINDING" = true ]; then
    CMAKE_OPTIONS+=" -DBUILD_SWIFT_BINDING=ON"
fi

if [ "$BUILD_RUBY_BINDING" = true ]; then
    CMAKE_OPTIONS+=" -DBUILD_RUBY_BINDING=ON"
fi

# Print configuration
echo -e "${GREEN}=== FoundationDB Build Configuration ===${NC}"
echo -e "${BLUE}Basic Settings:${NC}"
echo "  Version:      $VERSION"
echo "  Build Dir:    $BUILD_DIR"
echo "  Source Dir:   $SRC_DIR"
echo "  Docker Image: $DOCKER_IMAGE"
echo "  Update Repo:  $UPDATE_REPO"
echo ""

echo -e "${BLUE}Enabled Features:${NC}"
echo "  JEMALLOC:       $USE_JEMALLOC"
echo "  LTO:            $USE_LTO"
echo "  Debug Symbols:  $FULL_DEBUG_SYMBOLS"
echo "  Documentation:  $BUILD_DOCUMENTATION"
echo "  RocksDB:        $WITH_ROCKSDB"
echo "  gRPC:           $WITH_GRPC"
echo "  AWS Backup:     $BUILD_AWS_BACKUP"
echo "  Azure Backup:   $BUILD_AZURE_BACKUP"
echo ""

echo -e "${BLUE}Enabled Bindings:${NC}"
echo "  C:      $BUILD_C_BINDING"
echo "  Python: $BUILD_PYTHON_BINDING"
echo "  Java:   $BUILD_JAVA_BINDING"
echo "  Go:     $BUILD_GO_BINDING"
echo "  Swift:  $BUILD_SWIFT_BINDING"
echo "  Ruby:   $BUILD_RUBY_BINDING"
echo ""

echo -e "${BLUE}CMake Options:${NC}"
echo "$CMAKE_OPTIONS" | tr ' ' '\n' | grep -v '^$' | sed 's/^/  /'
echo ""

# Pull Docker image
echo -e "${YELLOW}Pulling Docker image...${NC}"
docker pull "$DOCKER_IMAGE"

# Clone or update repository
if [ -d "$SRC_DIR" ]; then
    echo -e "${YELLOW}Repository exists...${NC}"
    if [ "$UPDATE_REPO" = true ]; then
        echo "Updating repository..."
        cd "$SRC_DIR"
        git fetch origin
        git reset --hard origin/main
        cd ..
    else
        echo "Skipping repository update (-u flag set)"
    fi
else
    echo -e "${YELLOW}Cloning repository...${NC}"
    git clone "$REPO_URL" "$SRC_DIR"
fi

# Checkout specific version/tag
echo -e "${YELLOW}Checking out version: $VERSION${NC}"
cd "$SRC_DIR"
git checkout "$VERSION"
cd ..

# Create build output directory
mkdir -p "$BUILD_DIR"

# Run build in container
echo -e "${YELLOW}Starting build...${NC}"
docker run --rm \
    -v "$(pwd)/$SRC_DIR:/foundationdb/src:ro" \
    -v "$(pwd)/$BUILD_DIR:/foundationdb/build" \
    "$DOCKER_IMAGE" \
    bash -c "
        mkdir -p /foundationdb/build && \
        cd /foundationdb/build && \
        CC=clang CXX=clang++ LD=lld cmake -B build -D USE_LD=LLD -D USE_LIBCXX=1 -G Ninja $CMAKE_OPTIONS /foundationdb/src && \
        ninja -C build
    "

echo ""
echo -e "${GREEN}=== Build Complete! ===${NC}"
echo "Artifacts location: $(pwd)/$BUILD_DIR"
