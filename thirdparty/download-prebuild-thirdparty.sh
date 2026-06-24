#!/bin/bash
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

################################################################
# This script will download all thirdparties and java libraries
# which are defined in *vars.sh*, unpack patch them if necessary.
# You can run this script multi-times.
# Things will only be downloaded, unpacked and patched once.
################################################################

set -eo pipefail

VERSION="$1"

if [ -z "$VERSION" ]; then
  echo "Usage: sh download-prebuild-thirdparty.sh <version>"
  exit 1
fi

# ----------------------------
# Detect OS
# ----------------------------
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
  Darwin)
    PLATFORM="darwin"
    ;;
  Linux)
    PLATFORM="linux"
    ;;
  *)
    echo "Unsupported OS: $OS"
    exit 1
    ;;
esac

# ----------------------------
# Detect ARCH
# ----------------------------
case "$ARCH" in
  x86_64|amd64)
    ARCH="x86_64"
    ;;
  arm64|aarch64)
    ARCH="arm64"
    ;;
  *)
    echo "Unsupported architecture: $ARCH"
    exit 1
    ;;
esac

# ----------------------------
# Resolve base release tag
# ----------------------------
case "$VERSION" in
  master|4.0)
    RELEASE_TAG="automation"
    ;;
  3.1)
    RELEASE_TAG="automation-3.1"
    ;;
  3.0)
    RELEASE_TAG="automation-3.0"
    ;;
  2.1)
    RELEASE_TAG="automation-2.1"
    ;;
  *)
    echo "Unsupported version: $VERSION"
    exit 1
    ;;
esac

# ----------------------------
# Resolve filename
# ----------------------------
FILENAME=""

if [ "$PLATFORM" = "darwin" ]; then
  FILENAME="doris-thirdparty-prebuilt-darwin-${ARCH}.tar.xz"
else
  if [ "$ARCH" = "arm64" ]; then
    case "$VERSION" in
      master|4.0)
        FILENAME="doris-thirdparty-prebuild-arm64.tar.xz"
        ;;
      3.1)
        FILENAME="doris-thirdparty-3.1-prebuild-arm64.tar.xz"
        ;;
      3.0)
        FILENAME="doris-thirdparty-3.0-prebuild-arm64.tar.xz"
        ;;
      2.1)
        FILENAME="doris-thirdparty-2.1-prebuild-arm64.tar.xz"
        ;;
    esac
  else
    FILENAME="doris-thirdparty-prebuilt-linux-x86_64.tar.xz"
  fi
fi

# ----------------------------
# Final URL
# ----------------------------
URL="https://github.com/apache/doris-thirdparty/releases/download/${RELEASE_TAG}/${FILENAME}"

echo "Detected platform : $PLATFORM"
echo "Detected arch     : $ARCH"
echo "Version           : $VERSION"
echo "Downloading       : $URL"
echo

# ----------------------------
# Download
# ----------------------------
if command -v curl >/dev/null 2>&1; then
  curl -fL -o "$FILENAME" "$URL"
elif command -v wget >/dev/null 2>&1; then
  wget -O "$FILENAME" "$URL"
else
  echo "Error: curl or wget is required"
  exit 1
fi

echo
echo "Download completed:"
echo "  $(pwd)/$FILENAME"

