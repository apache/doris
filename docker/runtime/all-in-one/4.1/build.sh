#!/usr/bin/env bash
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
#
# Builds the Doris 4.1.x all-in-one images locally.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../../../.." && pwd)"
DOCKERFILE="docker/runtime/all-in-one/4.1/Dockerfile"

IMAGE="${IMAGE:-apache/doris}"
VERSION=""
SOURCE="image"
FLAVORS=""
STRIP_BE="debug"
PLATFORM=""
LOCAL_OUTPUT="output"
TARBALL_DIR=""
RUN_TEST=false
NO_CACHE=false
PUSH=false

usage() {
    cat <<'USAGE'
Usage: ./build.sh -v <doris-version> [options]

Builds one container image per flavor. Both flavors share every layer up to the
artifact stage, so building the second one after the first is cheap.

  base   internal tables, Hive, Iceberg (incl. system tables), Paimon,
         JDBC catalogs, external-table writeback, Java UDF   -> :all-in-one-<v>
  full   the above plus Hudi, Trino connector, MaxCompute    -> :all-in-one-<v>-full

Options:
  -v, --version <v>     Doris version, e.g. 4.1.3. Required.
  -f, --flavor <f>      base | full | both        (default: both)
  -s, --source <s>      image | local | tarball   (default: image)
                          image   -> apache/doris:fe-<v> and :be-<v>
                          local   -> ./output/{fe,be} from a local build
                          tarball -> --tarball-dir
      --tarball-dir <d> Extracted release package holding fe/ and be/,
                        as a path relative to the repository root.
      --local-output <d> Override the ./output path for --source local.
      --strip <mode>    debug | full | none       (default: debug)
                          debug -> strip --strip-debug, keeps .symtab
                          full  -> strip -s
                          none  -> ship doris_be as-is (+1.8 GB)
      --platform <p>    Target platform(s), e.g. linux/amd64 or
                        linux/amd64,linux/arm64. Comma-separated values produce
                        one multi-arch tag (an OCI image index) that resolves to
                        the right image per host, the way apache/doris:fe-* does.
                        Keeping a multi-platform result locally needs the
                        containerd image store; otherwise pass --push.
      --push            Push instead of loading into the local image store.
  -i, --image <name>    Image name without tag    (default: apache/doris)
      --no-cache        Pass --no-cache to docker build.
  -t, --test            Run resource/smoke-test.sh against each image built.
  -h, --help            This message.

Examples:
  ./build.sh -v 4.1.3                     # both flavors from the official images
  ./build.sh -v 4.1.3 -f base -t          # base only, then smoke test it
  ./build.sh -v dev -s local -f full      # from a local ./output
  ./build.sh -v 4.1.3 --platform linux/amd64,linux/arm64 --push
                                          # one multi-arch tag for both

Building a foreign architecture goes through emulation, and the two apt layers
dominate: roughly 18 minutes per foreign arch on an Apple Silicon host, plus the
one-off pull of that architecture's be image. Where both architectures matter,
building each one natively and joining them afterwards is far faster:

  # on an x86 host
  ./build.sh -v 4.1.3 --push -i myrepo/doris --platform linux/amd64   # tag it -amd64 by hand
  # on an arm host
  ./build.sh -v 4.1.3 --push -i myrepo/doris --platform linux/arm64   # tag it -arm64 by hand
  # then, anywhere
  docker buildx imagetools create -t myrepo/doris:all-in-one-4.1.3 \
      myrepo/doris:all-in-one-4.1.3-amd64 myrepo/doris:all-in-one-4.1.3-arm64
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -v|--version)      VERSION="$2"; shift 2 ;;
        -f|--flavor)       FLAVORS="$2"; shift 2 ;;
        -s|--source)       SOURCE="$2"; shift 2 ;;
        --tarball-dir)     TARBALL_DIR="$2"; shift 2 ;;
        --local-output)    LOCAL_OUTPUT="$2"; shift 2 ;;
        --strip)           STRIP_BE="$2"; shift 2 ;;
        --platform)        PLATFORM="$2"; shift 2 ;;
        -i|--image)        IMAGE="$2"; shift 2 ;;
        --no-cache)        NO_CACHE=true; shift ;;
        --push)            PUSH=true; shift ;;
        -t|--test)         RUN_TEST=true; shift ;;
        -h|--help)         usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

[[ -n "${VERSION}" ]] || { echo "error: -v/--version is required" >&2; usage; exit 1; }

case "${FLAVORS:-both}" in
    both|"") FLAVORS="base full" ;;
    base)    FLAVORS="base" ;;
    full)    FLAVORS="full" ;;
    *) echo "error: bad --flavor '${FLAVORS}', expected base|full|both" >&2; exit 1 ;;
esac

case "${SOURCE}" in
    image|local|tarball) ;;
    *) echo "error: bad --source '${SOURCE}'" >&2; exit 1 ;;
esac
case "${STRIP_BE}" in
    debug|full|none) ;;
    *) echo "error: bad --strip '${STRIP_BE}'" >&2; exit 1 ;;
esac

if [[ "${SOURCE}" == tarball && -z "${TARBALL_DIR}" ]]; then
    echo "error: --source tarball needs --tarball-dir" >&2; exit 1
fi
if [[ "${SOURCE}" == local && ! -d "${REPO_ROOT}/${LOCAL_OUTPUT}/be" ]]; then
    echo "error: ${REPO_ROOT}/${LOCAL_OUTPUT}/be not found; build Doris first" >&2; exit 1
fi

command -v docker >/dev/null || { echo "error: docker not found" >&2; exit 1; }

# A comma in --platform means one tag carrying an OCI image index. Docker can
# only hold that locally with the containerd image store; the classic store has
# nowhere to put a second architecture, so the build has to go straight to a
# registry.
MULTI_PLATFORM=false
case "${PLATFORM}" in *,*) MULTI_PLATFORM=true ;; esac
if [[ "${MULTI_PLATFORM}" == true && "${PUSH}" != true ]]; then
    if ! docker info --format '{{range .DriverStatus}}{{.}}{{end}}' 2>/dev/null | grep -q containerd; then
        echo "error: a multi-platform build needs --push, or the containerd image store" >&2
        echo "       (Docker Desktop: Settings > General > Use containerd for pulling and storing images)" >&2
        exit 1
    fi
fi
if [[ "${MULTI_PLATFORM}" == true && "${RUN_TEST}" == true ]]; then
    echo "error: --test cannot run against a multi-platform tag; build one platform at a time" >&2
    exit 1
fi

builder="docker buildx build"
docker buildx version >/dev/null 2>&1 || builder="docker build"

# Say which platforms are being built. Leaving --platform unset means the host
# architecture only, which is easy to mistake for a multi-arch build.
if [[ -n "${PLATFORM}" ]]; then
    platform_note="${PLATFORM}"
else
    platform_note="$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}' 2>/dev/null || echo host)"
    platform_note="${platform_note} (host only -- pass --platform for multi-arch)"
fi

echo "repository root : ${REPO_ROOT}"
echo "doris version   : ${VERSION}"
echo "artifact source : ${SOURCE}"
echo "strip mode      : ${STRIP_BE}"
echo "flavors         : ${FLAVORS}"
echo "platform(s)     : ${platform_note}"
echo "output          : $([[ "${PUSH}" == true ]] && echo 'push to registry' || echo 'load into local image store')"
echo

built_tags=()

for flavor in ${FLAVORS}; do
    suffix=""
    [[ "${flavor}" == full ]] && suffix="-full"
    tag="${IMAGE}:all-in-one-${VERSION}${suffix}"

    args=(
        --build-arg "DORIS_VERSION=${VERSION}"
        --build-arg "ARTIFACT_SOURCE=${SOURCE}"
        --build-arg "FLAVOR=${flavor}"
        --build-arg "STRIP_BE=${STRIP_BE}"
        --build-arg "LOCAL_OUTPUT=${LOCAL_OUTPUT}"
    )
    [[ -n "${TARBALL_DIR}" ]] && args+=(--build-arg "TARBALL_DIR=${TARBALL_DIR}")
    [[ -n "${PLATFORM}" ]] && args+=(--platform "${PLATFORM}")
    [[ "${NO_CACHE}" == true ]] && args+=(--no-cache)
    if [[ "${PUSH}" == true ]]; then
        args+=(--push)
    elif [[ "${builder}" == "docker buildx build" ]]; then
        # buildx leaves the result in the build cache by default; --load puts it
        # in the local image store where docker run can see it.
        args+=(--load)
    fi

    echo ">>> building ${tag} (flavor=${flavor})"
    DOCKER_BUILDKIT=1 ${builder} "${args[@]}" \
        -f "${DOCKERFILE}" -t "${tag}" "${REPO_ROOT}"

    built_tags+=("${tag}:${flavor}")
    echo
done

echo "=== built images ==="
if [[ "${PUSH}" == true ]]; then
    for entry in "${built_tags[@]}"; do
        tag="${entry%:*}"
        echo "  pushed ${tag}"
        docker buildx imagetools inspect "${tag}" 2>/dev/null \
            | grep -E "^(Name|MediaType)|Platform:" | sed 's/^/    /' || true
    done
    exit 0
fi
# Reported by docker image inspect. Note that `docker images` can print a much
# larger number when the containerd image store is enabled: it adds the
# compressed blobs to the unpacked snapshot instead of reporting one of them.
for entry in "${built_tags[@]}"; do
    tag="${entry%:*}"
    bytes="$(docker image inspect "${tag}" --format '{{.Size}}' 2>/dev/null || echo 0)"
    awk -v t="${tag}" -v b="${bytes}" \
        'BEGIN { printf "  %-42s %.2f GiB\n", t, b / 1073741824 }'
done

if [[ "${RUN_TEST}" == true ]]; then
    for entry in "${built_tags[@]}"; do
        tag="${entry%:*}"
        flavor="${entry##*:}"
        echo
        echo ">>> smoke test ${tag} (${flavor})"
        "${HERE}/resource/smoke-test.sh" "${tag}" "${flavor}"
    done
fi
