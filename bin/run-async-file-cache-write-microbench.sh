#!/usr/bin/env bash
#
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

set -eo pipefail

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
doris_home="$(cd "${curdir}/.." && pwd)"
benchmark="${doris_home}/lib/async_file_cache_write_microbench"
if [[ ! -x "${benchmark}" ]]; then
    benchmark="${doris_home}/output/be/lib/async_file_cache_write_microbench"
fi
if [[ ! -x "${benchmark}" ]]; then
    echo "Cannot find async_file_cache_write_microbench under ${doris_home}." >&2
    exit 1
fi

cache_path="./output/async_file_cache_write_microbench"
benchmark_args=("$@")
for ((argument_index = 0; argument_index < ${#benchmark_args[@]}; ++argument_index)); do
    argument="${benchmark_args[argument_index]}"
    case "${argument}" in
    --cache_path=*)
        cache_path="${argument#*=}"
        ;;
    --cache_path)
        argument_index=$((argument_index + 1))
        if [[ "${argument_index}" -ge "${#benchmark_args[@]}" ]]; then
            echo "--cache_path requires a value." >&2
            exit 1
        fi
        cache_path="${benchmark_args[argument_index]}"
        ;;
    esac
done
cache_path="${cache_path%/}"
if [[ -z "${cache_path}" ]]; then
    echo "--cache_path cannot be empty or root." >&2
    exit 1
fi

run_fio="${RUN_FIO:-auto}"
case "${run_fio}" in
auto)
    if command -v fio &>/dev/null; then
        run_fio=1
    else
        run_fio=0
        echo "DISK_BASELINE skipped: fio is not installed."
    fi
    ;;
1 | true | on)
    if ! command -v fio &>/dev/null; then
        echo "RUN_FIO=${run_fio}, but fio is not installed." >&2
        exit 1
    fi
    run_fio=1
    ;;
0 | false | off)
    run_fio=0
    ;;
*)
    echo "RUN_FIO must be auto, 1, or 0." >&2
    exit 1
    ;;
esac

if [[ "${run_fio}" -eq 1 ]]; then
    fio_size="${FIO_SIZE:-1G}"
    fio_runtime="${FIO_RUNTIME:-5}"
    if [[ ! "${fio_runtime}" =~ ^[1-9][0-9]*$ ]]; then
        echo "FIO_RUNTIME must be a positive integer number of seconds." >&2
        exit 1
    fi

    cache_parent="$(dirname "${cache_path}")"
    mkdir -p "${cache_parent}"
    fio_dir="$(mktemp -d "${cache_path}_fio.XXXXXX")"
    echo "DISK_BASELINE path=${fio_dir} size=${fio_size} runtime_seconds=${fio_runtime}"
    df -hT "${fio_dir}"

    fio --name=seqwrite_qd1 \
        --filename="${fio_dir}/seqwrite_qd1.data" \
        --size="${fio_size}" \
        --runtime="${fio_runtime}" \
        --ramp_time=1 \
        --time_based=1 \
        --rw=write \
        --bs=1m \
        --ioengine=libaio \
        --iodepth=1 \
        --numjobs=1 \
        --direct=1 \
        --invalidate=1 \
        --refill_buffers=1 \
        --group_reporting=1 \
        --eta=never \
        --unlink=1

    fio --name=randwrite_qd16 \
        --filename="${fio_dir}/randwrite_qd16.data" \
        --size="${fio_size}" \
        --runtime="${fio_runtime}" \
        --ramp_time=1 \
        --time_based=1 \
        --rw=randwrite \
        --bs=1m \
        --ioengine=libaio \
        --iodepth=16 \
        --numjobs=1 \
        --direct=1 \
        --invalidate=1 \
        --refill_buffers=1 \
        --randrepeat=1 \
        --group_reporting=1 \
        --eta=never \
        --unlink=1

    rmdir "${fio_dir}"
fi

if [[ -n "${JAVA_HOME:-}" && -d "${JAVA_HOME}/lib/server" ]]; then
    export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH:-}"
fi
exec "${benchmark}" "$@"
