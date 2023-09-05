#!/usr/bin/env bash

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR=${ROOT}
source "${CURDIR}/../conf/cluster.conf"

CLICKBENCH_DATA_DIR="${DATA_SAVE_DIR}"

if [ "_${CLICKBENCH_DATA_DIR}" == "_clickbench-data" ];then
    CLICKBENCH_DATA_DIR="$CURDIR/../bin/${DATA_SAVE_DIR}"
fi

mkdir -p $CLICKBENCH_DATA_DIR
echo "CLICKBENCH_DATA_DIR: ${CLICKBENCH_DATA_DIR}"

wget_pids=()

function download() {
    cd ${CLICKBENCH_DATA_DIR}
    for i in $(seq 0 9); do
        if [ ! -f "$CLICKBENCH_DATA_DIR/hits_split${i}" ]; then
            echo "will download hits_split${i} to $CLICKBENCH_DATA_DIR"
            wget --continue "https://doris-test-data.oss-cn-hongkong.aliyuncs.com/ClickBench/hits_split${i}" &
            # wget --continue "https://doris-test-data.oss-cn-hongkong-internal.aliyuncs.com/ClickBench/hits_split${i}" &
            PID=$!
            wget_pids[${#wget_pids[@]}]=$PID
        fi
    done
    echo "wait for download task done..."
    wait
    cd -
}


function signal_handler() {

    for PID in ${wget_pids[@]}; do
        kill -9 $PID
    done
}

trap signal_handler 2 3 6 15

echo "start..."
start=$(date +%s)
download
end=$(date +%s)
echo "download cost time: $((end - start)) seconds"