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

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export DORIS_HOME=${ROOT}

. ${DORIS_HOME}/env.sh

PARALLEL=$[$(nproc)/4+1]

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --clean    clean and build ut
     --run    build and run ut

  Eg.
    $0                      build ut
    $0 --run                build and run ut
    $0 --clean              clean and build ut
    $0 --clean --run        clean, build and run ut
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'run' \
  -l 'clean' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

CLEAN=
RUN=
if [ $# == 1 ] ; then
    #default
    CLEAN=0
    RUN=0
else
    CLEAN=0
    RUN=0
    while true; do 
        case "$1" in
            --clean) CLEAN=1 ; shift ;;
            --run) RUN=1 ; shift ;;
            --) shift ;  break ;;
            *) ehco "Internal error" ; exit 1 ;;
        esac
    done
fi

echo "Build Backend UT"

if [ ${CLEAN} -eq 1 ]; then
    rm ${DORIS_HOME}/be/build/ -rf
    rm ${DORIS_HOME}/be/output/ -rf
fi

if [ ! -d ${DORIS_HOME}/be/build ]; then
    mkdir -p ${DORIS_HOME}/be/build/
fi

cd ${DORIS_HOME}/be/build/

cmake ../ -DWITH_MYSQL=OFF -DMAKE_TEST=ON
make -j${PARALLEL}

if [ ${RUN} -ne 1 ]; then
    echo "Finished"
    exit 0
fi

echo "******************************"
echo "    Running PaloBe Unittest    "
echo "******************************"

cd ${DORIS_HOME}
export DORIS_TEST_BINARY_DIR=${DORIS_HOME}/be/build
export TERM=xterm
export UDF_RUNTIME_DIR=${DORIS_HOME}/lib/udf-runtime
export LOG_DIR=${DORIS_HOME}/log
for i in `sed 's/ //g' $DORIS_HOME/conf/be.conf | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*="`; do
    eval "export $i";
done

mkdir -p $LOG_DIR
mkdir -p ${UDF_RUNTIME_DIR}
rm -f ${UDF_RUNTIME_DIR}/*

if [ ${RUN} -ne 1 ]; then
    echo "Finished"
    exit 0
fi

echo "******************************"
echo "    Running PaloBe Unittest    "
echo "******************************"

export DORIS_TEST_BINARY_DIR=${DORIS_TEST_BINARY_DIR}/test/

# prepare util test_data
if [ -d ${DORIS_TEST_BINARY_DIR}/util/test_data ]; then
    rm -rf ${DORIS_TEST_BINARY_DIR}/util/test_data
fi
cp -r ${DORIS_HOME}/be/test/util/test_data ${DORIS_TEST_BINARY_DIR}/util/

# Running Util Unittest
${DORIS_TEST_BINARY_DIR}/util/bit_util_test
${DORIS_TEST_BINARY_DIR}/util/bitmap_test
${DORIS_TEST_BINARY_DIR}/util/path_trie_test
${DORIS_TEST_BINARY_DIR}/util/count_down_latch_test
${DORIS_TEST_BINARY_DIR}/util/lru_cache_util_test
${DORIS_TEST_BINARY_DIR}/util/filesystem_util_test
${DORIS_TEST_BINARY_DIR}/util/internal_queue_test
${DORIS_TEST_BINARY_DIR}/util/cidr_test
${DORIS_TEST_BINARY_DIR}/util/new_metrics_test
${DORIS_TEST_BINARY_DIR}/util/doris_metrics_test
${DORIS_TEST_BINARY_DIR}/util/system_metrics_test
${DORIS_TEST_BINARY_DIR}/util/core_local_test
${DORIS_TEST_BINARY_DIR}/util/arena_test
${DORIS_TEST_BINARY_DIR}/util/types_test
${DORIS_TEST_BINARY_DIR}/util/json_util_test
${DORIS_TEST_BINARY_DIR}/util/byte_buffer_test2
${DORIS_TEST_BINARY_DIR}/util/uid_util_test
${DORIS_TEST_BINARY_DIR}/util/aes_util_test
${DORIS_TEST_BINARY_DIR}/util/string_util_test
${DORIS_TEST_BINARY_DIR}/util/coding_test
${DORIS_TEST_BINARY_DIR}/util/faststring_test
${DORIS_TEST_BINARY_DIR}/util/tdigest_test

## Running common Unittest
${DORIS_TEST_BINARY_DIR}/common/resource_tls_test

## Running exprs unit test
${DORIS_TEST_BINARY_DIR}/exprs/string_functions_test
${DORIS_TEST_BINARY_DIR}/exprs/json_function_test
${DORIS_TEST_BINARY_DIR}/exprs/timestamp_functions_test
${DORIS_TEST_BINARY_DIR}/exprs/percentile_approx_test

## Running geo unit test
${DORIS_TEST_BINARY_DIR}/geo/geo_functions_test
${DORIS_TEST_BINARY_DIR}/geo/wkt_parse_test
${DORIS_TEST_BINARY_DIR}/geo/geo_types_test

## Running exec unit test
${DORIS_TEST_BINARY_DIR}/exec/plain_text_line_reader_uncompressed_test
${DORIS_TEST_BINARY_DIR}/exec/plain_text_line_reader_gzip_test
${DORIS_TEST_BINARY_DIR}/exec/plain_text_line_reader_bzip_test
${DORIS_TEST_BINARY_DIR}/exec/plain_text_line_reader_lz4frame_test
if [ -f ${DORIS_TEST_BINARY_DIR}/exec/plain_text_line_reader_lzop_test ];then
    ${DORIS_TEST_BINARY_DIR}/exec/plain_text_line_reader_lzop_test
fi
${DORIS_TEST_BINARY_DIR}/exec/broker_scanner_test
${DORIS_TEST_BINARY_DIR}/exec/parquet_scanner_test
${DORIS_TEST_BINARY_DIR}/exec/broker_scan_node_test
${DORIS_TEST_BINARY_DIR}/exec/es_scan_node_test
${DORIS_TEST_BINARY_DIR}/exec/es_http_scan_node_test
${DORIS_TEST_BINARY_DIR}/exec/es_predicate_test
${DORIS_TEST_BINARY_DIR}/exec/es_scan_reader_test
${DORIS_TEST_BINARY_DIR}/exec/es_query_builder_test
${DORIS_TEST_BINARY_DIR}/exec/olap_table_info_test
${DORIS_TEST_BINARY_DIR}/exec/olap_table_sink_test

## Running runtime Unittest
${DORIS_TEST_BINARY_DIR}/runtime/fragment_mgr_test
${DORIS_TEST_BINARY_DIR}/runtime/decimal_value_test
${DORIS_TEST_BINARY_DIR}/runtime/datetime_value_test
${DORIS_TEST_BINARY_DIR}/runtime/large_int_value_test
${DORIS_TEST_BINARY_DIR}/runtime/string_value_test
${DORIS_TEST_BINARY_DIR}/runtime/free_list_test
${DORIS_TEST_BINARY_DIR}/runtime/string_buffer_test
${DORIS_TEST_BINARY_DIR}/runtime/stream_load_pipe_test
${DORIS_TEST_BINARY_DIR}/runtime/tablet_writer_mgr_test
${DORIS_TEST_BINARY_DIR}/runtime/snapshot_loader_test
${DORIS_TEST_BINARY_DIR}/runtime/user_function_cache_test
${DORIS_TEST_BINARY_DIR}/runtime/small_file_mgr_test
# Running expr Unittest

# Running http
${DORIS_TEST_BINARY_DIR}/http/metrics_action_test
${DORIS_TEST_BINARY_DIR}/http/http_utils_test
${DORIS_TEST_BINARY_DIR}/http/stream_load_test
${DORIS_TEST_BINARY_DIR}/http/http_client_test

# Running OLAPEngine Unittest
${DORIS_TEST_BINARY_DIR}/olap/bit_field_test
${DORIS_TEST_BINARY_DIR}/olap/byte_buffer_test
${DORIS_TEST_BINARY_DIR}/olap/run_length_byte_test
${DORIS_TEST_BINARY_DIR}/olap/run_length_integer_test
${DORIS_TEST_BINARY_DIR}/olap/stream_index_test
${DORIS_TEST_BINARY_DIR}/olap/lru_cache_test
${DORIS_TEST_BINARY_DIR}/olap/bloom_filter_test
${DORIS_TEST_BINARY_DIR}/olap/bloom_filter_index_test
${DORIS_TEST_BINARY_DIR}/olap/row_block_test
${DORIS_TEST_BINARY_DIR}/olap/comparison_predicate_test
${DORIS_TEST_BINARY_DIR}/olap/in_list_predicate_test
${DORIS_TEST_BINARY_DIR}/olap/null_predicate_test
${DORIS_TEST_BINARY_DIR}/olap/file_helper_test
${DORIS_TEST_BINARY_DIR}/olap/file_utils_test
${DORIS_TEST_BINARY_DIR}/olap/delete_handler_test
${DORIS_TEST_BINARY_DIR}/olap/column_reader_test
${DORIS_TEST_BINARY_DIR}/olap/row_cursor_test
${DORIS_TEST_BINARY_DIR}/olap/skiplist_test
${DORIS_TEST_BINARY_DIR}/olap/serialize_test
${DORIS_TEST_BINARY_DIR}/olap/olap_header_manager_test
${DORIS_TEST_BINARY_DIR}/olap/olap_meta_test
${DORIS_TEST_BINARY_DIR}/olap/delta_writer_test
${DORIS_TEST_BINARY_DIR}/olap/field_info_test
${DORIS_TEST_BINARY_DIR}/olap/rowset/segment_v2/encoding_info_test
${DORIS_TEST_BINARY_DIR}/olap/rowset/segment_v2/ordinal_page_index_test
${DORIS_TEST_BINARY_DIR}/olap/rowset/segment_v2/bitshuffle_page_test
${DORIS_TEST_BINARY_DIR}/olap/rowset/segment_v2/plain_page_test
${DORIS_TEST_BINARY_DIR}/olap/rowset/segment_v2/binary_plain_page_test
${DORIS_TEST_BINARY_DIR}/olap/rowset/segment_v2/column_reader_writer_test
${DORIS_TEST_BINARY_DIR}/olap/rowset/segment_v2/rle_page_test

# Running routine load test
${DORIS_TEST_BINARY_DIR}/runtime/kafka_consumer_pipe_test
${DORIS_TEST_BINARY_DIR}/runtime/routine_load_task_executor_test

# Running agent unittest
# Prepare agent testdata
if [ -d ${DORIS_TEST_BINARY_DIR}/agent/test_data ]; then
    rm -rf ${DORIS_TEST_BINARY_DIR}/agent/test_data
fi
cp -r ${DORIS_HOME}/be/test/agent/test_data ${DORIS_TEST_BINARY_DIR}/agent/
cd ${DORIS_TEST_BINARY_DIR}/agent
# ./agent_server_test
# ./file_downloader_test
#./heartbeat_server_test
#./pusher_test
./utils_test
#./task_worker_pool_test
