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

# Set all libraries

add_library(gflags STATIC IMPORTED)
set_target_properties(gflags PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgflags.a)

add_library(glog STATIC IMPORTED)
set_target_properties(glog PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libglog.a)

add_library(backtrace STATIC IMPORTED)
set_target_properties(backtrace PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libbacktrace.a)

add_library(re2 STATIC IMPORTED)
set_target_properties(re2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libre2.a)

add_library(hyperscan STATIC IMPORTED)
set_target_properties(hyperscan PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libhs.a)

add_library(odbc STATIC IMPORTED)
set_target_properties(odbc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libodbc.a)

add_library(pprof STATIC IMPORTED)
set_target_properties(pprof PROPERTIES IMPORTED_LOCATION
    ${GPERFTOOLS_HOME}/lib/libprofiler.a)

add_library(tcmalloc STATIC IMPORTED)
set_target_properties(tcmalloc PROPERTIES IMPORTED_LOCATION
    ${GPERFTOOLS_HOME}/lib/libtcmalloc.a)

add_library(protobuf STATIC IMPORTED)
set_target_properties(protobuf PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libprotobuf.a)

add_library(protoc STATIC IMPORTED)
set_target_properties(protoc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libprotoc.a)

add_library(gtest STATIC IMPORTED)
set_target_properties(gtest PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgtest.a)

add_library(gtest_main STATIC IMPORTED)
set_target_properties(gtest_main PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgtest_main.a)

add_library(benchmark STATIC IMPORTED)
set_target_properties(benchmark PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libbenchmark.a)

add_library(gmock STATIC IMPORTED)
set_target_properties(gmock PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgmock.a)

add_library(snappy STATIC IMPORTED)
set_target_properties(snappy PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libsnappy.a)

add_library(curl STATIC IMPORTED)
set_target_properties(curl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libcurl.a)

add_library(lz4 STATIC IMPORTED)
set_target_properties(lz4 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/liblz4.a)

add_library(thrift STATIC IMPORTED)
set_target_properties(thrift PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libthrift.a)

add_library(thriftnb STATIC IMPORTED)
set_target_properties(thriftnb PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libthriftnb.a)

if(WITH_LZO)
    add_library(lzo STATIC IMPORTED)
    set_target_properties(lzo PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/liblzo2.a)
endif()

if (WITH_MYSQL)
    add_library(mysql STATIC IMPORTED)
    set_target_properties(mysql PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libmysqlclient.a)
endif()

add_library(libevent STATIC IMPORTED)
set_target_properties(libevent PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libevent.a)

add_library(libevent_pthreads STATIC IMPORTED)
set_target_properties(libevent_pthreads PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libevent_pthreads.a)

add_library(libbz2 STATIC IMPORTED)
set_target_properties(libbz2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libbz2.a)

add_library(libz STATIC IMPORTED)
set_target_properties(libz PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libz.a)

add_library(crypto STATIC IMPORTED)
set_target_properties(crypto PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libcrypto.a)

add_library(openssl STATIC IMPORTED)
set_target_properties(openssl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libssl.a)

add_library(leveldb STATIC IMPORTED)
set_target_properties(leveldb PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libleveldb.a)

add_library(jemalloc STATIC IMPORTED)
set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libjemalloc_doris.a)

add_library(jemalloc_arrow STATIC IMPORTED)
set_target_properties(jemalloc_arrow PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libjemalloc.a)

if (USE_UNWIND)
    add_library(libunwind STATIC IMPORTED)
    set_target_properties(libunwind PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libunwind.a)
endif()

add_library(brotlicommon STATIC IMPORTED)
set_target_properties(brotlicommon PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbrotlicommon.a)

add_library(brotlidec STATIC IMPORTED)
set_target_properties(brotlidec PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbrotlidec.a)

add_library(brotlienc STATIC IMPORTED)
set_target_properties(brotlienc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbrotlienc.a)

add_library(zstd STATIC IMPORTED)
set_target_properties(zstd PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libzstd.a)

add_library(arrow STATIC IMPORTED)
set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libarrow.a)

add_library(parquet STATIC IMPORTED)
set_target_properties(parquet PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libparquet.a)

add_library(brpc STATIC IMPORTED)
set_target_properties(brpc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbrpc.a)

add_library(rocksdb STATIC IMPORTED)
set_target_properties(rocksdb PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/librocksdb.a)

add_library(cyrus-sasl STATIC IMPORTED)
set_target_properties(cyrus-sasl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libsasl2.a)

add_library(librdkafka_cpp STATIC IMPORTED)
set_target_properties(librdkafka_cpp PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/librdkafka++.a)

add_library(librdkafka STATIC IMPORTED)
set_target_properties(librdkafka PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/librdkafka.a)

add_library(libs2 STATIC IMPORTED)
set_target_properties(libs2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libs2.a)

add_library(bitshuffle STATIC IMPORTED)
set_target_properties(bitshuffle PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libbitshuffle.a)

add_library(roaring STATIC IMPORTED)
set_target_properties(roaring PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libroaring.a)

add_library(fmt STATIC IMPORTED)
set_target_properties(fmt PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libfmt.a)

add_library(cctz STATIC IMPORTED)
set_target_properties(cctz PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libcctz.a)

add_library(aws-sdk-core STATIC IMPORTED)
set_target_properties(aws-sdk-core PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-cpp-sdk-core.a)

add_library(aws-sdk-s3 STATIC IMPORTED)
set_target_properties(aws-sdk-s3 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-cpp-sdk-s3.a)

add_library(aws-sdk-transfer STATIC IMPORTED)
set_target_properties(aws-sdk-transfer PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-cpp-sdk-transfer.a)

add_library(aws-sdk-s3-crt STATIC IMPORTED)
set_target_properties(aws-sdk-s3-crt PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-cpp-sdk-s3-crt.a)


add_library(aws-crt-cpp STATIC IMPORTED)
set_target_properties(aws-crt-cpp PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-crt-cpp.a)

add_library(aws-c-cal STATIC IMPORTED)
set_target_properties(aws-c-cal PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-cal.a)

add_library(aws-c-auth STATIC IMPORTED)
set_target_properties(aws-c-auth PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-auth.a)

add_library(aws-c-compression STATIC IMPORTED)
set_target_properties(aws-c-compression PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-compression.a)

add_library(aws-c-common STATIC IMPORTED)
set_target_properties(aws-c-common PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-common.a)

add_library(aws-c-event-stream STATIC IMPORTED)
set_target_properties(aws-c-event-stream PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-event-stream.a)

add_library(aws-c-io STATIC IMPORTED)
set_target_properties(aws-c-io PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-io.a)

add_library(aws-c-http STATIC IMPORTED)
set_target_properties(aws-c-http PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-http.a)

add_library(aws-c-mqtt STATIC IMPORTED)
set_target_properties(aws-c-mqtt PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-mqtt.a)

add_library(aws-checksums STATIC IMPORTED)
set_target_properties(aws-checksums PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-checksums.a)

add_library(aws-c-s3 STATIC IMPORTED)
set_target_properties(aws-c-s3 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-s3.a)

add_library(aws-c-sdkutils STATIC IMPORTED)
set_target_properties(aws-c-sdkutils PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-sdkutils.a)

if (NOT OS_MACOSX)
    add_library(aws-s2n STATIC IMPORTED)
    set_target_properties(aws-s2n PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libs2n.a)
endif()

add_library(minizip STATIC IMPORTED)
set_target_properties(minizip PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libminizip.a)

add_library(simdjson STATIC IMPORTED)
set_target_properties(simdjson PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libsimdjson.a)

add_library(idn STATIC IMPORTED)
set_target_properties(idn PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libidn.a)

add_library(opentelemetry_common STATIC IMPORTED)
set_target_properties(opentelemetry_common PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_common.a)

add_library(opentelemetry_exporter_zipkin_trace STATIC IMPORTED)
set_target_properties(opentelemetry_exporter_zipkin_trace PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_exporter_zipkin_trace.a)

add_library(opentelemetry_resources STATIC IMPORTED)
set_target_properties(opentelemetry_resources PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_resources.a)

add_library(opentelemetry_version STATIC IMPORTED)
set_target_properties(opentelemetry_version PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_version.a)

add_library(opentelemetry_exporter_ostream_span STATIC IMPORTED)
set_target_properties(opentelemetry_exporter_ostream_span PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_exporter_ostream_span.a)

add_library(opentelemetry_trace STATIC IMPORTED)
set_target_properties(opentelemetry_trace PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_trace.a)

add_library(opentelemetry_http_client_curl STATIC IMPORTED)
set_target_properties(opentelemetry_http_client_curl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_http_client_curl.a)

add_library(opentelemetry_exporter_otlp_http STATIC IMPORTED)
set_target_properties(opentelemetry_exporter_otlp_http PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_exporter_otlp_http.a)

add_library(opentelemetry_exporter_otlp_http_client STATIC IMPORTED)
set_target_properties(opentelemetry_exporter_otlp_http_client PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_exporter_otlp_http_client.a)

add_library(opentelemetry_otlp_recordable STATIC IMPORTED)
set_target_properties(opentelemetry_otlp_recordable PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_otlp_recordable.a)

add_library(opentelemetry_proto STATIC IMPORTED)
set_target_properties(opentelemetry_proto PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_proto.a)

add_library(xml2 STATIC IMPORTED)
set_target_properties(xml2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libxml2.a)

add_library(lzma STATIC IMPORTED)
set_target_properties(lzma PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/liblzma.a)

add_library(gsasl STATIC IMPORTED)
set_target_properties(gsasl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgsasl.a)

add_library(krb5support STATIC IMPORTED)
set_target_properties(krb5support PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libkrb5support.a)

add_library(krb5 STATIC IMPORTED)
set_target_properties(krb5 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libkrb5.a)

add_library(com_err STATIC IMPORTED)
set_target_properties(com_err PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libcom_err.a)

add_library(k5crypto STATIC IMPORTED)
set_target_properties(k5crypto PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libk5crypto.a)

add_library(gssapi_krb5 STATIC IMPORTED)
set_target_properties(gssapi_krb5 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgssapi_krb5.a)

find_program(THRIFT_COMPILER thrift ${CMAKE_SOURCE_DIR}/bin)

if (OS_MACOSX)
    add_library(bfd STATIC IMPORTED)
    set_target_properties(bfd PROPERTIES IMPORTED_LOCATION "${THIRDPARTY_DIR}/lib/libbfd.a")

    add_library(iberty STATIC IMPORTED)
    set_target_properties(iberty PROPERTIES IMPORTED_LOCATION "${THIRDPARTY_DIR}/lib/libiberty.a")

    add_library(intl STATIC IMPORTED)
    set_target_properties(intl PROPERTIES IMPORTED_LOCATION "${THIRDPARTY_DIR}/lib/libintl.a")
endif()
