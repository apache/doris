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

# define COMMON_THIRDPARTY list variable
set(COMMON_THIRDPARTY)

# define add_thirdparty function, append thirdparty libraries to COMMON_THIRDPARTY variable, and pass arg too add_library
# if arg exist lib64, use lib64, else use lib
# if arg exist noadd, not append to COMMON_THIRDPARTY variable
# if arg exist libname, use libname to find library
# if arg exist wholelibpath, use wholelibpath to find library
function(add_thirdparty)
    cmake_parse_arguments(DORIS_THIRDPARTY
        "NOTADD;LIB64"
        "LIBNAME;WHOLELIBPATH"
        ""
        ${ARGN})

    set(DORIS_THIRDPARTY_NAME ${DORIS_THIRDPARTY_UNPARSED_ARGUMENTS})
    add_library(${DORIS_THIRDPARTY_NAME} STATIC IMPORTED)

    if (NOT DORIS_THIRDPARTY_NOTADD)
        set(COMMON_THIRDPARTY ${COMMON_THIRDPARTY} ${DORIS_THIRDPARTY_NAME} PARENT_SCOPE)
    endif()

    if (DORIS_THIRDPARTY_LIB64)
        set(DORIS_THIRDPARTY_LIBPATH ${THIRDPARTY_DIR}/lib64/lib${DORIS_THIRDPARTY_NAME}.a)
    elseif (DORIS_THIRDPARTY_LIBNAME)
        set(DORIS_THIRDPARTY_LIBPATH ${THIRDPARTY_DIR}/${DORIS_THIRDPARTY_LIBNAME})
    elseif (DORIS_THIRDPARTY_WHOLELIBPATH)
        set(DORIS_THIRDPARTY_LIBPATH ${DORIS_THIRDPARTY_WHOLELIBPATH})
    else()
        set(DORIS_THIRDPARTY_LIBPATH ${THIRDPARTY_DIR}/lib/lib${DORIS_THIRDPARTY_NAME}.a)
    endif()
    set_target_properties(${DORIS_THIRDPARTY_NAME} PROPERTIES IMPORTED_LOCATION ${DORIS_THIRDPARTY_LIBPATH})
endfunction()

add_thirdparty(gflags)
add_thirdparty(glog)
add_thirdparty(backtrace)
add_thirdparty(re2)
add_thirdparty(hyperscan LIBNAME "lib64/libhs.a")
add_thirdparty(odbc)
add_thirdparty(pprof WHOLELIBPATH ${GPERFTOOLS_HOME}/lib/libprofiler.a)
add_thirdparty(tcmalloc WHOLELIBPATH ${GPERFTOOLS_HOME}/lib/libtcmalloc.a NOTADD)
add_thirdparty(protobuf)
add_thirdparty(gtest)
add_thirdparty(gtest_main)
add_thirdparty(benchmark)
add_thirdparty(gmock)
add_thirdparty(snappy)
add_thirdparty(curl)
add_thirdparty(lz4)
add_thirdparty(thrift)
add_thirdparty(thriftnb)

if(WITH_LZO)
    add_thirdparty(lzo LIBNAME "lib/liblzo2.a")
endif()

add_thirdparty(libevent LIBNAME "lib/libevent.a")
add_thirdparty(libevent_pthreads LIBNAME "lib/libevent_pthreads.a")
add_thirdparty(libbz2 LIBNAME "lib/libbz2.a")
add_thirdparty(libz LIBNAME "lib/libz.a")
add_thirdparty(crypto)
add_thirdparty(openssl LIBNAME "lib/libssl.a")
add_thirdparty(leveldb)
add_thirdparty(jemalloc LIBNAME "lib/libjemalloc_doris.a")
add_thirdparty(jemalloc_arrow LIBNAME "lib/libjemalloc.a")

if (WITH_MYSQL)
    add_thirdparty(mysql LIBNAME "lib/libmysqlclient.a")
endif()

if (USE_UNWIND)
    add_thirdparty(libunwind LIBNAME "lib64/libunwind.a")
endif()

add_thirdparty(grpc++_reflection LIB64)
add_thirdparty(grpc LIB64)
add_thirdparty(grpc++ LIB64)
add_thirdparty(grpc++_unsecure LIB64)
add_thirdparty(gpr LIB64)
add_thirdparty(upb LIB64)
add_thirdparty(cares LIB64)
add_thirdparty(address_sorting LIB64)
add_thirdparty(z LIB64)

add_thirdparty(brotlicommon LIB64)
add_thirdparty(brotlidec LIB64)
add_thirdparty(brotlienc LIB64)
add_thirdparty(zstd LIB64)
add_thirdparty(arrow LIB64)
add_thirdparty(arrow_flight LIB64)
add_thirdparty(arrow_flight_sql LIB64)
add_thirdparty(parquet LIB64)
add_thirdparty(brpc LIB64)
add_thirdparty(rocksdb)
add_thirdparty(cyrus-sasl LIBNAME "lib/libsasl2.a")
# put this after lz4 to avoid using lz4 lib in librdkafka
add_thirdparty(rdkafka_cpp LIBNAME "lib/librdkafka++.a")
add_thirdparty(rdkafka)
add_thirdparty(s2)
add_thirdparty(bitshuffle)
add_thirdparty(roaring)
add_thirdparty(fmt)
add_thirdparty(cctz)

add_thirdparty(aws-cpp-sdk-core LIB64)
add_thirdparty(aws-cpp-sdk-s3 LIB64)
add_thirdparty(aws-cpp-sdk-transfer LIB64)
add_thirdparty(aws-cpp-sdk-s3-crt LIB64)
add_thirdparty(aws-crt-cpp LIB64)
add_thirdparty(aws-c-cal LIB64)
add_thirdparty(aws-c-auth LIB64)
add_thirdparty(aws-c-compression LIB64)
add_thirdparty(aws-c-common LIB64)
add_thirdparty(aws-c-event-stream LIB64)
add_thirdparty(aws-c-io LIB64)
add_thirdparty(aws-c-http LIB64)
add_thirdparty(aws-c-mqtt LIB64)
add_thirdparty(aws-checksums LIB64)
add_thirdparty(aws-c-s3 LIB64)
add_thirdparty(aws-c-sdkutils LIB64)
if (NOT OS_MACOSX)
    add_thirdparty(aws-s2n LIBNAME "lib/libs2n.a")
endif()

add_thirdparty(minizip LIB64)
add_thirdparty(simdjson LIB64)
add_thirdparty(idn LIB64)
add_thirdparty(opentelemetry_common LIB64)
add_thirdparty(opentelemetry_exporter_zipkin_trace LIB64)
add_thirdparty(opentelemetry_resources LIB64)
add_thirdparty(opentelemetry_version LIB64)
add_thirdparty(opentelemetry_exporter_ostream_span LIB64)
add_thirdparty(opentelemetry_trace LIB64)
add_thirdparty(opentelemetry_http_client_curl LIB64)
add_thirdparty(opentelemetry_exporter_otlp_http LIB64)
add_thirdparty(opentelemetry_exporter_otlp_http_client LIB64)
add_thirdparty(opentelemetry_otlp_recordable LIB64)
add_thirdparty(opentelemetry_proto LIB64)
add_thirdparty(xml2 LIB64)
add_thirdparty(lzma LIB64)
add_thirdparty(gsasl)
add_thirdparty(krb5support)
add_thirdparty(krb5)
add_thirdparty(com_err)
add_thirdparty(k5crypto)
add_thirdparty(gssapi_krb5)
add_thirdparty(dragonbox_to_chars LIB64)
target_include_directories(dragonbox_to_chars INTERFACE "${THIRDPARTY_DIR}/include/dragonbox-1.1.3")

if (OS_MACOSX)
    add_thirdparty(bfd)
    add_thirdparty(iberty)
    add_thirdparty(intl)
endif()
