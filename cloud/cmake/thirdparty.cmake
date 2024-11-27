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

add_thirdparty(glog)
add_thirdparty(gflags)
add_thirdparty(backtrace)
add_thirdparty(pprof WHOLELIBPATH ${GPERFTOOLS_HOME}/lib/libprofiler.a)
add_thirdparty(protobuf)
add_thirdparty(thrift)
add_thirdparty(crypto)
add_thirdparty(openssl LIBNAME "lib/libssl.a")
if (USE_JEMALLOC)
    add_thirdparty(jemalloc LIBNAME "lib/libjemalloc_doris.a")
else()
    add_thirdparty(tcmalloc WHOLELIBPATH ${GPERFTOOLS_HOME}/lib/libtcmalloc.a NOTADD)
endif()
add_thirdparty(leveldb) # Required by brpc
add_thirdparty(brpc LIB64)
add_thirdparty(rocksdb) # For local storage mocking
add_thirdparty(libz LIBNAME "lib/libz.a") # Required by google::protobuf
add_thirdparty(curl)
add_thirdparty(zstd LIB64)
add_thirdparty(fmt)
# begin aws libs
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
add_thirdparty(aws-s2n LIBNAME "lib/libs2n.a")
# end aws libs
add_thirdparty(jsoncpp LIB64)
add_thirdparty(uuid LIB64)
add_thirdparty(ali-sdk LIBNAME "lib64/libalibabacloud-sdk-core.a")
# begin krb5 libs
add_thirdparty(krb5support)
add_thirdparty(krb5)
add_thirdparty(com_err)
add_thirdparty(gssapi_krb5)
add_thirdparty(k5crypto)
add_thirdparty(xml2 LIB64)
add_thirdparty(lzma LIB64)
add_thirdparty(idn LIB64)
add_thirdparty(gsasl)
# end krb5 libs
# begin azure libs
if(BUILD_AZURE STREQUAL "ON")
    add_thirdparty(azure-core)
    add_thirdparty(azure-identity)
    add_thirdparty(azure-storage-blobs)
    add_thirdparty(azure-storage-common)
endif()
# end azure libs

add_thirdparty(gtest NOTADD)
add_thirdparty(gtest_main NOTADD)
add_thirdparty(gmock NOTADD)
