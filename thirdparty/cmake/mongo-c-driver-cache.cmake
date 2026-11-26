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

# This initial cache is loaded after -DCMAKE_INSTALL_PREFIX and before upstream configuration.
# Upstream find modules reuse these forced values instead of resolving host libraries or stale caches.
function(doris_mongo_include cache_key header)
    find_path(DORIS_MONGO_INCLUDE NAMES "${header}"
              PATHS "${CMAKE_INSTALL_PREFIX}/include"
              NO_DEFAULT_PATH NO_CACHE REQUIRED)
    set(${cache_key} "${DORIS_MONGO_INCLUDE}" CACHE PATH "Doris MongoDB dependency" FORCE)
endfunction()

function(doris_mongo_library cache_key archive)
    find_library(DORIS_MONGO_LIBRARY NAMES "${archive}"
                 PATHS "${CMAKE_INSTALL_PREFIX}/lib64" "${CMAKE_INSTALL_PREFIX}/lib"
                 NO_DEFAULT_PATH NO_CACHE REQUIRED)
    set(${cache_key} "${DORIS_MONGO_LIBRARY}" CACHE FILEPATH "Doris MongoDB dependency" FORCE)
endfunction()

doris_mongo_include(OPENSSL_INCLUDE_DIR openssl/ssl.h)
doris_mongo_library(OPENSSL_SSL_LIBRARY libssl.a)
doris_mongo_library(OPENSSL_CRYPTO_LIBRARY libcrypto.a)
doris_mongo_include(SASL2_INCLUDE_DIR sasl/sasl.h)
doris_mongo_library(SASL2_LIBRARY libsasl2.a)
doris_mongo_include(SNAPPY_INCLUDE_DIRS snappy-c.h)
doris_mongo_library(SNAPPY_LIBRARIES libsnappy.a)
doris_mongo_include(ZLIB_INCLUDE_DIR zlib.h)
doris_mongo_library(ZLIB_LIBRARY libz.a)
doris_mongo_include(DORIS_MONGO_ZSTD_INCLUDE_DIR zstd.h)
doris_mongo_library(pkgcfg_lib_ZSTD_zstd libzstd.a)
