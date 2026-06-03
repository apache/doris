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

# thrift — hand-written add_library (no add_subdirectory)
set(_THRIFT_SRC "${TP_SOURCE_DIR}/thrift-0.16.0")
set(_THRIFT_CPP "${_THRIFT_SRC}/lib/cpp/src")
set(_THRIFT_GEN "${CMAKE_CURRENT_BINARY_DIR}/thrift_gen")
file(MAKE_DIRECTORY "${_THRIFT_GEN}/thrift")

# Generate thrift/config.h (pre-configured for Linux x86_64)
set(PACKAGE "thrift")
set(PACKAGE_BUGREPORT "")
set(PACKAGE_NAME "Apache Thrift")
set(PACKAGE_TARNAME "thrift")
set(PACKAGE_URL "")
set(PACKAGE_VERSION "0.16.0")
set(PACKAGE_STRING "thrift 0.16.0")
set(HAVE_ARPA_INET_H 1)
set(HAVE_FCNTL_H 1)
set(HAVE_INTTYPES_H 1)
set(HAVE_NETDB_H 1)
set(HAVE_NETINET_IN_H 1)
set(HAVE_SIGNAL_H 1)
set(HAVE_STDINT_H 1)
set(HAVE_UNISTD_H 1)
set(HAVE_PTHREAD_H 1)
set(HAVE_SYS_IOCTL_H 1)
set(HAVE_SYS_PARAM_H 1)
set(HAVE_SYS_RESOURCE_H 1)
set(HAVE_SYS_SOCKET_H 1)
set(HAVE_SYS_STAT_H 1)
set(HAVE_SYS_UN_H 1)
set(HAVE_POLL_H 1)
set(HAVE_SYS_POLL_H 1)
set(HAVE_SYS_SELECT_H 1)
set(HAVE_SYS_TIME_H 1)
set(HAVE_SCHED_H 1)
set(HAVE_STRINGS_H 1)
set(HAVE_GETHOSTBYNAME 1)
set(HAVE_GETHOSTBYNAME_R 1)
set(HAVE_STRERROR_R 1)
set(HAVE_SCHED_GET_PRIORITY_MAX 1)
set(HAVE_SCHED_GET_PRIORITY_MIN 1)
set(STRERROR_R_CHAR_P 1)
configure_file(
    "${_THRIFT_SRC}/build/cmake/config.h.in"
    "${_THRIFT_GEN}/thrift/config.h"
)

# ============================================================================
# Source files — from upstream lib/cpp/CMakeLists.txt
# ============================================================================

set(thriftcpp_SOURCES
    ${_THRIFT_CPP}/thrift/TApplicationException.cpp
    ${_THRIFT_CPP}/thrift/TOutput.cpp
    ${_THRIFT_CPP}/thrift/async/TAsyncChannel.cpp
    ${_THRIFT_CPP}/thrift/async/TAsyncProtocolProcessor.cpp
    ${_THRIFT_CPP}/thrift/async/TConcurrentClientSyncInfo.cpp
    ${_THRIFT_CPP}/thrift/concurrency/ThreadManager.cpp
    ${_THRIFT_CPP}/thrift/concurrency/TimerManager.cpp
    ${_THRIFT_CPP}/thrift/processor/PeekProcessor.cpp
    ${_THRIFT_CPP}/thrift/protocol/TBase64Utils.cpp
    ${_THRIFT_CPP}/thrift/protocol/TDebugProtocol.cpp
    ${_THRIFT_CPP}/thrift/protocol/TJSONProtocol.cpp
    ${_THRIFT_CPP}/thrift/protocol/TMultiplexedProtocol.cpp
    ${_THRIFT_CPP}/thrift/protocol/TProtocol.cpp
    ${_THRIFT_CPP}/thrift/transport/TTransportException.cpp
    ${_THRIFT_CPP}/thrift/transport/TFDTransport.cpp
    ${_THRIFT_CPP}/thrift/transport/TSimpleFileTransport.cpp
    ${_THRIFT_CPP}/thrift/transport/THttpTransport.cpp
    ${_THRIFT_CPP}/thrift/transport/THttpClient.cpp
    ${_THRIFT_CPP}/thrift/transport/THttpServer.cpp
    ${_THRIFT_CPP}/thrift/transport/TSocket.cpp
    ${_THRIFT_CPP}/thrift/transport/TSocketPool.cpp
    ${_THRIFT_CPP}/thrift/transport/TServerSocket.cpp
    ${_THRIFT_CPP}/thrift/transport/TTransportUtils.cpp
    ${_THRIFT_CPP}/thrift/transport/TBufferTransports.cpp
    ${_THRIFT_CPP}/thrift/transport/SocketCommon.cpp
    ${_THRIFT_CPP}/thrift/server/TConnectedClient.cpp
    ${_THRIFT_CPP}/thrift/server/TServerFramework.cpp
    ${_THRIFT_CPP}/thrift/server/TSimpleServer.cpp
    ${_THRIFT_CPP}/thrift/server/TThreadPoolServer.cpp
    ${_THRIFT_CPP}/thrift/server/TThreadedServer.cpp
    # non-WINCE (Linux)
    ${_THRIFT_CPP}/thrift/transport/TPipe.cpp
    ${_THRIFT_CPP}/thrift/transport/TPipeServer.cpp
    ${_THRIFT_CPP}/thrift/transport/TFileTransport.cpp
    # Unix-only
    ${_THRIFT_CPP}/thrift/VirtualProfiling.cpp
    ${_THRIFT_CPP}/thrift/server/TServer.cpp
    # OpenSSL
    ${_THRIFT_CPP}/thrift/transport/TSSLSocket.cpp
    ${_THRIFT_CPP}/thrift/transport/TSSLServerSocket.cpp
    ${_THRIFT_CPP}/thrift/transport/TWebSocketServer.cpp
)

set(thriftcpp_threads_SOURCES
    ${_THRIFT_CPP}/thrift/concurrency/ThreadFactory.cpp
    ${_THRIFT_CPP}/thrift/concurrency/Thread.cpp
    ${_THRIFT_CPP}/thrift/concurrency/Monitor.cpp
    ${_THRIFT_CPP}/thrift/concurrency/Mutex.cpp
)

set(thriftcppnb_SOURCES
    ${_THRIFT_CPP}/thrift/server/TNonblockingServer.cpp
    ${_THRIFT_CPP}/thrift/transport/TNonblockingServerSocket.cpp
    ${_THRIFT_CPP}/thrift/async/TEvhttpServer.cpp
    ${_THRIFT_CPP}/thrift/async/TEvhttpClientChannel.cpp
    # OpenSSL
    ${_THRIFT_CPP}/thrift/transport/TNonblockingSSLServerSocket.cpp
)

# ============================================================================
# thrift_static (main C++ library)
# ============================================================================
add_library(thrift_static STATIC ${thriftcpp_SOURCES} ${thriftcpp_threads_SOURCES})

target_include_directories(thrift_static PUBLIC
    ${_THRIFT_CPP}
    ${_THRIFT_GEN}
)
target_include_directories(thrift_static PRIVATE
    ${TP_SOURCE_DIR}/boost_1_81_0
)

target_compile_definitions(thrift_static PUBLIC THRIFT_STATIC_DEFINE)

target_link_libraries(thrift_static PUBLIC
    Threads::Threads
    openssl
    crypto
)

# libevent include dirs for thrift (it includes event2/*.h internally)
target_include_directories(thrift_static PRIVATE
    ${TP_SOURCE_DIR}/libevent-release-2.1.12-stable/include
    ${CMAKE_CURRENT_BINARY_DIR}/libevent/include
    ${CMAKE_CURRENT_BINARY_DIR}/openssl/include
)

set_target_properties(thrift_static PROPERTIES OUTPUT_NAME thrift)
add_library(thrift ALIAS thrift_static)

# ============================================================================
# thriftnb_static (non-blocking library, depends on libevent)
# ============================================================================
add_library(thriftnb_static STATIC ${thriftcppnb_SOURCES})

target_include_directories(thriftnb_static PUBLIC
    ${_THRIFT_CPP}
)
target_include_directories(thriftnb_static PRIVATE
    ${TP_SOURCE_DIR}/boost_1_81_0
    ${TP_SOURCE_DIR}/libevent-release-2.1.12-stable/include
    ${CMAKE_CURRENT_BINARY_DIR}/libevent/include
    ${CMAKE_CURRENT_BINARY_DIR}/openssl/include
)

target_compile_definitions(thriftnb_static PUBLIC THRIFT_STATIC_DEFINE)

target_link_libraries(thriftnb_static PUBLIC
    thrift_static
    event_core_static
    event_extra_static
)

set_target_properties(thriftnb_static PROPERTIES OUTPUT_NAME thriftnb)
add_library(thriftnb ALIAS thriftnb_static)
