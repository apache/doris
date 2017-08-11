# Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/rpc")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/rpc")

add_library(RPC STATIC
    checksum.cpp
    comm.cpp
    comm_address.cpp
    comm_header.cpp
    connection_manager.cpp
    dispatch_handler_synchronizer.cpp
    error.cpp
    event.cpp
    fast_clock.cpp
    file_utils.cpp
    handler_map.cpp
    inet_addr.cpp
    io_handler.cpp
    io_handler_accept.cpp
    io_handler_data.cpp
    io_handler_datagram.cpp
    io_handler_raw.cpp
    poll_event.cpp
    protocol.cpp
    proxy_map.cpp
    random.cpp
    reactor.cpp
    reactor_factory.cpp
    reactor_runner.cpp
    request_cache.cpp
    response_callback.cpp
    util.cpp
)
