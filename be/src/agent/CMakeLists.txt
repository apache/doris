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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/agent")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/agent")

set(AGENT_SOURCES
    agent_server.cpp
    heartbeat_server.cpp
    task_worker_pool.cpp
    utils.cpp
    cgroups_mgr.cpp
    topic_subscriber.cpp
    user_resource_listener.cpp
)

if (OS_MACOSX)
    list(REMOVE_ITEM AGENT_SOURCES cgroups_mgr.cpp user_resource_listener.cpp)
    list(APPEND AGENT_SOURCES cgroups_mgr_mac.cpp)
endif()

add_library(Agent STATIC ${AGENT_SOURCES})
