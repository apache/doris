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

# The Lance index worker execution library: a pure library with zero BE
# global-state dependency (no config, logging, metrics, or ExecEnv), callable
# from a bare main and from unit tests. Modeled on the storage/index/ann
# precedent: built as its own static library and linked into doris_be through
# DORIS_LINK_LIBS.

file(GLOB LANCE_SOURCES CONFIGURE_DEPENDS *.cpp)

add_library(Lance STATIC ${LANCE_SOURCES})

pch_reuse(Lance)
