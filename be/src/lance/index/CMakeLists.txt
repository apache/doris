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
# specific language governing permissions and
# limitations under the License.

# The Lance index worker components, modeled on the storage/index/ann
# precedent: built as their own static library and linked into doris_be
# through DORIS_LINK_LIBS.
#   * lance_index_worker_frame: the internal pipe frame codec - uint32
#     big-endian length prefix + thrift-compact struct (LanceIndexWorker.thrift),
#     depth/container-limited decoding, incremental assembly - shared by the
#     worker library and the supervisor.
#   * lance_index_contract: schema contract v1 recomputation and ordered
#     comparison against the FE's durable contract representation; a pure
#     library with zero BE global-state dependency (no config, logging,
#     metrics, or ExecEnv), callable from the worker subprocess and from unit
#     tests alike.
#   * lance_index_worker: the isolated worker execution library — one dispatch
#     frame in, handshake + at most one result frame out, pinned-version
#     reopen, contract/IF/DROP physical-snapshot semantics, single native
#     call. Registered as the --lance-worker subcommand of doris_main (before
#     any BE global state); the supervisor (a later commit) execs it via
#     /proc/self/exe.
#   * (later commits) lance_index_supervisor / lance_index_worker_service:
#     the in-BE supervisor and the thrift-facing service.

file(GLOB LANCE_SOURCES CONFIGURE_DEPENDS *.cpp)

add_library(Lance STATIC ${LANCE_SOURCES})

pch_reuse(Lance)
