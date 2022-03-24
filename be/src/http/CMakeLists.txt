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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/http")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/http")

add_library(Webserver STATIC
  http_headers.cpp
  http_method.cpp
  http_request.cpp
  http_channel.cpp
  http_status.cpp
  http_parser.cpp
  web_page_handler.cpp
  default_path_handlers.cpp
  utils.cpp
  ev_http_server.cpp
  http_client.cpp
  action/download_action.cpp
  action/mini_load.cpp
  action/monitor_action.cpp
  action/health_action.cpp
  action/tablet_migration_action.cpp
  action/tablets_info_action.cpp
  action/tablets_distribution_action.cpp
  action/checksum_action.cpp
  action/snapshot_action.cpp
  action/reload_tablet_action.cpp
  action/restore_tablet_action.cpp
  action/pprof_actions.cpp
  action/metrics_action.cpp
  action/stream_load.cpp
  action/stream_load_2pc.cpp
  action/meta_action.cpp
  action/compaction_action.cpp
  action/config_action.cpp
  action/check_rpc_channel_action.cpp
  action/reset_rpc_channel_action.cpp
)
