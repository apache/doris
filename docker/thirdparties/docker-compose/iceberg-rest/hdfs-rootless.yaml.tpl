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

# Use distinct real host ports so HDFS block locations work for host clients
# as well as the REST catalog containers. Requires rootless Docker >= 29.5.
services:
  hdfs-namenode:
    network_mode: host
    networks: !reset []
    ports: !reset []
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://${LOCAL_IP}:${HDFS_NAMENODE_RPC_PORT}
      - HDFS_CONF_dfs_namenode_rpc___address=${LOCAL_IP}:${HDFS_NAMENODE_RPC_PORT}
      - HDFS_CONF_dfs_namenode_http___address=0.0.0.0:${HDFS_NAMENODE_WEBUI_PORT}
    healthcheck:
      test: ["CMD", "curl", "-f", "http://127.0.0.1:${HDFS_NAMENODE_WEBUI_PORT}/"]

  hdfs-datanode:
    network_mode: host
    networks: !reset []
    ports: !reset []
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://${LOCAL_IP}:${HDFS_NAMENODE_RPC_PORT}
      - HDFS_CONF_dfs_datanode_hostname=${LOCAL_IP}
      - HDFS_CONF_dfs_datanode_address=0.0.0.0:20866
      - HDFS_CONF_dfs_datanode_ipc_address=0.0.0.0:20867
      - HDFS_CONF_dfs_datanode_http_address=0.0.0.0:${HDFS_DATANODE_WEBUI_PORT}
      - SERVICE_PRECONDITION=${LOCAL_IP}:${HDFS_NAMENODE_WEBUI_PORT}
    healthcheck:
      test: ["CMD", "curl", "-f", "http://127.0.0.1:${HDFS_DATANODE_WEBUI_PORT}/"]
