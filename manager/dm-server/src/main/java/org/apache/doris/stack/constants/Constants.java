// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.stack.constants;

public class Constants {

    public static final String KEY_DORIS_AGENT_START_SCRIPT = "bin/agent_start.sh";
    public static final String KEY_FE_QUERY_PORT = "query_port";
    public static final String KEY_FE_EDIT_LOG_PORT = "edit_log_port";
    public static final String KEY_BE_HEARTBEAT_PORT = "heartbeat_service_port";
    public static final String KEY_BROKER_IPC_PORT = "broker_ipc_port";

    public static final String DORIS_DEFAULT_QUERY_USER = "root";
    public static final String DORIS_DEFAULT_QUERY_PASSWORD = "";
    public static final Integer DORIS_DEFAULT_FE_QUERY_PORT = 9030;
    public static final Integer DORIS_DEFAULT_FE_EDIT_LOG_PORT = 9010;
    public static final Integer DORIS_DEFAULT_BROKER_IPC_PORT = 8000;

}
