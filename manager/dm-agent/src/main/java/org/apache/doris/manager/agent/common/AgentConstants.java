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

package org.apache.doris.manager.agent.common;

public class AgentConstants {
    public static final int COMMAND_LOG_PAGE_SIZE = 1000;
    public static final int COMMAND_EXECUTE_UNHEALTH_CODE = -1;

    public static final int FE_HTTP_PORT_DEFAULT = 8030;
    public static final int BE_HTTP_PORT_DEFAULT = 8040;

    public static final int COMMAND_HISTORY_SAVE_MAX_COUNT = 100;
    public static final int TASK_LOG_ROW_MAX_COUNT = 1000;

    public static final String BASH_BIN = "/bin/sh ";

    public static final String FE_CONFIG_KEY_META_DIR = "meta_dir";
    public static final String BE_CONFIG_KEY_STORAGE_DIR = "storage_root_path";
    public static final String FE_CONFIG_KEY_HTTP_PORT = "http_port";
    public static final String BE_CONFIG_KEY_HTTP_PORT = "webserver_port";

    public static final String FE_DEFAULT_META_DIR_RELATIVE_PATH = "/doris-meta";
    public static final String BE_DEFAULT_STORAGE_DIR_RELATIVE_PATH = "/storage";
    public static final String FE_CONFIG_FILE_RELATIVE_PATH = "/conf/fe.conf";
    public static final String BE_CONFIG_FILE_RELATIVE_PATH = "/conf/be.conf";
    public static final String LOG_FILE_RELATIVE_PATH = "/log/";
    public static final String TASK_LOG_FILE_RELATIVE_PATH = "/log/task.log";

    public static final int TASK_ERROR_CODE_DEFAULT = -501;
    public static final int TASK_ERROR_CODE_EXCEPTION = -502;
}
