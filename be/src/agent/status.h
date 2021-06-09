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

#ifndef DORIS_BE_SRC_AGENT_STATUS_H
#define DORIS_BE_SRC_AGENT_STATUS_H

namespace doris {

// TODO: this enum should be replaced by Status
enum AgentStatus {
    DORIS_SUCCESS = 0,
    DORIS_ERROR = -1,
    DORIS_TASK_REQUEST_ERROR = -101,
    DORIS_FILE_DOWNLOAD_INVALID_PARAM = -201,
    DORIS_FILE_DOWNLOAD_INSTALL_OPT_FAILED = -202,
    DORIS_FILE_DOWNLOAD_CURL_INIT_FAILED = -203,
    DORIS_FILE_DOWNLOAD_FAILED = -204,
    DORIS_FILE_DOWNLOAD_GET_LENGTH_FAILED = -205,
    DORIS_FILE_DOWNLOAD_NOT_EXIST = -206,
    DORIS_FILE_DOWNLOAD_LIST_DIR_FAIL = -207,
    DORIS_CREATE_TABLE_EXIST = -301,
    DORIS_CREATE_TABLE_DIFF_SCHEMA_EXIST = -302,
    DORIS_CREATE_TABLE_NOT_EXIST = -303,
    DORIS_DROP_TABLE_NOT_EXIST = -401,
    DORIS_PUSH_INVALID_TABLE = -501,
    DORIS_PUSH_INVALID_VERSION = -502,
    DORIS_PUSH_TIME_OUT = -503,
    DORIS_PUSH_HAD_LOADED = -504,
    DORIS_TIMEOUT = -901,
    DORIS_INTERNAL_ERROR = -902,
    DORIS_DISK_REACH_CAPACITY_LIMIT = -903
};
} // namespace doris
#endif // DORIS_BE_SRC_AGENT_STATUS_H
