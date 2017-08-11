// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_AGENT_STATUS_H
#define BDG_PALO_BE_SRC_AGENT_STATUS_H

namespace palo {

enum AgentStatus {
    PALO_SUCCESS = 0,
    PALO_ERROR = -1,
    PALO_TASK_REQUEST_ERROR = -101,
    PALO_FILE_DOWNLOAD_INVALID_PARAM = -201,
    PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED = -202,
    PALO_FILE_DOWNLOAD_CURL_INIT_FAILED = -203,
    PALO_FILE_DOWNLOAD_FAILED = -204,
    PALO_FILE_DOWNLOAD_GET_LENGTH_FAILED = -205,
    PALO_FILE_DOWNLOAD_NOT_EXIST = -206,
    PALO_FILE_DOWNLOAD_LIST_DIR_FAIL = -207,
    PALO_CREATE_TABLE_EXIST = -301,
    PALO_CREATE_TABLE_DIFF_SCHEMA_EXIST = -302,
    PALO_CREATE_TABLE_NOT_EXIST = -303,
    PALO_DROP_TABLE_NOT_EXIST = -401,
    PALO_PUSH_INVALID_TABLE = -501,
    PALO_PUSH_INVALID_VERSION = -502,
    PALO_PUSH_TIME_OUT = -503,
    PALO_PUSH_HAD_LOADED = -504,
    PALO_TIMEOUT = -901,
    PALO_INTERNAL_ERROR = -902,
};
}  // namespace palo
#endif  // BDG_PALO_BE_SRC_AGENT_STATUS_H
