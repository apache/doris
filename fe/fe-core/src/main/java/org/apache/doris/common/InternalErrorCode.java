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

package org.apache.doris.common;

public enum InternalErrorCode {
    OK(0),

    // for common error
    IMPOSSIBLE_ERROR_ERR(1),
    INTERNAL_ERR(2),
    REPLICA_FEW_ERR(3),
    PARTITIONS_ERR(4),
    DB_ERR(5),
    TABLE_ERR(6),
    META_NOT_FOUND_ERR(7),

    // for load job error
    MANUAL_PAUSE_ERR(100),
    MANUAL_STOP_ERR(101),
    TOO_MANY_FAILURE_ROWS_ERR(102),
    CREATE_TASKS_ERR(103),
    TASKS_ABORT_ERR(104),
    CANNOT_RESUME_ERR(105),
    TIMEOUT_TOO_MUCH(106),

    // for external catalog
    GET_REMOTE_DATA_ERROR(202),

    // for MoW table
    DELETE_BITMAP_LOCK_ERR(301);

    private long errCode;

    InternalErrorCode(long code) {
        this.errCode = code;
    }

    @Override
    public String toString() {
        return "errCode = " + errCode;
    }
}
