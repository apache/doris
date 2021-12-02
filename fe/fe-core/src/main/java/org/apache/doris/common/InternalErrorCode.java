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

import org.apache.doris.thrift.TStatusCode;

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

    // for DQL
    // Caused by user
    ANALYSIS_ERR(400),
    RESOURCE_LIMIT_EXCEEDED_ERR(401),

    // Caused by user or system failure
    TIMEOUT_ERR(410),
    CANCELLED_ERR(411),

    // Caused by system failure
    NO_READABLE_REPLICA_ERR(420)
    ;

    private long errCode;
    InternalErrorCode(long code) {
        this.errCode = code;
    }

    @Override
    public String toString() {
        return "errCode = " + errCode;
    }

    public static InternalErrorCode fromTStatusCode(final TStatusCode tStatusCode) {
        if (null == tStatusCode) {
            return INTERNAL_ERR;
        }
        switch (tStatusCode) {
            case OK:
                return OK;
            case TIMEOUT:
                return TIMEOUT_ERR;
            case CANCELLED:
                return CANCELLED_ERR;
            case MEM_LIMIT_EXCEEDED:
                return RESOURCE_LIMIT_EXCEEDED_ERR;
            case ANALYSIS_ERROR:
            case INCOMPLETE:  // This status code is not used now.
                return ANALYSIS_ERR;
            default:
                return INTERNAL_ERR;
        }
    }
}