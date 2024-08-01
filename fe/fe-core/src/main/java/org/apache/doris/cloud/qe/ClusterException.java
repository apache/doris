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

package org.apache.doris.cloud.qe;

import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.UserException;

/**
 * Thrown for cloud cluster errors
 */
public class ClusterException extends UserException {
    public enum FailedTypeEnum {
        NOT_CLOUD_MODE,
        CONNECT_CONTEXT_NOT_SET,
        CONNECT_CONTEXT_NOT_SET_CLUSTER,
        CURRENT_USER_NO_AUTH_TO_USE_ANY_CLUSTER,
        CURRENT_USER_NO_AUTH_TO_USE_DEFAULT_CLUSTER,
        CURRENT_CLUSTER_NO_BE,
        CLUSTERS_NO_ALIVE_BE,
        CURRENT_CLUSTER_NOT_EXIST,
        CURRENT_CLUSTER_BEEN_MANUAL_SHUTDOWN,
        SYSTEM_NOT_HAVE_CLUSTER,
    }

    private InternalErrorCode errorCode;
    private ErrorCode mysqlErrorCode;
    private final FailedTypeEnum failedType;
    public String  msg;

    public ClusterException(String msg, FailedTypeEnum failedType) {
        super(msg);
        this.msg = msg;
        this.mysqlErrorCode = ErrorCode.ERR_CLOUD_CLUSTER_ERROR;
        this.errorCode = InternalErrorCode.INTERNAL_ERR;
        this.failedType = failedType;
    }

    public String toString() {
        return msg +  ", ClusterException: " + failedType;
    }

    @Override
    public String getMessage() {
        String message = errorCode + ", detailMessage = " + super.getMessage() +  ", ClusterException: " + failedType;
        return deleteUselessMsg(message);
    }
}
