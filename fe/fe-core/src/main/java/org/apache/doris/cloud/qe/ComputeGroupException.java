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

import java.util.HashMap;
import java.util.Map;

/**
 * Thrown for cloud cluster errors
 */
public class ComputeGroupException extends UserException {
    public enum FailedTypeEnum {
        NOT_CLOUD_MODE,
        CONNECT_CONTEXT_NOT_SET,
        CONNECT_CONTEXT_NOT_SET_COMPUTE_GROUP,
        CURRENT_USER_NO_AUTH_TO_USE_ANY_COMPUTE_GROUP,
        CURRENT_USER_NO_AUTH_TO_USE_DEFAULT_COMPUTE_GROUP,
        CURRENT_USER_NO_AUTH_TO_USE_COMPUTE_GROUP,
        CURRENT_COMPUTE_GROUP_NO_BE,
        COMPUTE_GROUPS_NO_ALIVE_BE,
        CURRENT_COMPUTE_GROUP_NOT_EXIST,
        CURRENT_COMPUTE_GROUP_BEEN_MANUAL_SHUTDOWN,
        SYSTEM_NOT_HAVE_COMPUTE_GROUP,
    }

    private InternalErrorCode errorCode;
    private ErrorCode mysqlErrorCode;
    private final FailedTypeEnum failedType;
    public String  msg;

    private static final Map<FailedTypeEnum, String> helpInfos = new HashMap<>();

    static {
        // mostly appears in the logs.
        helpInfos.put(FailedTypeEnum.NOT_CLOUD_MODE, " contact the system administrator, impossible");
        helpInfos.put(FailedTypeEnum.CONNECT_CONTEXT_NOT_SET, " contact the system administrator, "
                + "unless it's a daemon job in log, check your job");
        helpInfos.put(FailedTypeEnum.CONNECT_CONTEXT_NOT_SET_COMPUTE_GROUP, " review your code to ensure you haven't "
                + "forgotten to set the compute group in the connect context");
        // user may encounter
        helpInfos.put(FailedTypeEnum.CURRENT_USER_NO_AUTH_TO_USE_ANY_COMPUTE_GROUP, " contact the system administrator "
                + "and request that they grant you the appropriate compute group permissions, "
                + "use SQL `GRANT USAGE_PRIV ON COMPUTE GROUP {compute_group_name} TO {user}`");
        helpInfos.put(FailedTypeEnum.CURRENT_USER_NO_AUTH_TO_USE_COMPUTE_GROUP,
                "use SQL `GRANT USAGE_PRIV ON COMPUTE GROUP {compute_group_name} TO {user}`");
        helpInfos.put(FailedTypeEnum.CURRENT_USER_NO_AUTH_TO_USE_DEFAULT_COMPUTE_GROUP,
                " contact the system administrator "
                + "and request that they grant you the default compute group permissions, "
                + "use SQL `SHOW PROPERTY like 'default_compute_group'` "
                + "and `GRANT USAGE_PRIV ON COMPUTE GROUP {compute_group_name} TO {user}`");
        helpInfos.put(FailedTypeEnum.CURRENT_COMPUTE_GROUP_NO_BE, " contact the system administrator "
                + "to check the status of the compute group nodes, or may be all nodes dropped");
        helpInfos.put(FailedTypeEnum.COMPUTE_GROUPS_NO_ALIVE_BE,
                " wait a moment and try again, or execute another query");
        helpInfos.put(FailedTypeEnum.CURRENT_COMPUTE_GROUP_NOT_EXIST, " contact the administrator to confirm "
                + "if the current compute group has been dropped");
        helpInfos.put(FailedTypeEnum.CURRENT_COMPUTE_GROUP_BEEN_MANUAL_SHUTDOWN,
                " contact the administrator to manually restart the compute group");
        helpInfos.put(FailedTypeEnum.SYSTEM_NOT_HAVE_COMPUTE_GROUP,
                " contact the administrator to add more compute group resources");
    }

    private String helpMsg() {
        return helpInfos.get(failedType);
    }

    public ComputeGroupException(String msg, FailedTypeEnum failedType) {
        super(msg);
        this.msg = msg;
        this.mysqlErrorCode = ErrorCode.ERR_CLOUD_CLUSTER_ERROR;
        this.errorCode = InternalErrorCode.INTERNAL_ERR;
        this.failedType = failedType;
    }

    public String toString() {
        return msg +  ", ComputeGroupException: " + failedType + ", you can" + helpMsg();
    }

    @Override
    public String getMessage() {
        String message = errorCode + ", detailMessage = " + super.getMessage()
                +  ", ComputeGroupException: " + failedType + ", you can" + helpMsg();
        return deleteUselessMsg(message);
    }
}
