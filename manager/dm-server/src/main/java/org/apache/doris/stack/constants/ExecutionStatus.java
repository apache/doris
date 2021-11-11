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

/**
 * running status for workflow and task nodes
 */
public enum ExecutionStatus {

    /**
     * status：
     * 0 submit success
     * 1 running
     * 2 failure
     * 3 success
     */
    SUBMITTED(0, "submit success"),
    RUNNING(1, "running"),
    FAILURE(2, "failure"),
    SUCCESS(3, "success");

    private final int code;
    private final String desc;

    ExecutionStatus(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    /**
     * status is success
     *
     * @return status
     */
    public boolean typeIsSuccess() {
        return this == SUCCESS;
    }

    /**
     * status is failure
     *
     * @return status
     */
    public boolean typeIsFailure() {
        return this == FAILURE;
    }

    /**
     * status is finished
     *
     * @return status
     */
    public boolean typeIsFinished() {
        return typeIsSuccess() || typeIsFailure();
    }

    /**
     * status is running
     *
     * @return status
     */
    public boolean typeIsRunning() {
        return this == RUNNING || this == SUBMITTED;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
