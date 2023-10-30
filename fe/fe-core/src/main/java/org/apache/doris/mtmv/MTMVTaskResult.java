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

package org.apache.doris.mtmv;

import com.google.gson.annotations.SerializedName;

/**
 * MTMVTaskResult
 */
public class MTMVTaskResult {
    @SerializedName("refreshErrorMsg")
    private String refreshErrorMsg;
    @SerializedName("executorSql")
    private String executorSql;
    @SerializedName("refreshState")
    private boolean refreshState;
    @SerializedName("refreshFinishedTime")
    private long refreshFinishedTime;
    @SerializedName("refreshStartTime")
    private long refreshStartTime;
    @SerializedName("refreshTaskId")
    private String refreshTaskId;

    public MTMVTaskResult() {
    }

    /**
     * Constructor
     */
    public MTMVTaskResult(String refreshErrorMsg, String executorSql, boolean refreshState, long refreshFinishedTime,
            long refreshStartTime, String refreshTaskId) {
        this.refreshErrorMsg = refreshErrorMsg;
        this.executorSql = executorSql;
        this.refreshState = refreshState;
        this.refreshFinishedTime = refreshFinishedTime;
        this.refreshStartTime = refreshStartTime;
        this.refreshTaskId = refreshTaskId;
    }

    public String getRefreshErrorMsg() {
        return refreshErrorMsg;
    }

    public String getExecutorSql() {
        return executorSql;
    }

    public boolean isRefreshState() {
        return refreshState;
    }

    public long getRefreshFinishedTime() {
        return refreshFinishedTime;
    }

    public long getRefreshStartTime() {
        return refreshStartTime;
    }

    public String getRefreshTaskId() {
        return refreshTaskId;
    }
}
