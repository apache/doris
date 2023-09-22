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

public class MTMVStatus {
    @SerializedName("lfem")
    private String lastRefreshErrorMsg;
    @SerializedName("les")
    private String lastExecutorSql;
    @SerializedName("lfs")
    private boolean lastRefreshState;
    @SerializedName("lrft")
    private long lastRefreshFinishedTime;
    @SerializedName("lrst")
    private long lastRefreshStartTime;
    @SerializedName("lrji")
    private long lastRefreshJobId;

    public MTMVStatus(String lastRefreshErrorMsg, String lastExecutorSql, boolean lastRefreshState,
            long lastRefreshStartTime, long lastRefreshFinishedTime, long lastRefreshJobId) {
        this.lastRefreshErrorMsg = lastRefreshErrorMsg;
        this.lastExecutorSql = lastExecutorSql;
        this.lastRefreshState = lastRefreshState;
        this.lastRefreshStartTime = lastRefreshStartTime;
        this.lastRefreshFinishedTime = lastRefreshFinishedTime;
        this.lastRefreshJobId = lastRefreshJobId;
    }

    public String getLastRefreshErrorMsg() {
        return lastRefreshErrorMsg;
    }

    public String getLastExecutorSql() {
        return lastExecutorSql;
    }

    public boolean isLastRefreshState() {
        return lastRefreshState;
    }

    public long getLastRefreshFinishedTime() {
        return lastRefreshFinishedTime;
    }

    public long getLastRefreshStartTime() {
        return lastRefreshStartTime;
    }

    public long getLastRefreshJobId() {
        return lastRefreshJobId;
    }
}
