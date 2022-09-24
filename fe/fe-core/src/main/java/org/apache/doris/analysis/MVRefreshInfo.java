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

package org.apache.doris.analysis;

import org.apache.doris.common.UserException;

import com.google.gson.annotations.SerializedName;

public class MVRefreshInfo {
    @SerializedName("neverRefresh")
    private boolean neverRefresh;
    @SerializedName("refreshMethod")
    private RefreshMethod refreshMethod;
    @SerializedName("triggerInfo")
    private MVRefreshTriggerInfo triggerInfo;

    // For deserialization
    public MVRefreshInfo() {}

    public MVRefreshInfo(boolean neverRefresh) {
        this(neverRefresh, RefreshMethod.COMPLETE, null);
    }

    public MVRefreshInfo(RefreshMethod method, MVRefreshTriggerInfo trigger) {
        this(false, method, trigger);
    }

    public MVRefreshInfo(boolean neverRefresh, RefreshMethod method, MVRefreshTriggerInfo trigger) {
        this.neverRefresh = neverRefresh;
        refreshMethod = method;
        triggerInfo = trigger;
    }

    void analyze(Analyzer analyzer) throws UserException {
        if (triggerInfo != null) {
            triggerInfo.analyze(analyzer);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (neverRefresh) {
            sb.append(" NEVER REFRESH ");
        } else {
            sb.append(" REFRESH ");
            sb.append(refreshMethod.toString());
            sb.append(triggerInfo.toString());
        }
        return sb.toString();
    }

    public boolean isNeverRefresh() {
        return neverRefresh;
    }

    public RefreshMethod getRefreshMethod() {
        return refreshMethod;
    }

    public MVRefreshTriggerInfo getTriggerInfo() {
        return triggerInfo;
    }

    enum RefreshMethod {
        COMPLETE, FAST, FORCE
    }

    enum RefreshTrigger {
        DEMAND, COMMIT, INTERVAL
    }

    public enum BuildMode {
        IMMEDIATE, DEFERRED
    }
}
