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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.mtmv.BaseTableInfo;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Thin persistent IVM metadata stored on MTMV.
 */
public class IvmInfo {

    @SerializedName("en")
    private boolean enableIvm = false;

    @SerializedName("bb")
    private boolean binlogBroken = false;

    /** True while an incremental refresh is in progress. Persisted for crash recovery. */
    @SerializedName("rr")
    private boolean runningIvmRefresh = false;

    @SerializedName("bs")
    private Map<BaseTableInfo, IvmStreamRef> baseTableStreams;

    public IvmInfo() {
        this.baseTableStreams = Maps.newHashMap();
    }

    /** Deep-copy persisted IVM metadata before publishing an updated edit-log entry. */
    public IvmInfo(IvmInfo other) {
        this.enableIvm = other.enableIvm;
        this.binlogBroken = other.binlogBroken;
        this.runningIvmRefresh = other.runningIvmRefresh;
        this.baseTableStreams = Maps.newHashMap();
        if (other.baseTableStreams != null) {
            for (Map.Entry<BaseTableInfo, IvmStreamRef> entry : other.baseTableStreams.entrySet()) {
                this.baseTableStreams.put(entry.getKey(), new IvmStreamRef(entry.getValue()));
            }
        }
    }

    public boolean isEnableIvm() {
        return enableIvm;
    }

    public void setEnableIvm(boolean enableIvm) {
        this.enableIvm = enableIvm;
    }

    public boolean isBinlogBroken() {
        return binlogBroken;
    }

    public void setBinlogBroken(boolean binlogBroken) {
        this.binlogBroken = binlogBroken;
    }

    public boolean isRunningIvmRefresh() {
        return runningIvmRefresh;
    }

    public void setRunningIvmRefresh(boolean runningIvmRefresh) {
        this.runningIvmRefresh = runningIvmRefresh;
    }

    public Map<BaseTableInfo, IvmStreamRef> getBaseTableStreams() {
        return baseTableStreams;
    }

    public void setBaseTableStreams(Map<BaseTableInfo, IvmStreamRef> baseTableStreams) {
        this.baseTableStreams = baseTableStreams;
    }

    @Override
    public String toString() {
        return "IvmInfo{"
                + "enableIvm=" + enableIvm
                + ", binlogBroken=" + binlogBroken
                + ", runningIvmRefresh=" + runningIvmRefresh
                + ", baseTableStreams=" + baseTableStreams
                + '}';
    }
}
