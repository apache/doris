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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.nereids.trees.plans.commands.info.MTMVRefreshEnum.BuildMode;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVRefreshEnum.RefreshMethod;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * refresh info
 */
public class MTMVRefreshInfo {
    @SerializedName("bm")
    private BuildMode buildMode;
    @SerializedName("rm")
    private RefreshMethod refreshMethod;
    @SerializedName("rti")
    private MTMVRefreshTriggerInfo refreshTriggerInfo;

    public MTMVRefreshInfo() {
    }

    public MTMVRefreshInfo(BuildMode buildMode,
            RefreshMethod refreshMethod,
            MTMVRefreshTriggerInfo refreshTriggerInfo) {
        this.buildMode = Objects.requireNonNull(buildMode, "require buildMode object");
        this.refreshMethod = Objects.requireNonNull(refreshMethod, "require refreshMethod object");
        this.refreshTriggerInfo = Objects.requireNonNull(refreshTriggerInfo, "require refreshTriggerInfo object");
    }

    public void validate() {
        if (refreshTriggerInfo != null) {
            refreshTriggerInfo.validate();
        }
    }

    public BuildMode getBuildMode() {
        return buildMode;
    }

    public RefreshMethod getRefreshMethod() {
        return refreshMethod;
    }

    public MTMVRefreshTriggerInfo getRefreshTriggerInfo() {
        return refreshTriggerInfo;
    }

    public void setBuildMode(BuildMode buildMode) {
        this.buildMode = buildMode;
    }

    public void setRefreshMethod(RefreshMethod refreshMethod) {
        this.refreshMethod = refreshMethod;
    }

    public void setRefreshTriggerInfo(
            MTMVRefreshTriggerInfo refreshTriggerInfo) {
        this.refreshTriggerInfo = refreshTriggerInfo;
    }

    /**
     * update refreshInfo
     */
    public MTMVRefreshInfo updateNotNull(MTMVRefreshInfo newRefreshInfo) {
        Objects.requireNonNull(newRefreshInfo);
        if (newRefreshInfo.buildMode != null) {
            this.buildMode = newRefreshInfo.buildMode;
        }
        if (newRefreshInfo.refreshMethod != null) {
            this.refreshMethod = newRefreshInfo.refreshMethod;
        }
        if (newRefreshInfo.refreshTriggerInfo != null) {
            this.refreshTriggerInfo = newRefreshInfo.refreshTriggerInfo;
        }
        return this;
    }
}
