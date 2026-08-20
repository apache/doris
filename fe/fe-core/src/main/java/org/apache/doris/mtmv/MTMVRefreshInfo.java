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

import org.apache.doris.mtmv.MTMVRefreshEnum.BuildMode;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.nereids.exceptions.AnalysisException;

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
    // Nullable because ALTER may update only the trigger or method. A null
    // value means "not specified in this update" or "old metadata"; the effective
    // value is derived from the refresh method.
    @SerializedName("af")
    private Boolean allowFallback;
    @SerializedName("rti")
    private MTMVRefreshTriggerInfo refreshTriggerInfo;

    public MTMVRefreshInfo() {
    }

    public MTMVRefreshInfo(BuildMode buildMode,
            RefreshMethod refreshMethod,
            MTMVRefreshTriggerInfo refreshTriggerInfo) {
        this(buildMode, refreshMethod, defaultAllowFallback(refreshMethod), refreshTriggerInfo);
    }

    public MTMVRefreshInfo(BuildMode buildMode,
            RefreshMethod refreshMethod,
            boolean allowFallback,
            MTMVRefreshTriggerInfo refreshTriggerInfo) {
        this.buildMode = Objects.requireNonNull(buildMode, "require buildMode object");
        this.refreshMethod = Objects.requireNonNull(refreshMethod, "require refreshMethod object");
        this.allowFallback = allowFallback;
        this.refreshTriggerInfo = Objects.requireNonNull(refreshTriggerInfo, "require refreshTriggerInfo object");
    }

    public void validate() {
        // COMPLETE is already the last refresh attempt, so FALLBACK would be
        // meaningless and can hide invalid SQL/metadata.
        if (refreshMethod == RefreshMethod.COMPLETE && allowFallback()) {
            throw new AnalysisException("COMPLETE refresh does not support FALLBACK");
        }
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

    public Boolean getAllowFallback() {
        return allowFallback;
    }

    public boolean allowFallback() {
        if (allowFallback != null) {
            return allowFallback;
        }
        // Backward compatibility for metadata written before allowFallback was
        // persisted: AUTO kept its implicit fallback behavior; strict methods do
        // not gain fallback unless it is explicitly stored.
        return defaultAllowFallback(refreshMethod);
    }

    public static boolean defaultAllowFallback(RefreshMethod refreshMethod) {
        return refreshMethod == RefreshMethod.AUTO;
    }

    public void setBuildMode(BuildMode buildMode) {
        this.buildMode = buildMode;
    }

    public void setRefreshMethod(RefreshMethod refreshMethod) {
        this.refreshMethod = refreshMethod;
    }

    public void setAllowFallback(Boolean allowFallback) {
        this.allowFallback = allowFallback;
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
            // When ALTER changes the method but does not specify FALLBACK,
            // reset fallback to the new method default instead of carrying over
            // the old method's fallback setting.
            this.allowFallback = newRefreshInfo.allowFallback != null
                    ? newRefreshInfo.allowFallback : defaultAllowFallback(newRefreshInfo.refreshMethod);
        } else if (newRefreshInfo.allowFallback != null) {
            this.allowFallback = newRefreshInfo.allowFallback;
        }
        if (newRefreshInfo.refreshTriggerInfo != null) {
            this.refreshTriggerInfo = newRefreshInfo.refreshTriggerInfo;
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("BUILD ");
        builder.append(buildMode);
        builder.append(" REFRESH ");
        builder.append(refreshMethod);
        if (allowFallback() && refreshMethod != RefreshMethod.AUTO) {
            builder.append(" FALLBACK");
        }
        builder.append(" ");
        builder.append(refreshTriggerInfo);
        return builder.toString();
    }

}
