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

package org.apache.doris.job.extensions.mtmv;

import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class MTMVTaskContext {

    @SerializedName(value = "triggerMode")
    private MTMVTaskTriggerMode triggerMode;

    @SerializedName(value = "partitions")
    private List<String> partitions;

    @SerializedName(value = "isComplete")
    private boolean isComplete;

    @SerializedName(value = "computeGroup")
    private String computeGroup;

    @SerializedName(value = "refreshMode")
    private RefreshMode refreshMode;

    // Nullable for compatibility and partial policy sources:
    // null means the task did not persist an explicit fallback choice and should
    // use the default of its resolved refresh mode.
    @SerializedName(value = "af", alternate = "allowFallback")
    private Boolean allowFallback;

    // True for scheduled/on-commit/system tasks whose refresh method must be
    // resolved from the MV's persisted refreshInfo at execution time. This is
    // separate from refreshMode == null because old task JSON also lacks
    // refreshMode and must still fall back to the legacy isComplete field.
    @SerializedName(value = "md", alternate = "useMvDefaultRefreshPolicy")
    private boolean useMvDefaultRefreshPolicy;

    public MTMVTaskContext(MTMVTaskTriggerMode triggerMode) {
        this.triggerMode = triggerMode;
        this.refreshMode = RefreshMode.AUTO;
        this.allowFallback = true;
    }

    private MTMVTaskContext(MTMVTaskTriggerMode triggerMode, List<String> partitions, RefreshMode refreshMode,
            boolean allowFallback, String computeGroup) {
        this.triggerMode = triggerMode;
        this.partitions = partitions;
        this.refreshMode = refreshMode;
        this.isComplete = refreshMode == RefreshMode.COMPLETE;
        this.allowFallback = allowFallback;
        this.computeGroup = computeGroup;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public MTMVTaskTriggerMode getTriggerMode() {
        return triggerMode;
    }

    /**
     * Backward-compatible: returns true when refresh mode is COMPLETE.
     * For deserialized old tasks without refreshMode, falls back to the isComplete field.
     */
    public boolean isComplete() {
        if (refreshMode != null) {
            return refreshMode == RefreshMode.COMPLETE;
        }
        return isComplete;
    }

    public String getComputeGroup() {
        return computeGroup;
    }

    public RefreshMode getRefreshMode() {
        if (refreshMode != null) {
            return refreshMode;
        }
        return isComplete ? RefreshMode.COMPLETE : RefreshMode.AUTO;
    }

    public boolean allowFallback() {
        if (allowFallback != null) {
            return allowFallback;
        }
        // Old serialized tasks did not have allowFallback. Reconstruct the
        // intended behavior from the resolved refresh mode: only AUTO defaults
        // to fallback.
        return defaultAllowFallback(getRefreshMode());
    }

    public boolean useMvDefaultRefreshPolicy() {
        return useMvDefaultRefreshPolicy;
    }

    public static MTMVTaskContext forMvDefault(MTMVTaskTriggerMode triggerMode) {
        MTMVTaskContext taskContext = new MTMVTaskContext(triggerMode);
        // Do not persist AUTO here. The actual method may be PARTITIONS,
        // INCREMENTAL, or COMPLETE after CREATE/ALTER, so the task must read
        // the MV default policy when it runs.
        taskContext.refreshMode = null;
        taskContext.allowFallback = null;
        taskContext.useMvDefaultRefreshPolicy = true;
        return taskContext;
    }

    public static MTMVTaskContext of(MTMVTaskTriggerMode triggerMode, List<String> partitions,
            RefreshMode refreshMode) {
        return new MTMVTaskContext(triggerMode, partitions, refreshMode, defaultAllowFallback(refreshMode), null);
    }

    public static MTMVTaskContext of(MTMVTaskTriggerMode triggerMode, List<String> partitions,
            RefreshMode refreshMode, boolean allowFallback, String computeGroup) {
        return new MTMVTaskContext(triggerMode, partitions, refreshMode, allowFallback, computeGroup);
    }

    private static boolean defaultAllowFallback(RefreshMode refreshMode) {
        return refreshMode == RefreshMode.AUTO;
    }

    @Override
    public String toString() {
        return "MTMVTaskContext{"
                + "triggerMode=" + triggerMode
                + ", partitions=" + partitions
                + ", computeGroup=" + computeGroup
                + ", refreshMode=" + getRefreshMode()
                + ", allowFallback=" + allowFallback()
                + ", useMvDefaultRefreshPolicy=" + useMvDefaultRefreshPolicy
                + '}';
    }
}
