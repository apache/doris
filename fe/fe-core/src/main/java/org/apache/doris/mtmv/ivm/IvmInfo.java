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

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Thin persistent IVM metadata stored on MTMV.
 *
 * <p>Consumption positions are managed by {@code OlapTableStream} per-partition offsets.
 * The old {@code IvmStreamRef}-based tracking has been removed.
 */
public class IvmInfo {

    @SerializedName("en")
    private boolean enableIvm = false;

    @SerializedName("brr")
    // Keep an explicit COMPLETE requirement because persisted partition names are a point-in-time snapshot.
    // For example, if the MV has {p1, p2} when invalidated and partition sync adds p3 before refresh,
    // COMPLETE must rebuild p3 too.
    private boolean completeBaselineRebuildRequired;

    @SerializedName("brp")
    // MV partitions that must be rebuilt before their IVM offsets can be used again.
    private Set<String> pendingBaselineRebuildPartitions = new HashSet<>();

    /** Persisted ivm_use_full_keys flag: true means the MV unique keys include identity key columns. */
    @SerializedName("ukf")
    private boolean useFullKeys = false;

    /** Compact persisted SHA-256 layout signature; see IvmPlanSignature#canonicalString for details. */
    @SerializedName("ps")
    private String planSignature;

    @SerializedName("rv")
    private long refreshVersion;

    public IvmInfo() {
    }

    public IvmInfo(IvmInfo other) {
        this.enableIvm = other.enableIvm;
        this.completeBaselineRebuildRequired = other.completeBaselineRebuildRequired;
        this.pendingBaselineRebuildPartitions = new HashSet<>(other.pendingBaselineRebuildPartitions);
        this.useFullKeys = other.useFullKeys;
        this.planSignature = other.planSignature;
        this.refreshVersion = other.refreshVersion;
    }

    public boolean isEnableIvm() {
        return enableIvm;
    }

    public void setEnableIvm(boolean enableIvm) {
        this.enableIvm = enableIvm;
    }

    public boolean isBaselineRebuildRequired() {
        return completeBaselineRebuildRequired || !pendingBaselineRebuildPartitions.isEmpty();
    }

    public boolean requiresCompleteBaselineRebuild() {
        return completeBaselineRebuildRequired;
    }

    public Set<String> getPendingBaselineRebuildPartitions() {
        return Collections.unmodifiableSet(new HashSet<>(pendingBaselineRebuildPartitions));
    }

    public void requireCompleteBaselineRebuild() {
        completeBaselineRebuildRequired = true;
        pendingBaselineRebuildPartitions.clear();
    }

    public void addPendingBaselineRebuildPartitions(Set<String> partitions) {
        Preconditions.checkArgument(!partitions.isEmpty(), "baseline rebuild partitions can not be empty");
        if (!completeBaselineRebuildRequired) {
            pendingBaselineRebuildPartitions.addAll(partitions);
        }
    }

    public void clearBaselineRebuild() {
        completeBaselineRebuildRequired = false;
        pendingBaselineRebuildPartitions.clear();
    }

    public boolean isUseFullKeys() {
        return useFullKeys;
    }

    public void setUseFullKeys(boolean useFullKeys) {
        this.useFullKeys = useFullKeys;
    }

    public String getPlanSignature() {
        return planSignature;
    }

    public void setPlanSignature(String planSignature) {
        this.planSignature = planSignature;
    }

    public long getRefreshVersion() {
        return refreshVersion;
    }

    public void advanceRefreshVersion() {
        refreshVersion++;
    }

    @Override
    public String toString() {
        return "IvmInfo{"
                + "enableIvm=" + enableIvm
                + ", completeBaselineRebuildRequired=" + completeBaselineRebuildRequired
                + ", pendingBaselineRebuildPartitions=" + pendingBaselineRebuildPartitions
                + ", useFullKeys=" + useFullKeys
                + ", planSignature='" + planSignature + '\''
                + '}';
    }
}
