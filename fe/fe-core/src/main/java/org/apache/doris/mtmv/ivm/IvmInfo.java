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

import com.google.gson.annotations.SerializedName;


/**
 * Thin persistent IVM metadata stored on MTMV.
 *
 * <p>Consumption positions are managed by {@code OlapTableStream} per-partition offsets.
 * The old {@code IvmStreamRef}-based tracking has been removed.
 */
public class IvmInfo {

    @SerializedName("en")
    private boolean enableIvm = false;

    /** Persisted ivm_use_full_keys flag: true means the MV unique keys include identity key columns. */
    @SerializedName("ukf")
    private boolean useFullKeys = false;

    /** Compact persisted SHA-256 layout signature; see IvmPlanSignature#canonicalString for details. */
    @SerializedName("ps")
    private String planSignature;

    /** The prefix of the sequence values this MV's rows are stamped with; see IvmSequenceCalculator. */
    @SerializedName("sp")
    private long sequencePrefix;

    public IvmInfo() {
    }

    public IvmInfo(IvmInfo other) {
        this.enableIvm = other.enableIvm;
        this.useFullKeys = other.useFullKeys;
        this.planSignature = other.planSignature;
        this.sequencePrefix = other.sequencePrefix;
    }

    public boolean isEnableIvm() {
        return enableIvm;
    }

    public void setEnableIvm(boolean enableIvm) {
        this.enableIvm = enableIvm;
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

    public long getSequencePrefix() {
        return sequencePrefix;
    }

    public void advanceSequencePrefix() {
        sequencePrefix++;
    }

    @Override
    public String toString() {
        return "IvmInfo{"
                + "enableIvm=" + enableIvm
                + ", useFullKeys=" + useFullKeys
                + ", planSignature='" + planSignature + '\''
                + '}';
    }
}
