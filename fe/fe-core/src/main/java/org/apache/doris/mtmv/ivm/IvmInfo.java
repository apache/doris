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

    @SerializedName("bb")
    private boolean binlogBroken = false;

    /** Compact persisted SHA-256 layout signature; see IvmPlanSignature#canonicalString for details. */
    @SerializedName("ps")
    private String planSignature;

    public IvmInfo() {
    }

    public IvmInfo(IvmInfo other) {
        this.enableIvm = other.enableIvm;
        this.binlogBroken = other.binlogBroken;
        this.planSignature = other.planSignature;
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

    public String getPlanSignature() {
        return planSignature;
    }

    public void setPlanSignature(String planSignature) {
        this.planSignature = planSignature;
    }

    @Override
    public String toString() {
        return "IvmInfo{"
                + "enableIvm=" + enableIvm
                + ", binlogBroken=" + binlogBroken
                + ", planSignature='" + planSignature + '\''
                + '}';
    }
}
