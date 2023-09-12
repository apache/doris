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

import org.apache.doris.analysis.MVRefreshInfo.RefreshTrigger;
import org.apache.doris.nereids.exceptions.AnalysisException;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class MVRefreshTriggerInfo {
    @SerializedName("refreshTrigger")
    private RefreshTrigger refreshTrigger;
    @SerializedName("intervalTrigger")
    private MVRefreshSchedule intervalTrigger;

    // For deserialization
    public MVRefreshTriggerInfo() {}

    public MVRefreshTriggerInfo(RefreshTrigger trigger) {
        this(trigger, null);
    }

    public MVRefreshTriggerInfo(RefreshTrigger refreshTrigger, MVRefreshSchedule intervalTrigger) {
        this.refreshTrigger = Objects.requireNonNull(refreshTrigger, "require refreshTrigger object");
        this.intervalTrigger = intervalTrigger;
    }

    public RefreshTrigger getRefreshTrigger() {
        return refreshTrigger;
    }

    public MVRefreshSchedule getIntervalTrigger() {
        return intervalTrigger;
    }

    public void validate() {
        if (refreshTrigger.equals(RefreshTrigger.SCHEDULE)) {
            if (intervalTrigger == null) {
                throw new AnalysisException("require intervalTrigger object.");
            }
            intervalTrigger.validate();
        }
    }
}
