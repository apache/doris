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

package org.apache.doris.job.base;

import org.apache.doris.job.common.IntervalUnit;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

@Data
public class TimerDefinition {

    @SerializedName(value = "il")
    private Long interval;

    @SerializedName(value = "iu")
    private IntervalUnit intervalUnit;
    @SerializedName(value = "stm")
    private Long startTimeMs;
    @SerializedName(value = "etm")
    private Long endTimeMs;

    private Long latestSchedulerTimeMs;


    public void checkParams() {
        if (null == startTimeMs) {
            startTimeMs = System.currentTimeMillis() + intervalUnit.getIntervalMs(interval);
        }
        if (null != endTimeMs && endTimeMs < startTimeMs) {
            throw new IllegalArgumentException("endTimeMs must be greater than the start time");
        }

        if (null != intervalUnit) {
            if (null == interval) {
                throw new IllegalArgumentException("interval cannot be null when intervalUnit is not null");
            }
            if (interval <= 0) {
                throw new IllegalArgumentException("interval must be greater than 0");
            }
        }
    }
}
