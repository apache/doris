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

import com.google.gson.annotations.SerializedName;

public class MVRefreshIntervalTriggerInfo {
    @SerializedName("startTime")
    private String startTime;
    @SerializedName("interval")
    private long interval;
    @SerializedName("timeUnit")
    private String timeUnit;

    // For deserialization
    public MVRefreshIntervalTriggerInfo() {
    }

    public MVRefreshIntervalTriggerInfo(String startTime, long interval, String timeUnit) {
        this.startTime = startTime;
        this.interval = interval;
        this.timeUnit = timeUnit;
    }

    public String getStartTime() {
        return startTime;
    }

    public long getInterval() {
        return interval;
    }

    public String getTimeUnit() {
        return timeUnit;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (startTime != null) {
            sb.append(" START WITH \"").append(startTime).append("\"");
        }
        if (interval > 0) {
            sb.append(" NEXT ").append(interval).append(" ").append(timeUnit);
        }
        return sb.toString();
    }
}
