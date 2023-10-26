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

import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.scheduler.common.IntervalUnit;

import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * refresh schedule in mtmv
 */
public class MTMVRefreshSchedule {
    @SerializedName("st")
    private String startTime;
    @SerializedName("i")
    private long interval;
    @SerializedName("tu")
    private IntervalUnit timeUnit;

    // For deserialization
    public MTMVRefreshSchedule() {
    }

    public MTMVRefreshSchedule(String startTime, int interval, IntervalUnit timeUnit) {
        this.startTime = startTime;
        this.interval = Objects.requireNonNull(interval, "require interval object");
        this.timeUnit = Objects.requireNonNull(timeUnit, "require timeUnit object");
    }

    /**
     * getStartTime
     * @return startTime
     */
    public String getStartTime() {
        return startTime;
    }

    /**
     * getInterval
     * @return interval
     */
    public long getInterval() {
        return interval;
    }

    /**
     * getTimeUnit
     * @return timeUnit
     */
    public IntervalUnit getTimeUnit() {
        return timeUnit;
    }

    /**
     * validate
     */
    public void validate() {
        if (interval <= 0) {
            throw new AnalysisException("interval must be greater than 0");
        }
        if (!StringUtils.isEmpty(startTime)) {
            long startsTimeMillis = TimeUtils.timeStringToLong(startTime);
            if (startsTimeMillis < System.currentTimeMillis()) {
                throw new AnalysisException("starts time must be greater than current time");
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("EVERY ");
        builder.append(interval);
        builder.append(" ");
        builder.append(timeUnit);
        if (!StringUtils.isEmpty(startTime)) {
            builder.append(" STARTS ");
            builder.append(startTime);
        }
        return builder.toString();
    }
}
