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

import org.apache.doris.common.io.Text;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MVRefreshIntervalTriggerInfo implements Writable {
    private String startTime;
    private long interval;
    private String timeUnit;

    // For deserialization
    public MVRefreshIntervalTriggerInfo() {}

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
            sb.append(" START WITH ").append(startTime);
        }
        if (interval > 0) {
            sb.append(" NEXT ").append(interval).append(" ").append(timeUnit);
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, startTime);
        out.writeLong(interval);
        out.writeBoolean(timeUnit != null);
        if (timeUnit != null) {
            Text.writeString(out, timeUnit);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        startTime = Text.readString(in);
        interval = in.readLong();
        boolean hasTimeUnit = in.readBoolean();
        if (hasTimeUnit) {
            timeUnit = Text.readString(in);
        }
    }
}
