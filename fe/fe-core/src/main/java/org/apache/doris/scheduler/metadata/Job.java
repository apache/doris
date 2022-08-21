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

package org.apache.doris.scheduler.metadata;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.scheduler.Utils;
import org.apache.doris.scheduler.Utils.JobState;
import org.apache.doris.scheduler.Utils.TriggerMode;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Job implements Writable {
    @SerializedName("id")
    private long id;

    @SerializedName("name")
    private String name;

    // set default to MANUAL is for compatibility
    @SerializedName("triggerMode")
    private Utils.TriggerMode triggerMode = Utils.TriggerMode.MANUAL;

    // set default to UNKNOWN is for compatibility
    @SerializedName("state")
    private Utils.JobState state = Utils.JobState.UNKNOWN;

    @SerializedName("schedule")
    private JobSchedule schedule;

    @SerializedName("createTime")
    private long createTime;

    @SerializedName("dbName")
    private String dbName;

    @SerializedName("definition")
    private String definition;

    @SerializedName("properties")
    private Map<String, String> properties;

    @SerializedName("expireTime")
    private long expireTime = -1;

    // set default to ROOT is for compatibility
    @SerializedName("createUser")
    private String createUser = "root";

    public Job(String name) {
        this.name = name;
        this.createTime = System.currentTimeMillis();
    }

    public static Job read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Job.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TriggerMode getTriggerMode() {
        return triggerMode;
    }

    public void setTriggerMode(TriggerMode triggerMode) {
        this.triggerMode = triggerMode;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public JobSchedule getSchedule() {
        return schedule;
    }

    public void setSchedule(JobSchedule schedule) {
        this.schedule = schedule;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public static class JobSchedule {
        @SerializedName("startTime")
        private long startTime;

        @SerializedName("period")
        private long period;

        @SerializedName("timeUnit")
        private TimeUnit timeUnit;

        public JobSchedule(long startTime, long period, TimeUnit timeUnit) {
            this.startTime = startTime;
            this.period = period;
            this.timeUnit = timeUnit;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getPeriod() {
            return period;
        }

        public void setPeriod(long period) {
            this.period = period;
        }

        public TimeUnit getTimeUnit() {
            return timeUnit;
        }

        public void setTimeUnit(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        public String toString() {
            return " (START " + LocalDateTime.ofInstant(Instant.ofEpochSecond(startTime), ZoneId.systemDefault())
                    + " EVERY(" + period + " " + timeUnit + "))";
        }
    }
}
