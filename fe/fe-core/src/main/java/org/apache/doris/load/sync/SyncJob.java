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

package org.apache.doris.load.sync;

import org.apache.doris.analysis.ChannelDescription;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

@Deprecated
public abstract class SyncJob implements Writable {
    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "jobName")
    protected String jobName;
    @SerializedName(value = "channelDescriptions")
    protected List<ChannelDescription> channelDescriptions;
    @SerializedName(value = "createTimeMs")
    protected long createTimeMs;
    @SerializedName(value = "lastStartTimeMs")
    protected long lastStartTimeMs;
    @SerializedName(value = "lastStopTimeMs")
    protected long lastStopTimeMs;
    @SerializedName(value = "finishTimeMs")
    protected long finishTimeMs;
    @SerializedName(value = "jobState")
    protected JobState jobState;
    @SerializedName(value = "failMsg")
    protected SyncFailMsg failMsg;
    @SerializedName(value = "dataSyncJobType")
    protected DataSyncJobType dataSyncJobType;

    public enum JobState {
        PENDING,
        RUNNING,
        PAUSED,
        CANCELLED
    }

    // only use for persist when job state changed
    public static class SyncJobUpdateStateInfo implements Writable {
        @SerializedName(value = "id")
        private long id;
        @SerializedName(value = "lastStartTimeMs")
        protected long lastStartTimeMs;
        @SerializedName(value = "lastStopTimeMs")
        protected long lastStopTimeMs;
        @SerializedName(value = "finishTimeMs")
        protected long finishTimeMs;
        @SerializedName(value = "jobState")
        protected JobState jobState;
        @SerializedName(value = "failMsg")
        protected SyncFailMsg failMsg;

        public SyncJobUpdateStateInfo(long id, JobState jobState, long lastStartTimeMs,
                long lastStopTimeMs, long finishTimeMs, SyncFailMsg failMsg) {
            this.id = id;
            this.jobState = jobState;
            this.lastStartTimeMs = lastStartTimeMs;
            this.lastStopTimeMs = lastStopTimeMs;
            this.finishTimeMs = finishTimeMs;
            this.failMsg = failMsg;
        }

        @Override
        public String toString() {
            return GsonUtils.GSON.toJson(this);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            String json = GsonUtils.GSON.toJson(this);
            Text.writeString(out, json);
        }

        public static SyncJobUpdateStateInfo read(DataInput in) throws IOException {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, SyncJobUpdateStateInfo.class);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this, SyncJob.class));
    }

    public static SyncJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, SyncJob.class);
    }
}
