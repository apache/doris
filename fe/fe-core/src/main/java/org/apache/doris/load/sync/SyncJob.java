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

import org.apache.doris.analysis.BinlogDesc;
import org.apache.doris.analysis.ChannelDescription;
import org.apache.doris.analysis.CreateDataSyncJobStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.sync.SyncFailMsg.MsgType;
import org.apache.doris.load.sync.canal.CanalSyncJob;
import org.apache.doris.persist.gson.GsonUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public abstract class SyncJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(SyncJob.class);

    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "jobName")
    protected String jobName;
    @SerializedName(value = "channelDescriptions")
    protected List<ChannelDescription> channelDescriptions;
    protected BinlogDesc binlogDesc;
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
    protected List<SyncChannel> channels;

    public SyncJob(long id, String jobName, long dbId) {
        this.id = id;
        this.dbId = dbId;
        this.jobName = jobName;
        this.jobState = JobState.PENDING;
        this.createTimeMs = System.currentTimeMillis();
        this.lastStartTimeMs = -1L;
        this.lastStopTimeMs = -1L;
        this.finishTimeMs = -1L;
    }

    public enum JobState {
        PENDING,
        RUNNING,
        PAUSED,
        CANCELLED
    }

    public static SyncJob fromStmt(long jobId, CreateDataSyncJobStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }
        SyncJob syncJob = null;
        try {
            switch (stmt.getDataSyncJobType()) {
                case CANAL:
                    syncJob = new CanalSyncJob(jobId, stmt.getJobName(), db.getId());
                    break;
                default:
                    throw new DdlException("Unknown load job type.");
            }
            syncJob.setChannelDescriptions(stmt.getChannelDescriptions());
            syncJob.checkAndSetBinlogInfo(stmt.getBinlogDesc());
            return syncJob;
        } catch (Exception e) {
            throw new DdlException(e.getMessage());
        }
    }

    // return true if job is done (CANCELLED)
    public boolean isCompleted() {
        return jobState == JobState.CANCELLED;
    }

    public boolean isPaused() {
        return jobState == JobState.PAUSED;
    }

    public boolean isRunning() {
        return jobState == JobState.RUNNING;
    }

    public boolean isCancelled() {
        return jobState == JobState.CANCELLED;
    }

    public synchronized void updateState(JobState newState, boolean isReplay) {
        this.jobState = newState;
        switch (newState) {
            case PENDING:
                break;
            case RUNNING:
                this.lastStartTimeMs = System.currentTimeMillis();
                break;
            case PAUSED:
                this.lastStopTimeMs = System.currentTimeMillis();
                break;
            case CANCELLED:
                this.lastStopTimeMs = System.currentTimeMillis();
                this.finishTimeMs = System.currentTimeMillis();
                break;
            default:
                Preconditions.checkState(false, "wrong job state: " + newState.name());
                break;
        }
        if (!isReplay) {
            SyncJobUpdateStateInfo info = new SyncJobUpdateStateInfo(id, jobState, lastStartTimeMs, lastStopTimeMs,
                    finishTimeMs, failMsg);
            Catalog.getCurrentCatalog().getEditLog().logUpdateSyncJobState(info);
        }
    }

    public void checkAndSetBinlogInfo(BinlogDesc binlogDesc) throws DdlException {
        this.binlogDesc = binlogDesc;
    }

    public abstract void execute() throws UserException;

    public void cancel(MsgType msgType, String errMsg) {
    }

    public void pause() throws DdlException {
        throw new DdlException("not implemented");
    }

    public void resume() throws DdlException {
        throw new DdlException("not implemented");
    }

    public String getStatus() {
        return "\\N";
    }

    public String getJobConfig() {
        return "\\N";
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

        public SyncJobUpdateStateInfo(long id, JobState jobState, long lastStartTimeMs, long lastStopTimeMs, long finishTimeMs, SyncFailMsg failMsg) {
            this.id = id;
            this.jobState = jobState;
            this.lastStartTimeMs = lastStartTimeMs;
            this.lastStopTimeMs = lastStopTimeMs;
            this.finishTimeMs = finishTimeMs;
            this.failMsg = failMsg;
        }

        public long getId() {
            return this.id;
        }

        public long getLastStartTimeMs() {
            return this.lastStartTimeMs;
        }

        public long getLastStopTimeMs() {
            return this.lastStopTimeMs;
        }

        public long getFinishTimeMs() {
            return this.finishTimeMs;
        }

        public JobState getJobState() {
            return this.jobState;
        }

        public SyncFailMsg getFailMsg() {
            return this.failMsg;
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

    public List<Comparable> getShowInfo() {
        List<Comparable> jobInfo = Lists.newArrayList();
        // jobId
        jobInfo.add(id);
        // jobName
        jobInfo.add(jobName);
        // type
        jobInfo.add(dataSyncJobType.name());
        // state
        jobInfo.add(jobState.name());
        // channel
        StringBuilder channelInfo = new StringBuilder();
        if (channels != null) {
            for (int i = 0; i < channels.size(); i++) {
                channelInfo.append(channels.get(i).getInfo());
                if (i < channels.size() - 1) {
                    channelInfo.append(", ");
                }
            }
            jobInfo.add(channelInfo.toString());
        } else {
            jobInfo.add(FeConstants.null_string);
        }

        // status
        jobInfo.add(getStatus());
        // jobConfig
        jobInfo.add(getJobConfig());
        // createTimeMs
        jobInfo.add(TimeUtils.longToTimeString(createTimeMs));
        // lastStartTimeMs
        jobInfo.add(TimeUtils.longToTimeString(lastStartTimeMs));
        // lastStopTimeMs
        jobInfo.add(TimeUtils.longToTimeString(lastStopTimeMs));
        // finishTimeMs
        jobInfo.add(TimeUtils.longToTimeString(finishTimeMs));
        // failMsg
        if (failMsg == null) {
            jobInfo.add(FeConstants.null_string);
        } else {
            jobInfo.add(failMsg.toString());
        }
        return jobInfo;
    }

    public void replayUpdateSyncJobState(SyncJobUpdateStateInfo info) {
        lastStartTimeMs = info.getLastStartTimeMs();
        lastStopTimeMs = info.getLastStopTimeMs();
        finishTimeMs = info.getFinishTimeMs();
        updateState(info.getJobState(), true);
        LOG.info("replay update sync job state: {}", info);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this, SyncJob.class));
    }

    public static SyncJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, SyncJob.class);
    }

    public void setChannelDescriptions(List<ChannelDescription> channelDescriptions) {
        this.channelDescriptions = channelDescriptions;
    }

    public long getId() {
        return this.id;
    }

    public long getDbId() {
        return this.dbId;
    }

    public String getJobName() {
        return this.jobName;
    }

    public JobState getJobState() {
        return this.jobState;
    }

    public DataSyncJobType getJobType() {
        return this.dataSyncJobType;
    }

    public SyncFailMsg getFailMsg() {
        return failMsg;
    }

    public void setFailMsg(SyncFailMsg failMsg) {
        this.failMsg = failMsg;
    }

    public List<ChannelDescription> getChannelDescriptions() {
        return this.channelDescriptions;
    }

}