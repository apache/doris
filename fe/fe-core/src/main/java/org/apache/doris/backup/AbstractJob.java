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

package org.apache.doris.backup;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/*
 * The design of JobI is as follows
 * 1. Here are only two methods: run() and cancel() that can modify the internal state of a Job.
 *    And each method is implemented as synchronized to avoid handling concurrent modify things.
 *
 * 2. isDone() method is used to check whether we can submit the next job.
 */
public abstract class AbstractJob implements Writable {

    public enum JobType {
        BACKUP, RESTORE
    }

    @SerializedName(value = "type")
    protected JobType type;

    // must be set right before job's running
    @SerializedName(value = "env")
    protected Env env;
    // repo will be set at first run()
    @SerializedName(value = "repo")
    protected Repository repo;
    @SerializedName(value = "repoId")
    protected long repoId;

    /*
     * In BackupJob, jobId will be generated every time before we call prepareAndSendSnapshotTask();
     * Because prepareAndSendSnapshotTask() may be called several times due to FE restart.
     * And each time this method is called, the snapshot tasks will be sent with (maybe) different
     * version and version hash. So we have to use different job id to identify the tasks in different batches.
     */
    @SerializedName(value = "jobId")
    protected long jobId = -1;

    @SerializedName(value = "label")
    protected String label;
    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "dbName")
    protected String dbName;

    @SerializedName(value = "status")
    protected Status status = Status.OK;

    @SerializedName(value = "createTime")
    protected long createTime = -1;
    @SerializedName(value = "finishedTime")
    protected long finishedTime = -1;
    @SerializedName(value = "timeoutMs")
    protected long timeoutMs;

    // task signature -> <finished num / total num>
    @SerializedName(value = "taskProgress")
    protected Map<Long, Pair<Integer, Integer>> taskProgress = Maps.newConcurrentMap();

    @SerializedName(value = "isTypeRead")
    protected boolean isTypeRead = false;

    // save err msg of tasks
    @SerializedName(value = "taskErrMsg")
    protected Map<Long, String> taskErrMsg = Maps.newHashMap();

    protected AbstractJob(JobType type) {
        this.type = type;
    }

    protected AbstractJob(JobType type, String label, long dbId, String dbName,
            long timeoutMs, Env env, long repoId) {
        this.type = type;
        this.label = label;
        this.dbId = dbId;
        this.dbName = dbName;
        this.createTime = System.currentTimeMillis();
        this.timeoutMs = timeoutMs;
        this.env = env;
        this.repoId = repoId;
    }

    public JobType getType() {
        return type;
    }

    public long getJobId() {
        return jobId;
    }

    public String getLabel() {
        return label;
    }

    public long getDbId() {
        return dbId;
    }

    public String getDbName() {
        return dbName;
    }

    public Status getStatus() {
        return status;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getFinishedTime() {
        return finishedTime;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setEnv(Env env) {
        this.env = env;
    }

    public long getRepoId() {
        return repoId;
    }

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public abstract void run();

    public abstract Status cancel();

    public abstract void replayRun();

    public abstract void replayCancel();

    public abstract boolean isDone();

    public abstract boolean isPending();

    public abstract boolean isCancelled();

    public static AbstractJob read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_127) {
            AbstractJob job = null;
            JobType type = JobType.valueOf(Text.readString(in));
            if (type == JobType.BACKUP) {
                job = new BackupJob();
            } else if (type == JobType.RESTORE) {
                job = new RestoreJob();
            } else {
                throw new IOException("Unknown job type: " + type.name());
            }

            job.setTypeRead(true);
            job.readFields(in);
            return job;
        }
        String json = Text.readString(in);
        JsonObject jsonObject = GsonUtils.GSON.fromJson(json, JsonObject.class);
        JobType type = JobType.valueOf(jsonObject.get("type").getAsString());
        switch (type) {
            case BACKUP:
                return GsonUtils.GSON.fromJson(json, BackupJob.class);
            case RESTORE:
                return GsonUtils.GSON.fromJson(json, RestoreJob.class);
            default:
                throw new IOException("Unknown job type: " + type.name());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        if (!isTypeRead) {
            type = JobType.valueOf(Text.readString(in));
            isTypeRead = true;
        }

        repoId = in.readLong();
        label = Text.readString(in);
        jobId = in.readLong();
        dbId = in.readLong();
        dbName = Text.readString(in);

        createTime = in.readLong();
        finishedTime = in.readLong();
        timeoutMs = in.readLong();

        if (in.readBoolean()) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                long taskId = in.readLong();
                String msg = Text.readString(in);
                taskErrMsg.put(taskId, msg);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(type.name());
        sb.append(" repo id: ").append(repoId).append(", label: ").append(label);
        sb.append(", job id: ").append(jobId).append(", db id: ").append(dbId).append(", db name: ").append(dbName);
        sb.append(", status: ").append(status);
        sb.append(", timeout: ").append(timeoutMs);
        return sb.toString();
    }
}
