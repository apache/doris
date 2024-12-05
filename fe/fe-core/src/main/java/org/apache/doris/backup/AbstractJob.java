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
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/*
 * The design of JobI is as follows
 * 1. Here are only two methods: run() and cancel() that can modify the internal state of a Job.
 *    And each method is implemented as synchronized to avoid handling concurrent modify things.
 *
 * 2. isDone() method is used to check whether we can submit the next job.
 */
public abstract class AbstractJob implements Writable {
    public static final String COMPRESSED_JOB_ID = "COMPRESSED";

    public enum JobType {
        BACKUP, RESTORE, BACKUP_COMPRESSED, RESTORE_COMPRESSED
    }

    @SerializedName("t")
    protected JobType type;

    // must be set right before job's running
    protected Env env;
    // repo will be set at first run()
    protected Repository repo;
    @SerializedName("rid")
    protected long repoId;

    /*
     * In BackupJob, jobId will be generated every time before we call prepareAndSendSnapshotTask();
     * Because prepareAndSendSnapshotTask() may be called several times due to FE restart.
     * And each time this method is called, the snapshot tasks will be sent with (maybe) different
     * version and version hash. So we have to use different job id to identify the tasks in different batches.
     */
    @SerializedName("jid")
    protected long jobId = -1;

    @SerializedName("l")
    protected String label;
    @SerializedName("dbid")
    protected long dbId;
    @SerializedName("dbn")
    protected String dbName;

    protected Status status = Status.OK;

    @SerializedName("ct")
    protected long createTime = -1;
    @SerializedName("ft")
    protected long finishedTime = -1;
    @SerializedName("to")
    protected long timeoutMs;

    // task signature -> <finished num / total num>
    protected Map<Long, Pair<Integer, Integer>> taskProgress = Maps.newConcurrentMap();

    protected boolean isTypeRead = false;

    // save err msg of tasks
    @SerializedName("msg")
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

    public abstract boolean isFinished();

    public abstract Status updateRepo(Repository repo);

    public static AbstractJob read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_136) {
            AbstractJob job = null;
            JobType type = JobType.valueOf(Text.readString(in));
            if (type == JobType.BACKUP || type == JobType.BACKUP_COMPRESSED) {
                job = new BackupJob(type);
            } else if (type == JobType.RESTORE || type == JobType.RESTORE_COMPRESSED) {
                job = new RestoreJob(type);
            } else {
                throw new IOException("Unknown job type: " + type.name());
            }

            job.setTypeRead(true);
            job.readFields(in);
            return job;
        } else {
            String json = Text.readString(in);
            if (COMPRESSED_JOB_ID.equals(json)) {
                return GsonUtils.fromJsonCompressed(in, AbstractJob.class);
            } else {
                return GsonUtils.GSON.fromJson(json, AbstractJob.class);
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int savedNum = Math.min(3, taskErrMsg.size());
        Iterator<Map.Entry<Long, String>> iterator = taskErrMsg.entrySet().iterator();
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            if (count >= savedNum) {
                iterator.remove();
            }
            count++;
        }

        // For a completed job, there's no need to save it with compressed serialization as it has
        // no snapshot or backup meta info, making it small in size. This helps maintain compatibility
        // more easily.
        if (!isDone() && ((type == JobType.BACKUP && Config.backup_job_compressed_serialization)
                || (type == JobType.RESTORE && Config.restore_job_compressed_serialization))) {
            Text.writeString(out, COMPRESSED_JOB_ID);
            GsonUtils.toJsonCompressed(out, this);
        } else {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }
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
