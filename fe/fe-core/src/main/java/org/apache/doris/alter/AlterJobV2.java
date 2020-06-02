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

package org.apache.doris.alter;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/*
 * Version 2 of AlterJob, for replacing the old version of AlterJob.
 * This base class of RollupJob and SchemaChangeJob
 */
public abstract class AlterJobV2 implements Writable {
    private static final Logger LOG = LogManager.getLogger(AlterJobV2.class);

    public enum JobState {
        PENDING, // Job is created
        WAITING_TXN, // New replicas are created and Shadow catalog object is visible for incoming txns,
                     // waiting for previous txns to be finished
        RUNNING, // alter tasks are sent to BE, and waiting for them finished.
        FINISHED, // job is done
        CANCELLED; // job is cancelled(failed or be cancelled by user)

        public boolean isFinalState() {
            return this == JobState.FINISHED || this == JobState.CANCELLED;
        }
    }

    public enum JobType {
        ROLLUP, SCHEMA_CHANGE
    }

    @SerializedName(value = "type")
    protected JobType type;
    @SerializedName(value = "jobId")
    protected long jobId;
    @SerializedName(value = "jobState")
    protected JobState jobState;

    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "tableId")
    protected long tableId;
    @SerializedName(value = "tableName")
    protected String tableName;

    @SerializedName(value = "errMsg")
    protected String errMsg = "";
    @SerializedName(value = "createTimeMs")
    protected long createTimeMs = -1;
    @SerializedName(value = "finishedTimeMs")
    protected long finishedTimeMs = -1;
    @SerializedName(value = "timeoutMs")
    protected long timeoutMs = -1;

    public AlterJobV2(long jobId, JobType jobType, long dbId, long tableId, String tableName, long timeoutMs) {
        this.jobId = jobId;
        this.type = jobType;
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.timeoutMs = timeoutMs;

        this.createTimeMs = System.currentTimeMillis();
        this.jobState = JobState.PENDING;
    }

    protected AlterJobV2(JobType type) {
        this.type = type;
    }

    public long getJobId() {
        return jobId;
    }

    public JobState getJobState() {
        return jobState;
    }

    public JobType getType() {
        return type;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() - createTimeMs > timeoutMs;
    }

    public boolean isExpire() {
        return isDone() && (System.currentTimeMillis() - finishedTimeMs) / 1000 > Config.history_job_keep_max_second;
    }

    public boolean isDone() {
        return jobState.isFinalState();
    }

    public long getFinishedTimeMs() {
        return finishedTimeMs;
    }

    /**
     * The keyword 'synchronized' only protects 2 methods:
     * run() and cancel()
     * Only these 2 methods can be visited by different thread(internal working thread and user connection thread)
     * So using 'synchronized' to make sure only one thread can run the job at one time.
     * 
     * lock order:
     *      synchronized
     *      db lock
     */
    public synchronized void run() {
        if (isTimeout()) {
            cancelImpl("Timeout");
            return;
        }

        try {
            switch (jobState) {
            case PENDING:
                runPendingJob();
                break;
            case WAITING_TXN:
                runWaitingTxnJob();
                break;
            case RUNNING:
                runRunningJob();
                break;
            default:
                break;
            }
        } catch (AlterCancelException e) {
            cancelImpl(e.getMessage());
        }
    }

    public final boolean cancel(String errMsg) {
        synchronized (this) {
            return cancelImpl(errMsg);
        }
    }

    /**
    * should be call before executing the job.
    * return false if table is not stable.
    */
    protected boolean checkTableStable(Database db) throws AlterCancelException {
        OlapTable tbl = (OlapTable) db.getTable(tableId);

        boolean isStable = tbl.isStable(Catalog.getCurrentSystemInfo(),
                    Catalog.getCurrentCatalog().getTabletScheduler(), db.getClusterName());

        tbl.writeLock();
        try {
            if (!isStable) {
                errMsg = "table is unstable";
                LOG.warn("wait table {} to be stable before doing {} job", tableId, type);
                tbl.setState(OlapTableState.WAITING_STABLE);
                return false;
            } else {
                // table is stable, set is to ROLLUP and begin altering.
                LOG.info("table {} is stable, start {} job {}", tableId, type);
                tbl.setState(type == JobType.ROLLUP ? OlapTableState.ROLLUP : OlapTableState.SCHEMA_CHANGE);
                return true;
            }
        } finally {
            tbl.writeUnlock();
        }
    }

    protected abstract void runPendingJob() throws AlterCancelException;

    protected abstract void runWaitingTxnJob() throws AlterCancelException;

    protected abstract void runRunningJob() throws AlterCancelException;

    protected abstract boolean cancelImpl(String errMsg);

    protected abstract void getInfo(List<List<Comparable>> infos);

    public abstract void replay(AlterJobV2 replayedJob);

    public static AlterJobV2 read(DataInput in) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_86) {
            JobType type = JobType.valueOf(Text.readString(in));
            switch (type) {
                case ROLLUP:
                    return RollupJobV2.read(in);
                case SCHEMA_CHANGE:
                    return SchemaChangeJobV2.read(in);
                default:
                    Preconditions.checkState(false);
                    return null;
            }
        } else {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, AlterJobV2.class);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
        Text.writeString(out, jobState.name());

        out.writeLong(jobId);
        out.writeLong(dbId);
        out.writeLong(tableId);
        Text.writeString(out, tableName);

        Text.writeString(out, errMsg);
        out.writeLong(createTimeMs);
        out.writeLong(finishedTimeMs);
        out.writeLong(timeoutMs);
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        // read common members as write in AlterJobV2.write().
        // except 'type' member, which is read in AlterJobV2.read()
        jobState = JobState.valueOf(Text.readString(in));

        jobId = in.readLong();
        dbId = in.readLong();
        tableId = in.readLong();
        tableName = Text.readString(in);

        errMsg = Text.readString(in);
        createTimeMs = in.readLong();
        finishedTimeMs = in.readLong();
        timeoutMs = in.readLong();
    }
}
