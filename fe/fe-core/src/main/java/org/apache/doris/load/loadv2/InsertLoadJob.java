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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

/**
 * The class records both running and finished insert load jobs. A running job is registered before
 * execution so SHOW LOAD can report it and CANCEL LOAD can stop its transaction and coordinator.
 * Insert load jobs are driven by their statement executors and are never scheduled by JobScheduler.
 */
public class InsertLoadJob extends LoadJob {

    private static final Logger LOG = LogManager.getLogger(InsertLoadJob.class);

    @SerializedName("tid")
    private long tableId;

    // Snapshot of loadStatistic.toJson() captured when the job finishes.
    // loadStatistic is not persisted (no @SerializedName), so we save it here
    // to survive FE restarts.
    @SerializedName("jdj")
    private String jobDetailsJson = null;

    private transient TUniqueId queryId;

    // only for log replay
    public InsertLoadJob() {
        super(EtlJobType.INSERT);
    }

    public InsertLoadJob(long dbId, String label, long jobId) {
        super(EtlJobType.INSERT, dbId, label, jobId);
    }

    /**
     * Create a load job for a running insert. Unlike a historical insert load record, this job is
     * published before the insert finishes and must contain enough metadata for SHOW/CANCEL LOAD.
     */
    public InsertLoadJob(long dbId, long tableId, String dbName, String tableName, String label,
            long jobId, TUniqueId queryId, UserIdentity userInfo) {
        super(EtlJobType.INSERT, dbId, label, jobId);
        this.tableId = tableId;
        this.queryId = queryId;
        this.authorizationInfo = new AuthorizationInfo(dbName, Sets.newHashSet(tableName));
        this.userInfo = userInfo;
    }

    public InsertLoadJob(String label, long transactionId, long dbId, long tableId,
            long createTimestamp, String failMsg, String trackingUrl, String firstErrorMsg,
            UserIdentity userInfo) throws MetaNotFoundException {
        super(EtlJobType.INSERT, dbId, label);
        setJobProperties(transactionId, tableId, createTimestamp, failMsg, trackingUrl, firstErrorMsg, userInfo);
    }

    public InsertLoadJob(String label, long transactionId, long dbId, long tableId,
                         long createTimestamp, String failMsg, String trackingUrl, String firstErrorMsg,
                         UserIdentity userInfo, Long jobId) throws MetaNotFoundException {
        super(EtlJobType.INSERT, dbId, label, jobId);
        setJobProperties(transactionId, tableId, createTimestamp, failMsg, trackingUrl, firstErrorMsg, userInfo);
    }

    public void setJobProperties(long transactionId, long tableId, long createTimestamp,
                                        String failMsg, String trackingUrl, String firstErrorMsg,
                                        UserIdentity userInfo) throws MetaNotFoundException {
        this.tableId = tableId;
        this.transactionId = transactionId;
        this.createTimestamp = createTimestamp;
        this.loadStartTimestamp = createTimestamp;
        this.finishTimestamp = System.currentTimeMillis();
        if (Strings.isNullOrEmpty(failMsg)) {
            this.state = JobState.FINISHED;
            this.progress = 100;
        } else {
            this.state = JobState.CANCELLED;
            this.failMsg = new FailMsg(CancelType.LOAD_RUN_FAIL, failMsg);
            this.progress = 0;
        }
        this.authorizationInfo = gatherAuthInfo();
        this.loadingStatus.setTrackingUrl(trackingUrl);
        this.loadingStatus.setFirstErrorMsg(firstErrorMsg);
        this.userInfo = userInfo;
        // Snapshot the current loadStatistic so it survives FE restarts.
        // loadStatistic itself is not annotated with @SerializedName and won't be persisted.
        this.jobDetailsJson = this.loadStatistic.toJson();
    }

    public AuthorizationInfo gatherAuthInfo() throws MetaNotFoundException {
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        return new AuthorizationInfo(database.getFullName(), getTableNames());
    }

    /**
     * Bind the transaction created by the insert executor to the running load job.
     *
     * @return false if CANCEL LOAD won the race and the executor must not start the coordinator
     */
    public boolean bindTransaction(long transactionId) {
        writeLock();
        try {
            this.transactionId = transactionId;
            return state != JobState.CANCELLED;
        } finally {
            writeUnlock();
        }
    }

    public boolean isCancelled() {
        readLock();
        try {
            return state == JobState.CANCELLED;
        } finally {
            readUnlock();
        }
    }

    @Override
    protected void addLoadIdsToCancel(List<TUniqueId> loadIds) {
        if (queryId != null) {
            loadIds.add(queryId);
        }
    }

    @Override
    public Set<String> getTableNamesForShow() {
        String name = Env.getCurrentInternalCatalog().getDb(dbId).flatMap(db -> db.getTable(tableId))
                .map(TableIf::getName).orElse(String.valueOf(tableId));
        return Sets.newHashSet(name);
    }

    @Override
    public Set<String> getTableNames() throws MetaNotFoundException {
        try {
            Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
            Table table = database.getTableOrMetaException(tableId);
            return Sets.newHashSet(table.getName());
        } catch (Exception e) {
            LOG.warn(e);
            throw e;
        }
    }

    @Override
    protected String getJobDetailsJson() {
        // Use the persisted snapshot when loadStatistic is empty (e.g. after FE restart).
        // Fall back to the live loadStatistic during execution.
        if (jobDetailsJson != null) {
            return jobDetailsJson;
        }
        return loadStatistic.toJson();
    }
}
