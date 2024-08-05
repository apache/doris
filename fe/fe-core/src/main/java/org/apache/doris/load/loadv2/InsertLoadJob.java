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
import org.apache.doris.common.annotation.LogException;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.FailMsg.CancelType;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.IOException;
import java.util.Set;

/**
 * The class is performed to record the finished info of insert load job.
 * It is created after txn is visible which belongs to insert load job.
 * The state of insert load job is always finished, so it will never be scheduled by JobScheduler.
 */
public class InsertLoadJob extends LoadJob {
    @SerializedName("tid")
    private long tableId;

    // only for log replay
    public InsertLoadJob() {
        super(EtlJobType.INSERT);
    }

    public InsertLoadJob(String label, long transactionId, long dbId, long tableId,
            long createTimestamp, String failMsg, String trackingUrl,
            UserIdentity userInfo) throws MetaNotFoundException {
        super(EtlJobType.INSERT, dbId, label);
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
        this.userInfo = userInfo;
    }

    public InsertLoadJob(String label, long transactionId, long dbId, long tableId,
                         long createTimestamp, String failMsg, String trackingUrl,
                         UserIdentity userInfo, Long jobId) throws MetaNotFoundException {
        super(EtlJobType.INSERT_JOB, dbId, label, jobId);
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
        this.userInfo = userInfo;
    }

    public AuthorizationInfo gatherAuthInfo() throws MetaNotFoundException {
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        return new AuthorizationInfo(database.getFullName(), getTableNames());
    }

    @Override
    public Set<String> getTableNamesForShow() {
        String name = Env.getCurrentInternalCatalog().getDb(dbId).flatMap(db -> db.getTable(tableId))
                .map(TableIf::getName).orElse(String.valueOf(tableId));
        return Sets.newHashSet(name);
    }

    @LogException
    @Override
    public Set<String> getTableNames() throws MetaNotFoundException {
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        Table table = database.getTableOrMetaException(tableId);
        return Sets.newHashSet(table.getName());
    }

    @Override
    protected void readFields(DataInput in) throws IOException {
        super.readFields(in);
        tableId = in.readLong();
    }
}
