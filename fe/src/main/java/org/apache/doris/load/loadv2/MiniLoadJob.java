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

import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TMiniLoadBeginRequest;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.Sets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

public class MiniLoadJob extends LoadJob {

    private String tableName;

    // only for log replay
    public MiniLoadJob() {
        super();
        this.jobType = EtlJobType.MINI;
    }

    public MiniLoadJob(long dbId, TMiniLoadBeginRequest request) throws MetaNotFoundException {
        super(dbId, request.getLabel());
        this.jobType = EtlJobType.MINI;
        this.tableName = request.getTbl();
        if (request.isSetTimeout_second()) {
            this.timeoutSecond = request.getTimeout_second();
        } else {
            this.timeoutSecond = Config.stream_load_default_timeout_second;
        }
        if (request.isSetMax_filter_ratio()) {
            this.maxFilterRatio = request.getMax_filter_ratio();
        }
        this.isCancellable = false;
        this.createTimestamp = request.getCreate_timestamp();
        this.loadStartTimestamp = createTimestamp;
        this.authorizationInfo = gatherAuthInfo();
    }

    @Override
    public Set<String> getTableNamesForShow() {
        return Sets.newHashSet(tableName);
    }

    @Override
    public Set<String> getTableNames() throws MetaNotFoundException {
        return Sets.newHashSet(tableName);
    }

    public AuthorizationInfo gatherAuthInfo() throws MetaNotFoundException {
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        return new AuthorizationInfo(database.getFullName(), getTableNames());
    }

    @Override
    public void beginTxn() throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException {

        transactionId = Catalog.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, label, -1, "FE: " + FrontendOptions.getLocalHostAddress(),
                                  TransactionState.LoadJobSourceType.BACKEND_STREAMING, id,
                                  timeoutSecond);
    }

    @Override
    protected void executeAfterAborted(TransactionState txnState) {
        updateLoadingStatue(txnState);
    }

    @Override
    protected void executeAfterVisible(TransactionState txnState) {
        updateLoadingStatue(txnState);
    }

    @Override
    protected void executeReplayOnAborted(TransactionState txnState) {
        updateLoadingStatue(txnState);
    }

    @Override
    protected void executeReplayOnVisible(TransactionState txnState) {
        updateLoadingStatue(txnState);
    }

    private void updateLoadingStatue(TransactionState txnState) {
        MiniLoadTxnCommitAttachment miniLoadTxnCommitAttachment =
                (MiniLoadTxnCommitAttachment) txnState.getTxnCommitAttachment();
        loadingStatus.replaceCounter(DPP_ABNORMAL_ALL, String.valueOf(miniLoadTxnCommitAttachment.getFilteredRows()));
        loadingStatus.replaceCounter(DPP_NORMAL_ALL, String.valueOf(miniLoadTxnCommitAttachment.getLoadedRows()));
        if (miniLoadTxnCommitAttachment.getErrorLogUrl() != null) {
            loadingStatus.setTrackingUrl(miniLoadTxnCommitAttachment.getErrorLogUrl());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, tableName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        tableName = Text.readString(in);
    }
}
