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
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.io.Text;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TMiniLoadBeginRequest;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

public class MiniLoadJob extends LoadJob {
    private static final Logger LOG = LogManager.getLogger(MiniLoadJob.class);

    private String tableName;

    private long tableId;

    public MiniLoadJob() {
        super(EtlJobType.MINI);
    }

    public MiniLoadJob(long dbId, long tableId, TMiniLoadBeginRequest request) throws MetaNotFoundException {
        super(EtlJobType.MINI, dbId, request.getLabel());
        this.tableId = tableId;
        this.tableName = request.getTbl();
        if (request.isSetTimeoutSecond()) {
            setTimeout(request.getTimeoutSecond());
        }
        if (request.isSetMaxFilterRatio()) {
            setMaxFilterRatio(request.getMaxFilterRatio());
        }
        this.createTimestamp = request.getCreateTimestamp();
        this.loadStartTimestamp = createTimestamp;
        this.authorizationInfo = gatherAuthInfo();
        this.requestId = request.getRequestId();
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
        Database database = Catalog.getCurrentCatalog().getDbOrMetaException(dbId);
        return new AuthorizationInfo(database.getFullName(), getTableNames());
    }

    @Override
    public void beginTxn()
            throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {
        transactionId = Catalog.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, Lists.newArrayList(tableId), label, requestId,
                                  new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                                  TransactionState.LoadJobSourceType.BACKEND_STREAMING, id,
                                  getTimeout());
    }

    @Override
    protected void replayTxnAttachment(TransactionState txnState) {
        updateLoadingStatue(txnState);
    }

    private void updateLoadingStatue(TransactionState txnState) {
        MiniLoadTxnCommitAttachment miniLoadTxnCommitAttachment =
                (MiniLoadTxnCommitAttachment) txnState.getTxnCommitAttachment();
        if (miniLoadTxnCommitAttachment == null) {
            // aborted txn may not has attachment
            LOG.info("no miniLoadTxnCommitAttachment, txn id: {} status: {}", txnState.getTransactionId(),
                    txnState.getTransactionStatus());
            return;
        }
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
