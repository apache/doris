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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

@Deprecated
public class MiniLoadJob extends LoadJob {
    private static final Logger LOG = LogManager.getLogger(MiniLoadJob.class);

    @SerializedName("tn")
    private String tableName;

    private long tableId;

    public MiniLoadJob() {
        super(EtlJobType.MINI);
    }

    @Override
    public Set<String> getTableNamesForShow() {
        return Sets.newHashSet(tableName);
    }

    @Override
    public Set<String> getTableNames() throws MetaNotFoundException {
        return Sets.newHashSet(tableName);
    }

    @Override
    public void beginTxn() {
    }

    @Override
    protected void replayTxnAttachment(TransactionState txnState) {
        updateLoadingStatue(txnState);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        tableName = Text.readString(in);
    }

    public AuthorizationInfo gatherAuthInfo() throws MetaNotFoundException {
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        return new AuthorizationInfo(database.getFullName(), getTableNames());
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
}
