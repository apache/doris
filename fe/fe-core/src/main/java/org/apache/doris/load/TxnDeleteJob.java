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

package org.apache.doris.load;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TxnDeleteJob extends DeleteJob {
    private static final Logger LOG = LogManager.getLogger(TxnDeleteJob.class);

    public TxnDeleteJob(long id, long transactionId, String label, Map<Long, Short> partitionReplicaNum,
            DeleteInfo deleteInfo) {
        super(id, transactionId, label, partitionReplicaNum, deleteInfo);
    }

    @Override
    public long beginTxn() throws Exception {
        TransactionEntry txnEntry = ConnectContext.get().getTxnEntry();
        txnEntry.beginTransaction(targetTbl.getDatabase(), targetTbl);
        this.transactionId = txnEntry.getTransactionId();
        this.label = txnEntry.getLabel();
        return this.transactionId;
    }

    @Override
    public String commit() throws Exception {
        List<TabletCommitInfo> tabletCommitInfos = generateTabletCommitInfos();
        TransactionEntry txnEntry = ConnectContext.get().getTxnEntry();
        txnEntry.addCommitInfos(targetTbl,
                tabletCommitInfos.stream().map(c -> new TTabletCommitInfo(c.getTabletId(), c.getBackendId()))
                        .collect(Collectors.toList()));

        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(label).append("', 'txnId':'").append(transactionId);
        sb.append("', 'status':'").append(TransactionStatus.PREPARE.name()).append("'").append("}");
        return sb.toString();
    }

    @Override
    public void cancel(String reason) {
    }
}
