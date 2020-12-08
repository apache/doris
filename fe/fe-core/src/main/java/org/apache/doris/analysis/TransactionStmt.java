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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.proto.PUniqueId;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.thrift.TTxnParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TransactionStmt extends StatementBase {
    private static final Logger LOG = LogManager.getLogger(TransactionStmt.class);

    private final static String TXN_BEGIN = "begin";
    private final static String TXN_COMMIT = "commit";
    private final static String TXN_ROLLBACK = "rollback";

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        switch (getOrigStmt().originStmt.toLowerCase()) {
            case TXN_BEGIN:
                begin();
                break;
            case TXN_COMMIT:
                commit();
                break;
            case TXN_ROLLBACK:
                rollback();
                break;
            default:
                throw new AnalysisException(String.format("Invalid command: %s", getOrigStmt().originStmt));
        }
    }

    private void begin() throws AnalysisException {
        if (analyzer.getContext().getTxnConf() != null) {
            throw new AnalysisException("A transaction has already begin");
        }
        TTxnParams txnParams = new TTxnParams();
        txnParams.setNeedTxn(true);
        txnParams.setThriftRpcTimeoutMs(5000);
        analyzer.getContext().setTxnConf(txnParams);
        resetTxn();
    }

    private void commit() throws AnalysisException, UserException {
        if (analyzer.getContext().getTxnConf() == null || analyzer.getContext().getBackend() == null) {
            throw new AnalysisException("No transaction to commit");
        }
        PUniqueId fragmentInstanceId = new PUniqueId();
        fragmentInstanceId.hi = analyzer.getContext().getTxnConf().getFragmentInstanceId().getHi();
        fragmentInstanceId.lo = analyzer.getContext().getTxnConf().getFragmentInstanceId().getLo();

        try {
            BackendServiceProxy.getInstance().commitForTxn(fragmentInstanceId, analyzer.getContext().getBackend());
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage());
        }

        analyzer.getContext().setTxnConf(null);
        analyzer.getContext().setBackend(null);
    }

    private void rollback() throws AnalysisException, UserException {
        if (analyzer.getContext().getTxnConf() == null || analyzer.getContext().getBackend() == null) {
            throw new AnalysisException("No transaction to rollback");
        }
        PUniqueId fragmentInstanceId = new PUniqueId();
        fragmentInstanceId.hi = analyzer.getContext().getTxnConf().getFragmentInstanceId().getHi();
        fragmentInstanceId.lo = analyzer.getContext().getTxnConf().getFragmentInstanceId().getLo();

        try {
            BackendServiceProxy.getInstance().rollbackForTxn(fragmentInstanceId, analyzer.getContext().getBackend());
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage());
        }

        analyzer.getContext().setTxnConf(null);
        analyzer.getContext().setBackend(null);
    }

    private void resetTxn() {
        analyzer.getContext().getTxnConf().setTxnId(-1);
        analyzer.getContext().getTxnConf().setDb("");
        analyzer.getContext().getTxnConf().setTbl("");
    }
}