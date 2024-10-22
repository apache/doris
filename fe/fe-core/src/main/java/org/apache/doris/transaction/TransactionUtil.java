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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.List;
import java.util.Set;

public class TransactionUtil {

    public static List<String> getTxnStateInfo(TransactionState txnState, List<String> info) {
        info.add(String.valueOf(txnState.getTransactionId()));
        info.add(txnState.getLabel());
        info.add(txnState.getCoordinator().toString());
        info.add(txnState.getTransactionStatus().name());
        info.add(txnState.getSourceType().name());
        info.add(TimeUtils.longToTimeString(txnState.getPrepareTime()));
        info.add(TimeUtils.longToTimeString(txnState.getPreCommitTime()));
        info.add(TimeUtils.longToTimeString(txnState.getCommitTime()));
        info.add(TimeUtils.longToTimeString(txnState.getLastPublishVersionTime()));
        info.add(TimeUtils.longToTimeString(txnState.getFinishTime()));
        info.add(txnState.getReason());
        info.add(String.valueOf(txnState.getErrorReplicas().size()));
        info.add(String.valueOf(txnState.getCallbackId()));
        info.add(String.valueOf(txnState.getTimeoutMs()));
        info.add(txnState.getErrMsg());
        return info;
    }

    public static void checkAuth(long dbId, TransactionState txnState) throws AnalysisException {
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbId);
        if (ConnectContext.get() != null) {
            // check auth
            Set<Long> tblIds = txnState.getIdToTableCommitInfos().keySet();
            for (Long tblId : tblIds) {
                Table tbl = db.getTableNullable(tblId);
                if (tbl != null) {
                    if (!Env.getCurrentEnv().getAccessManager()
                            .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME,
                            db.getFullName(),
                            tbl.getName(), PrivPredicate.SHOW)) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                                "SHOW TRANSACTION",
                                ConnectContext.get().getQualifiedUser(),
                                ConnectContext.get().getRemoteIP(),
                                db.getFullName() + ": " + tbl.getName());
                    }
                }
            }
        }
    }
}
