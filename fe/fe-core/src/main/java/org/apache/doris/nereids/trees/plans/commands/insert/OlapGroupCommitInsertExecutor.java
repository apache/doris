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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Insert executor for olap table with group commit
 */
public class OlapGroupCommitInsertExecutor extends OlapInsertExecutor {
    private static final Logger LOG = LogManager.getLogger(OlapGroupCommitInsertExecutor.class);

    public OlapGroupCommitInsertExecutor(ConnectContext ctx, Table table,
            String labelName, NereidsPlanner planner, Optional<InsertCommandContext> insertCtx,
            boolean emptyInsert) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert);
    }

    protected static void analyzeGroupCommit(ConnectContext ctx, TableIf table, UnboundTableSink<?> tableSink) {
        // The flag is set to false before execute sql, if it is true, this is a http stream
        if (ctx.isGroupCommit()) {
            return;
        }
        ctx.setGroupCommit(ctx.getSessionVariable().isEnableInsertGroupCommit() && !ctx.isTxnModel()
                && !ctx.getSessionVariable().isEnableUniqueKeyPartialUpdate() && table instanceof OlapTable
                && ((OlapTable) table).getTableProperty().getUseSchemaLightChange()
                && !((OlapTable) table).getQualifiedDbName().equalsIgnoreCase(FeConstants.INTERNAL_DB_NAME)
                && tableSink.getPartitions().isEmpty()
                && (!(table instanceof MTMV) || MTMVUtil.allowModifyMTMVData(ctx))
                && (tableSink.child() instanceof OneRowRelation || tableSink.child() instanceof LogicalUnion));
    }

    @Override
    public void beginTransaction() {
    }

    @Override
    protected void onComplete() {
        if (ctx.getState().getStateType() == MysqlStateType.ERR) {
            txnStatus = TransactionStatus.ABORTED;
        } else {
            txnStatus = TransactionStatus.PREPARE;
        }
    }

    @Override
    protected void onFail(Throwable t) {
        errMsg = t.getMessage() == null ? "unknown reason" : t.getMessage();
        String queryId = DebugUtil.printId(ctx.queryId());
        // if any throwable being thrown during insert operation, first we should abort this txn
        LOG.warn("insert [{}] with query id {} failed, url={}", labelName, queryId, coordinator.getTrackingUrl(), t);
        StringBuilder sb = new StringBuilder(t.getMessage());
        if (!Strings.isNullOrEmpty(coordinator.getTrackingUrl())) {
            sb.append(". url: ").append(coordinator.getTrackingUrl());
        }
        ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, sb.toString());
    }

    @Override
    protected void afterExec(StmtExecutor executor) {
        labelName = coordinator.getLabel();
        txnId = coordinator.getTxnId();
        setReturnInfo();
    }
}
