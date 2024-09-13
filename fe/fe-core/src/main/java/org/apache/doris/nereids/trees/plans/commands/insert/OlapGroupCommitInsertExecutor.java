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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

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

    protected static void analyzeGroupCommit(ConnectContext ctx, TableIf table, LogicalPlan logicalQuery,
            Optional<InsertCommandContext> insertCtx) {
        // The flag is set to false before execute sql, if it is true, this is a http stream
        if (ctx.isGroupCommit()) {
            return;
        }
        if (logicalQuery instanceof UnboundTableSink) {
            UnboundTableSink<?> tableSink = (UnboundTableSink<?>) logicalQuery;
            List<Pair<BooleanSupplier, Supplier<String>>> conditions = new ArrayList<>();
            conditions.add(Pair.of(() -> ctx.getSessionVariable().isEnableInsertGroupCommit(),
                    () -> "group_commit session variable: " + ctx.getSessionVariable().groupCommit));
            conditions.add(Pair.of(() -> !ctx.isTxnModel(), () -> "isTxnModel"));
            conditions.add(Pair.of(() -> !ctx.getSessionVariable().isEnableUniqueKeyPartialUpdate(),
                    () -> "enableUniqueKeyPartialUpdate"));
            conditions.add(Pair.of(() -> table instanceof OlapTable,
                    () -> "not olapTable, class: " + table.getClass().getName()));
            conditions.add(Pair.of(() -> ((OlapTable) table).getTableProperty().getUseSchemaLightChange(),
                    () -> "notUseSchemaLightChange"));
            conditions.add(Pair.of(
                    () -> !((OlapTable) table).getQualifiedDbName().equalsIgnoreCase(FeConstants.INTERNAL_DB_NAME),
                    () -> "db is internal"));
            conditions.add(Pair.of(() -> tableSink.getPartitions().isEmpty(),
                    () -> "partitions is empty: " + tableSink.getPartitions()));
            conditions.add(Pair.of(() -> (!(table instanceof MTMV) || MTMVUtil.allowModifyMTMVData(ctx)),
                    () -> "not allowModifyMTMVData"));
            conditions.add(Pair.of(() -> !(insertCtx.isPresent() && insertCtx.get() instanceof OlapInsertCommandContext
                    && ((OlapInsertCommandContext) insertCtx.get()).isOverwrite()), () -> "is overwrite command"));
            conditions.add(Pair.of(
                    () -> tableSink.child() instanceof OneRowRelation || tableSink.child() instanceof LogicalUnion,
                    () -> "not one row relation or union, class: " + tableSink.child().getClass().getName()));
            ctx.setGroupCommit(conditions.stream().allMatch(p -> p.first.getAsBoolean()));
            if (!ctx.isGroupCommit() && LOG.isDebugEnabled()) {
                for (Pair<BooleanSupplier, Supplier<String>> pair : conditions) {
                    if (pair.first.getAsBoolean() == false) {
                        LOG.debug("group commit is off for query_id: {}, table: {}, because: {}",
                                DebugUtil.printId(ctx.queryId()), table.getName(), pair.second.get());
                        break;
                    }
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("group commit is off for query_id: {}, table: {}, because logicalQuery class: {}",
                        DebugUtil.printId(ctx.queryId()), table.getName(), logicalQuery.getClass().getName());
            }
        }
    }

    @Override
    protected void beforeExec() {
        if (Env.getCurrentEnv().getGroupCommitManager().isBlock(this.table.getId())) {
            String msg = "insert table " + this.table.getId() + GroupCommitPlanner.SCHEMA_CHANGE;
            LOG.info(msg);
            throw new AnalysisException(msg);
        }
        try {
            this.coordinator.setGroupCommitBe(Env.getCurrentEnv().getGroupCommitManager()
                    .selectBackendForGroupCommit(table.getId(), ctx));
        } catch (LoadException | DdlException e) {
            throw new RuntimeException(e);
        }
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
