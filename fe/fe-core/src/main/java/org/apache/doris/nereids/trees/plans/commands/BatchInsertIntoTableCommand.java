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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalInlineTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * insert into values with in txn model.
 */
public class BatchInsertIntoTableCommand extends Command implements ForwardWithSync, Explainable {

    public static final Logger LOG = LogManager.getLogger(BatchInsertIntoTableCommand.class);

    private LogicalPlan logicalQuery;

    public BatchInsertIntoTableCommand(LogicalPlan logicalQuery) {
        super(PlanType.BATCH_INSERT_INTO_TABLE_COMMAND);
        this.logicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery should not be null");
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) throws Exception {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }
        return InsertExecutor.normalizePlan(this.logicalQuery, InsertExecutor.getTargetTable(this.logicalQuery, ctx));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitBatchInsertIntoTableCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }

        UnboundTableSink<? extends Plan> unboundTableSink = (UnboundTableSink<? extends Plan>) logicalQuery;
        Plan query = unboundTableSink.child();
        if (!(query instanceof LogicalInlineTable)) {
            throw new AnalysisException("Insert into ** select is not supported in a transaction");
        }

        PhysicalOlapTableSink<?> sink;
        TableIf targetTableIf = InsertExecutor.getTargetTable(logicalQuery, ctx);
        targetTableIf.readLock();
        try {
            this.logicalQuery = (LogicalPlan) InsertExecutor.normalizePlan(logicalQuery, targetTableIf);
            LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
            NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
            planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
            executor.checkBlockRules();
            if (ctx.getMysqlChannel() != null) {
                ctx.getMysqlChannel().reset();
            }

            Optional<TreeNode<?>> plan = planner.getPhysicalPlan()
                    .<Set<TreeNode<?>>>collect(PhysicalOlapTableSink.class::isInstance).stream().findAny();
            Preconditions.checkArgument(plan.isPresent(), "insert into command must contain OlapTableSinkNode");
            sink = ((PhysicalOlapTableSink<?>) plan.get());
            Table targetTable = sink.getTargetTable();
            // should set columns of sink since we maybe generate some invisible columns
            List<Column> fullSchema = sink.getTargetTable().getFullSchema();
            List<Column> targetSchema = Lists.newArrayList();
            if (sink.isPartialUpdate()) {
                List<String> partialUpdateColumns = sink.getCols().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
                for (Column column : fullSchema) {
                    if (partialUpdateColumns.contains(column.getName())) {
                        targetSchema.add(column);
                    }
                }
            } else {
                targetSchema = fullSchema;
            }
            // check auth
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), targetTable.getQualifiedDbName(), targetTable.getName(),
                            PrivPredicate.LOAD)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                        targetTable.getQualifiedDbName() + ": " + targetTable.getName());
            }

            Optional<PhysicalUnion> union = planner.getPhysicalPlan()
                    .<Set<PhysicalUnion>>collect(PhysicalUnion.class::isInstance).stream().findAny();
            if (union.isPresent()) {
                InsertExecutor.executeBatchInsertTransaction(ctx, targetTable.getQualifiedDbName(),
                        targetTable.getName(), targetSchema, union.get().getConstantExprsList());
                return;
            }
            Optional<PhysicalOneRowRelation> oneRowRelation = planner.getPhysicalPlan()
                    .<Set<PhysicalOneRowRelation>>collect(PhysicalOneRowRelation.class::isInstance).stream().findAny();
            if (oneRowRelation.isPresent()) {
                InsertExecutor.executeBatchInsertTransaction(ctx, targetTable.getQualifiedDbName(),
                        targetTable.getName(), targetSchema, ImmutableList.of(oneRowRelation.get().getProjects()));
                return;
            }
            // TODO: update error msg
            throw new AnalysisException("could not run this sql");
        } finally {
            targetTableIf.readUnlock();
        }
    }
}
