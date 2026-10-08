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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.IsTrue;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;

/** Copy-on-write DELETE for plugin connectors that provide atomic full-table overwrite. */
public final class ConnectorDeleteCommand extends Command implements ForwardWithSync, Explainable {
    private final List<String> nameParts;
    private final boolean isTempPart;
    private final List<String> partitions;
    private final LogicalPlan logicalQuery;

    public ConnectorDeleteCommand(List<String> nameParts, boolean isTempPart,
            List<String> partitions, LogicalPlan logicalQuery) {
        super(PlanType.DELETE_COMMAND);
        this.nameParts = ImmutableList.copyOf(nameParts);
        this.isTempPart = isTempPart;
        this.partitions = ImmutableList.copyOf(partitions);
        this.logicalQuery = logicalQuery;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        PluginDrivenExternalTable table = requireDeleteTable(ctx);
        ConnectorCopyOnWriteUtils.requireUnrestrictedSource(ctx, table);
        buildOverwriteCommand().run(ctx, executor);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        PluginDrivenExternalTable table = requireDeleteTable(ctx);
        ConnectorCopyOnWriteUtils.requireUnrestrictedSource(ctx, table);
        return buildOverwriteCommand().getExplainPlan(ctx);
    }

    private PluginDrivenExternalTable requireDeleteTable(ConnectContext ctx) {
        requireFullTableDelete(isTempPart, partitions);
        List<String> qualifiedName = RelationUtil.getQualifierName(ctx, nameParts);
        TableIf table = RelationUtil.getTable(qualifiedName, ctx.getEnv(), Optional.empty());
        if (!(table instanceof PluginDrivenExternalTable)) {
            throw new AnalysisException("Connector DELETE requires a plugin-driven external table");
        }
        PluginDrivenExternalTable connectorTable = (PluginDrivenExternalTable) table;
        if (!connectorTable.connectorSupportedWriteOperations().contains(WriteOperation.DELETE)
                || !connectorTable.connectorSupportsCopyOnWriteDml()) {
            throw new AnalysisException("Connector does not support DELETE for table: "
                    + connectorTable.getName());
        }
        return connectorTable;
    }

    private InsertOverwriteTableCommand buildOverwriteCommand() {
        LogicalPlan survivorQuery = buildSurvivorQuery(logicalQuery);
        LogicalPlan sink = (LogicalPlan) UnboundTableSinkCreator.createUnboundTableSink(
                nameParts, ImmutableList.of(), ImmutableList.of(), false, ImmutableList.of(),
                false, TPartialUpdateNewRowPolicy.APPEND, DMLCommandType.DELETE, survivorQuery);
        return new InsertOverwriteTableCommand(
                sink, Optional.empty(), Optional.empty(), Optional.empty());
    }

    static LogicalPlan buildSurvivorQuery(LogicalPlan deleteQuery) {
        if (deleteQuery instanceof LogicalFilter) {
            LogicalFilter<?> deleteFilter = (LogicalFilter<?>) deleteQuery;
            Not survivorPredicate = new Not(new IsTrue(deleteFilter.getPredicate()));
            return new LogicalFilter<>(ImmutableSet.of(survivorPredicate),
                    (LogicalPlan) deleteFilter.child());
        }
        return new LogicalFilter<>(ImmutableSet.of(BooleanLiteral.FALSE), deleteQuery);
    }

    static void requireFullTableDelete(boolean isTempPart, List<String> partitions) {
        if (isTempPart || !partitions.isEmpty()) {
            throw new AnalysisException(
                    "Connector copy-on-write DELETE currently supports full tables only");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DELETE;
    }
}
