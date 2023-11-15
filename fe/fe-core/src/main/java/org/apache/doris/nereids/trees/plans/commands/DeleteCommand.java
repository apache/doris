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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.analyzer.UnboundOlapTableSink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * delete from unique key table.
 */
public class DeleteCommand extends Command implements ForwardWithSync, Explainable {
    private final List<String> nameParts;
    private final String tableAlias;
    private final List<String> partitions;
    private LogicalPlan logicalQuery;
    private OlapTable targetTable;
    private final Optional<LogicalPlan> cte;

    /**
     * constructor
     */
    public DeleteCommand(List<String> nameParts, String tableAlias, List<String> partitions,
            LogicalPlan logicalQuery, Optional<LogicalPlan> cte) {
        super(PlanType.DELETE_COMMAND);
        this.nameParts = Utils.copyRequiredList(nameParts);
        this.tableAlias = tableAlias;
        this.partitions = Utils.copyRequiredList(partitions);
        this.logicalQuery = logicalQuery;
        this.cte = cte;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        new InsertIntoTableCommand(completeQueryPlan(ctx, logicalQuery), Optional.empty(), false).run(ctx, executor);
    }

    private void checkTable(ConnectContext ctx) {
        List<String> qualifieredTableName = RelationUtil.getQualifierName(ctx, nameParts);
        TableIf table = RelationUtil.getTable(qualifieredTableName, ctx.getEnv());
        if (!(table instanceof OlapTable)) {
            throw new AnalysisException("table must be olapTable in delete command");
        }
        targetTable = ((OlapTable) table);
        if (targetTable.getKeysType() != KeysType.UNIQUE_KEYS) {
            throw new AnalysisException("Nereids only support delete command on unique key table now");
        }
    }

    /**
     * public for test
     */
    public LogicalPlan completeQueryPlan(ConnectContext ctx, LogicalPlan logicalQuery) {
        checkTable(ctx);

        // add select and insert node.
        List<NamedExpression> selectLists = Lists.newArrayList();
        List<String> cols = Lists.newArrayList();
        boolean isMow = targetTable.getEnableUniqueKeyMergeOnWrite();
        String tableName = tableAlias != null ? tableAlias : targetTable.getName();
        for (Column column : targetTable.getFullSchema()) {
            if (column.getName().equalsIgnoreCase(Column.DELETE_SIGN)) {
                selectLists.add(new Alias(new TinyIntLiteral(((byte) 1)), Column.DELETE_SIGN));
            } else if (column.getName().equalsIgnoreCase(Column.SEQUENCE_COL)) {
                selectLists.add(new UnboundSlot(tableName, targetTable.getSequenceMapCol()));
            } else if (column.isKey()) {
                selectLists.add(new UnboundSlot(tableName, column.getName()));
            } else if (!isMow && !column.isVisible()) {
                selectLists.add(new UnboundSlot(tableName, column.getName()));
            } else {
                continue;
            }
            cols.add(column.getName());
        }

        logicalQuery = new LogicalProject<>(selectLists, logicalQuery);
        if (cte.isPresent()) {
            logicalQuery = ((LogicalPlan) cte.get().withChildren(logicalQuery));
        }

        boolean isPartialUpdate = targetTable.getEnableUniqueKeyMergeOnWrite()
                && cols.size() < targetTable.getColumns().size();

        // make UnboundTableSink
        return new UnboundOlapTableSink<>(nameParts, cols, ImmutableList.of(),
                partitions, isPartialUpdate, logicalQuery);
    }

    public LogicalPlan getLogicalQuery() {
        return logicalQuery;
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return completeQueryPlan(ctx, logicalQuery);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDeleteCommand(this, context);
    }
}
