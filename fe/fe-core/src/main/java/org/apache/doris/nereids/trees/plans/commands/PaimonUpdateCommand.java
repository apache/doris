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
import org.apache.doris.catalog.Column;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonRowChangeOperation;
import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPaimonTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** UPDATE implementation for Paimon primary-key tables. */
public class PaimonUpdateCommand extends Command implements ForwardWithSync, Explainable {
    private final List<String> nameParts;
    private final String tableAlias;
    private final List<EqualTo> assignments;
    private final LogicalPlan logicalQuery;
    private final Optional<LogicalPlan> cte;

    /** Create a Paimon update command from the parsed UPDATE query. */
    public PaimonUpdateCommand(List<String> nameParts, String tableAlias,
            List<EqualTo> assignments, LogicalPlan logicalQuery, Optional<LogicalPlan> cte) {
        super(PlanType.UPDATE_COMMAND);
        this.nameParts = nameParts;
        this.tableAlias = tableAlias;
        this.assignments = assignments;
        this.logicalQuery = logicalQuery;
        this.cte = cte;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        new InsertIntoTableCommand(buildPlan(ctx), Optional.empty(), Optional.empty(),
                Optional.empty(), true, Optional.empty()).run(ctx, executor);
    }

    private LogicalPlan buildPlan(ConnectContext ctx) {
        PaimonExternalTable table = (PaimonExternalTable) RelationUtil.getTable(
                RelationUtil.getQualifierName(ctx, nameParts), ctx.getEnv(), Optional.empty());
        PaimonWriteTarget target = PaimonDmlCommandUtils.loadTarget(table);
        Map<String, Expression> changes = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo assignment : assignments) {
            List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
            UpdateCommand.checkAssignmentColumn(ctx, parts, nameParts, tableAlias);
            String column = parts.get(parts.size() - 1);
            if (changes.put(column, assignment.right()) != null) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                        "Duplicate column name in Paimon UPDATE: " + column);
            }
        }
        PaimonDmlCommandUtils.checkUpdate(target, changes.keySet());

        String targetName = tableAlias != null ? tableAlias : Util.getTempTableDisplayName(table.getName());
        List<NamedExpression> projects = new ArrayList<>();
        projects.add(new UnboundAlias(new TinyIntLiteral(PaimonRowChangeOperation.UPDATE),
                PaimonRowChangeOperation.OPERATION_COLUMN));
        for (Column column : target.getSchema()) {
            Expression value = changes.remove(column.getName());
            if (value == null) {
                value = new UnboundSlot(targetName, column.getName());
            }
            projects.add(value instanceof NamedExpression
                    ? (NamedExpression) value : new UnboundAlias(value, column.getName()));
        }
        if (!changes.isEmpty()) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "Unknown column in Paimon UPDATE: " + String.join(", ", changes.keySet()));
        }

        LogicalPlan project = new LogicalProject<>(projects, logicalQuery);
        if (cte.isPresent()) {
            project = (LogicalPlan) cte.get().withChildren(project);
        }
        return new LogicalPaimonTableSink<>(
                (PaimonExternalDatabase) table.getDatabase(), target, target.getSchema(), projects,
                DMLCommandType.UPDATE, Optional.empty(), Optional.empty(), project);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return buildPlan(ctx);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.UPDATE;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }
}
