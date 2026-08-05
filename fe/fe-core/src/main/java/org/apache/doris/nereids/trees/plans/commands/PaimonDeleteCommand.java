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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** DELETE implementation for Paimon primary-key tables. */
public class PaimonDeleteCommand extends Command implements ForwardWithSync, Explainable {
    private final List<String> nameParts;
    private final String tableAlias;
    private final LogicalPlan logicalQuery;

    /** Create a Paimon delete command from the parsed DELETE query. */
    public PaimonDeleteCommand(List<String> nameParts, String tableAlias, LogicalPlan logicalQuery) {
        super(PlanType.DELETE_COMMAND);
        this.nameParts = nameParts;
        this.tableAlias = tableAlias;
        this.logicalQuery = logicalQuery;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        new InsertIntoTableCommand(buildPlan(ctx), Optional.empty(), Optional.empty(),
                Optional.empty(), false, Optional.empty()).run(ctx, executor);
    }

    private LogicalPlan buildPlan(ConnectContext ctx) {
        PaimonExternalTable table = (PaimonExternalTable) RelationUtil.getTable(
                RelationUtil.getQualifierName(ctx, nameParts), ctx.getEnv(), Optional.empty());
        PaimonWriteTarget target = PaimonDmlCommandUtils.loadTarget(table);
        PaimonDmlCommandUtils.checkDelete(target);
        String targetName = tableAlias != null ? tableAlias : Util.getTempTableDisplayName(table.getName());

        List<NamedExpression> projects = new ArrayList<>();
        projects.add(new UnboundAlias(new TinyIntLiteral(PaimonRowChangeOperation.DELETE),
                PaimonRowChangeOperation.OPERATION_COLUMN));
        for (Column column : target.getSchema()) {
            projects.add(new UnboundSlot(targetName, column.getName()));
        }
        LogicalProject<LogicalPlan> project = new LogicalProject<>(projects, logicalQuery);
        return new LogicalPaimonTableSink<>(
                (PaimonExternalDatabase) table.getDatabase(), target, target.getSchema(), projects,
                DMLCommandType.DELETE, Optional.empty(), Optional.empty(), project);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return buildPlan(ctx);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DELETE;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }
}
