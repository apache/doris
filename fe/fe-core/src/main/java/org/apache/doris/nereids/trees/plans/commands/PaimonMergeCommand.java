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
import org.apache.doris.nereids.analyzer.UnboundPaimonTableSink;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.LogicalPlanBuilderAssistant;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.PaimonRowChangeSpec;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/** MERGE INTO implementation for Paimon primary-key tables. */
public class PaimonMergeCommand extends Command implements ForwardWithSync, Explainable {
    private final List<String> targetNameParts;
    private final Optional<String> targetAlias;
    private final Optional<LogicalPlan> cte;
    private final LogicalPlan source;
    private final Expression onClause;
    private final List<MergeMatchedClause> matchedClauses;
    private final List<MergeNotMatchedClause> notMatchedClauses;

    /** Create a Paimon merge command from the parsed MERGE query. */
    public PaimonMergeCommand(List<String> targetNameParts, Optional<String> targetAlias,
            Optional<LogicalPlan> cte, LogicalPlan source, Expression onClause,
            List<MergeMatchedClause> matchedClauses,
            List<MergeNotMatchedClause> notMatchedClauses) {
        super(PlanType.MERGE_INTO_COMMAND);
        this.targetNameParts = targetNameParts;
        this.targetAlias = targetAlias;
        this.cte = cte;
        this.source = source;
        this.onClause = onClause;
        this.matchedClauses = matchedClauses;
        this.notMatchedClauses = notMatchedClauses;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        new InsertIntoTableCommand(buildPlan(ctx), Optional.empty(), Optional.empty(), cte)
                .run(ctx, executor);
    }

    private LogicalPlan buildPlan(ConnectContext ctx) {
        for (MergeMatchedClause clause : matchedClauses) {
            for (EqualTo assignment : clause.getAssignments()) {
                UpdateCommand.checkAssignmentColumn(ctx,
                        ((UnboundSlot) assignment.left()).getNameParts(),
                        targetNameParts, targetAlias.orElse(null));
            }
        }
        List<String> targetNameInPlan = targetAlias.isPresent()
                ? ImmutableList.of(targetAlias.get())
                : RelationUtil.getQualifierName(ctx, targetNameParts);
        PaimonRowChangeSpec.Merge spec = new PaimonRowChangeSpec.Merge(
                targetNameInPlan, matchedClauses, notMatchedClauses);
        return new UnboundPaimonTableSink<>(targetNameParts, generateBasePlan(), spec);
    }

    private LogicalPlan generateBasePlan() {
        LogicalPlan target = LogicalPlanBuilderAssistant.withCheckPolicy(
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), targetNameParts));
        if (targetAlias.isPresent()) {
            target = new LogicalSubQueryAlias<>(targetAlias.get(), target);
        }
        JoinType joinType = notMatchedClauses.isEmpty()
                ? JoinType.INNER_JOIN : JoinType.LEFT_OUTER_JOIN;
        return new LogicalJoin<>(joinType, ImmutableList.of(), ImmutableList.of(onClause),
                source, target, JoinReorderContext.EMPTY);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        LogicalPlan plan = buildPlan(ctx);
        return cte.isPresent() ? cte.get().withChildren(plan) : plan;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.MERGE_INTO;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }
}
