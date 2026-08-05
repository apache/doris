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
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonRowChangeOperation;
import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.LogicalPlanBuilderAssistant;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPaimonTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/** MERGE INTO implementation for Paimon primary-key tables. */
public class PaimonMergeCommand extends Command implements ForwardWithSync, Explainable {
    private static final String BRANCH_LABEL = "__DORIS_PAIMON_MERGE_BRANCH__";

    private final List<String> targetNameParts;
    private final Optional<String> targetAlias;
    private final List<String> targetNameInPlan;
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
        this.targetNameInPlan = targetAlias.isPresent()
                ? ImmutableList.of(targetAlias.get()) : targetNameParts;
        this.cte = cte;
        this.source = source;
        this.onClause = onClause;
        this.matchedClauses = matchedClauses;
        this.notMatchedClauses = notMatchedClauses;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        new InsertIntoTableCommand(buildPlan(ctx), Optional.empty(), Optional.empty(),
                Optional.empty(), false, Optional.empty()).run(ctx, executor);
    }

    private LogicalPlan buildPlan(ConnectContext ctx) {
        PaimonExternalTable table = (PaimonExternalTable) RelationUtil.getTable(
                RelationUtil.getQualifierName(ctx, targetNameParts), ctx.getEnv(), Optional.empty());
        PaimonWriteTarget target = PaimonDmlCommandUtils.loadTarget(table);
        Set<String> updatedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        boolean containsUpdate = false;
        boolean containsDelete = false;
        for (MergeMatchedClause clause : matchedClauses) {
            containsDelete |= clause.isDelete();
            containsUpdate |= !clause.isDelete();
            for (EqualTo assignment : clause.getAssignments()) {
                List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
                updatedColumns.add(parts.get(parts.size() - 1));
            }
        }
        PaimonDmlCommandUtils.checkMerge(
                target, updatedColumns, containsUpdate, containsDelete);

        LogicalPlan plan = generateBasePlan();
        Expression targetPresent = targetPresence(target);
        plan = new LogicalProject<>(ImmutableList.of(
                new UnboundStar(ImmutableList.of()), generateBranchLabel(targetPresent)), plan);
        plan = new LogicalFilter<>(
                ImmutableSet.of(new Not(new IsNull(new UnboundSlot(BRANCH_LABEL)))), plan);

        List<List<Expression>> branchProjections = new ArrayList<>();
        for (MergeMatchedClause clause : matchedClauses) {
            branchProjections.add(clause.isDelete()
                    ? buildDeleteProjection(target)
                    : buildUpdateProjection(ctx, target, clause));
        }
        for (MergeNotMatchedClause clause : notMatchedClauses) {
            branchProjections.add(buildInsertProjection(target, clause));
        }
        if (branchProjections.isEmpty()) {
            throw new AnalysisException("Paimon MERGE requires at least one WHEN clause");
        }

        List<String> outputNames = new ArrayList<>();
        outputNames.add(PaimonRowChangeOperation.OPERATION_COLUMN);
        for (Column column : target.getSchema()) {
            outputNames.add(column.getName());
        }
        List<NamedExpression> finalProjects = generateFinalProjections(outputNames, branchProjections);
        plan = new LogicalProject<>(finalProjects, plan);
        if (cte.isPresent()) {
            plan = (LogicalPlan) cte.get().withChildren(plan);
        }
        return new LogicalPaimonTableSink<>(
                (PaimonExternalDatabase) table.getDatabase(), target, target.getSchema(), finalProjects,
                DMLCommandType.MERGE, Optional.empty(), Optional.empty(), plan);
    }

    private LogicalPlan generateBasePlan() {
        LogicalPlan target = LogicalPlanBuilderAssistant.withCheckPolicy(
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), targetNameParts));
        if (targetAlias.isPresent()) {
            target = new LogicalSubQueryAlias<>(targetAlias.get(), target);
        }
        JoinType joinType = notMatchedClauses.isEmpty() ? JoinType.INNER_JOIN : JoinType.LEFT_OUTER_JOIN;
        return new LogicalJoin<>(joinType, ImmutableList.of(), ImmutableList.of(onClause),
                source, target, JoinReorderContext.EMPTY);
    }

    private Expression targetPresence(PaimonWriteTarget target) {
        String primaryKey = target.getTable().primaryKeys().get(0);
        List<String> parts = Lists.newArrayList(targetNameInPlan);
        parts.add(primaryKey);
        return new Not(new IsNull(new UnboundSlot(parts)));
    }

    private NamedExpression generateBranchLabel(Expression targetPresent) {
        Expression matchedLabel = new NullLiteral();
        for (int i = matchedClauses.size() - 1; i >= 0; i--) {
            MergeMatchedClause clause = matchedClauses.get(i);
            if (i != matchedClauses.size() - 1 && !clause.getCasePredicate().isPresent()) {
                throw new AnalysisException("Only the last matched clause may omit its condition");
            }
            Expression result = new IntegerLiteral(i);
            matchedLabel = clause.getCasePredicate().isPresent()
                    ? new If(clause.getCasePredicate().get(), result, matchedLabel) : result;
        }
        Expression notMatchedLabel = new NullLiteral();
        for (int i = notMatchedClauses.size() - 1; i >= 0; i--) {
            MergeNotMatchedClause clause = notMatchedClauses.get(i);
            if (i != notMatchedClauses.size() - 1 && !clause.getCasePredicate().isPresent()) {
                throw new AnalysisException("Only the last not matched clause may omit its condition");
            }
            Expression result = new IntegerLiteral(i + matchedClauses.size());
            notMatchedLabel = clause.getCasePredicate().isPresent()
                    ? new If(clause.getCasePredicate().get(), result, notMatchedLabel) : result;
        }
        return new UnboundAlias(new If(targetPresent, matchedLabel, notMatchedLabel), BRANCH_LABEL);
    }

    private List<Expression> buildDeleteProjection(PaimonWriteTarget target) {
        List<Expression> output = new ArrayList<>();
        output.add(new TinyIntLiteral(PaimonRowChangeOperation.DELETE));
        for (Column column : target.getSchema()) {
            output.add(targetSlot(column.getName()));
        }
        return output;
    }

    private List<Expression> buildUpdateProjection(ConnectContext ctx, PaimonWriteTarget target,
            MergeMatchedClause clause) {
        Map<String, Expression> changes = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo assignment : clause.getAssignments()) {
            List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
            UpdateCommand.checkAssignmentColumn(
                    ctx, parts, targetNameParts, targetAlias.orElse(null));
            String column = parts.get(parts.size() - 1);
            if (changes.put(column, assignment.right()) != null) {
                throw new AnalysisException("Duplicate column name in Paimon MERGE UPDATE: " + column);
            }
        }
        List<Expression> output = new ArrayList<>();
        output.add(new TinyIntLiteral(PaimonRowChangeOperation.UPDATE));
        for (Column column : target.getSchema()) {
            output.add(changes.containsKey(column.getName())
                    ? changes.remove(column.getName()) : targetSlot(column.getName()));
        }
        if (!changes.isEmpty()) {
            throw new AnalysisException("Unknown column in Paimon MERGE UPDATE: "
                    + String.join(", ", changes.keySet()));
        }
        return output;
    }

    private List<Expression> buildInsertProjection(PaimonWriteTarget target,
            MergeNotMatchedClause clause) {
        if (clause.getRow().size() != target.getSchema().size()) {
            throw new AnalysisException(
                    "Paimon MERGE INSERT currently requires values for every table column");
        }
        Map<String, Expression> values = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        if (!clause.getColNames().isEmpty()) {
            if (clause.getColNames().size() != clause.getRow().size()
                    || clause.getColNames().size() != target.getSchema().size()) {
                throw new AnalysisException(
                        "Paimon MERGE INSERT currently requires every table column");
            }
            for (int i = 0; i < clause.getColNames().size(); i++) {
                if (values.put(clause.getColNames().get(i), unwrap(clause.getRow().get(i))) != null) {
                    throw new AnalysisException("Duplicate column in Paimon MERGE INSERT");
                }
            }
        }
        List<Expression> output = new ArrayList<>();
        output.add(new TinyIntLiteral(PaimonRowChangeOperation.INSERT));
        for (int i = 0; i < target.getSchema().size(); i++) {
            Column column = target.getSchema().get(i);
            Expression value = clause.getColNames().isEmpty()
                    ? unwrap(clause.getRow().get(i)) : values.remove(column.getName());
            if (value == null) {
                throw new AnalysisException("Missing column in Paimon MERGE INSERT: " + column.getName());
            }
            output.add(value);
        }
        if (!values.isEmpty()) {
            throw new AnalysisException("Unknown column in Paimon MERGE INSERT: "
                    + String.join(", ", values.keySet()));
        }
        return output;
    }

    private Expression unwrap(NamedExpression expression) {
        return expression instanceof Alias || expression instanceof UnboundAlias
                ? expression.child(0) : expression;
    }

    private Expression targetSlot(String column) {
        List<String> parts = Lists.newArrayList(targetNameInPlan);
        parts.add(column);
        return new UnboundSlot(parts);
    }

    private List<NamedExpression> generateFinalProjections(
            List<String> names, List<List<Expression>> branches) {
        List<NamedExpression> output = new ArrayList<>();
        for (int column = 0; column < branches.get(0).size(); column++) {
            Expression value = new NullLiteral();
            for (int branch = branches.size() - 1; branch >= 0; branch--) {
                value = new If(new EqualTo(new UnboundSlot(BRANCH_LABEL),
                        new IntegerLiteral(branch)), branches.get(branch).get(column), value);
            }
            output.add(new UnboundAlias(value, names.get(column)));
        }
        return output;
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return buildPlan(ctx);
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
