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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.paimon.PaimonRowChangeOperation;
import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.PaimonRowChangeSpec;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Builds Paimon changelog projections while binding against the current write target. */
final class PaimonRowChangePlanBuilder {
    private static final String BRANCH_LABEL = "__DORIS_PAIMON_MERGE_BRANCH__";
    private static final String MATCH_COUNT = "__DORIS_PAIMON_MERGE_MATCH_COUNT__";

    private PaimonRowChangePlanBuilder() {
    }

    static LogicalProject<?> build(
            PaimonWriteTarget target, PaimonRowChangeSpec spec, LogicalPlan child) {
        checkCapabilities(target, spec);
        LogicalProject<?> project;
        if (spec instanceof PaimonRowChangeSpec.Update) {
            project = buildUpdate(target,
                    (PaimonRowChangeSpec.Update) spec, child);
        } else if (spec instanceof PaimonRowChangeSpec.Delete) {
            project = buildDelete(target, (PaimonRowChangeSpec.Delete) spec, child);
        } else if (spec instanceof PaimonRowChangeSpec.Merge) {
            project = buildMerge(target,
                    (PaimonRowChangeSpec.Merge) spec, child);
        } else {
            throw new AnalysisException("Unsupported Paimon row-change specification: "
                    + spec.getClass().getSimpleName());
        }
        return project;
    }

    private static void checkCapabilities(PaimonWriteTarget target, PaimonRowChangeSpec spec) {
        if (spec instanceof PaimonRowChangeSpec.Update) {
            PaimonRowChangeCapabilities.checkUpdate(target,
                    updatedColumns(((PaimonRowChangeSpec.Update) spec).getAssignments()));
            return;
        }
        if (spec instanceof PaimonRowChangeSpec.Delete) {
            PaimonRowChangeCapabilities.checkDelete(target);
            return;
        }
        if (!(spec instanceof PaimonRowChangeSpec.Merge)) {
            throw new AnalysisException("Unsupported Paimon row-change specification: "
                    + spec.getClass().getSimpleName());
        }
        PaimonRowChangeSpec.Merge merge = (PaimonRowChangeSpec.Merge) spec;
        Set<String> updatedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        boolean containsUpdate = false;
        boolean containsDelete = false;
        for (MergeMatchedClause clause : merge.getMatchedClauses()) {
            containsDelete |= clause.isDelete();
            containsUpdate |= !clause.isDelete();
            updatedColumns.addAll(updatedColumns(clause.getAssignments()));
        }
        PaimonRowChangeCapabilities.checkMerge(
                target, updatedColumns, containsUpdate, containsDelete);
    }

    private static Set<String> updatedColumns(List<EqualTo> assignments) {
        Set<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo assignment : assignments) {
            List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
            columns.add(parts.get(parts.size() - 1));
        }
        return columns;
    }

    private static LogicalProject<?> buildUpdate(PaimonWriteTarget target,
            PaimonRowChangeSpec.Update update, LogicalPlan child) {
        Map<String, Expression> changes = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo assignment : update.getAssignments()) {
            List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
            String column = parts.get(parts.size() - 1);
            if (changes.put(column, assignment.right()) != null) {
                throw new AnalysisException("Duplicate column name in Paimon UPDATE: " + column);
            }
        }

        String targetName = update.getTableAlias() != null
                ? update.getTableAlias()
                : Util.getTempTableDisplayName(target.getDorisTable().getName());
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
            throw new AnalysisException(
                    "Unknown column in Paimon UPDATE: " + String.join(", ", changes.keySet()));
        }
        return new LogicalProject<>(projects, child);
    }

    private static LogicalProject<?> buildDelete(PaimonWriteTarget target,
            PaimonRowChangeSpec.Delete delete, LogicalPlan child) {
        String targetName = delete.getTableAlias() != null
                ? delete.getTableAlias()
                : Util.getTempTableDisplayName(target.getDorisTable().getName());
        List<NamedExpression> projects = new ArrayList<>();
        projects.add(new UnboundAlias(new TinyIntLiteral(PaimonRowChangeOperation.DELETE),
                PaimonRowChangeOperation.OPERATION_COLUMN));
        for (Column column : target.getSchema()) {
            projects.add(new UnboundSlot(targetName, column.getName()));
        }
        return new LogicalProject<>(projects, child);
    }

    private static LogicalProject<?> buildMerge(PaimonWriteTarget target,
            PaimonRowChangeSpec.Merge merge, LogicalPlan child) {
        LogicalPlan plan = child;
        Expression targetPresent = targetPresence(target, merge.getTargetNameInPlan());
        if (!merge.getMatchedClauses().isEmpty()) {
            plan = addTargetMatchCount(plan, target, merge.getTargetNameInPlan());
            targetPresent = new And(targetPresent, new AssertTrue(
                    new LessThanEqual(new UnboundSlot(MATCH_COUNT), new BigIntLiteral(1)),
                    new VarcharLiteral("Paimon MERGE matched one target row with multiple source rows")));
        }
        plan = new LogicalProject<>(ImmutableList.of(
                new UnboundStar(ImmutableList.of()), generateBranchLabel(merge, targetPresent)), plan);
        plan = new LogicalFilter<>(
                ImmutableSet.of(new Not(new IsNull(new UnboundSlot(BRANCH_LABEL)))), plan);

        List<List<Expression>> branchProjections = new ArrayList<>();
        for (MergeMatchedClause clause : merge.getMatchedClauses()) {
            branchProjections.add(clause.isDelete()
                    ? buildDeleteProjection(target, merge.getTargetNameInPlan())
                    : buildUpdateProjection(target, merge, clause));
        }
        for (MergeNotMatchedClause clause : merge.getNotMatchedClauses()) {
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
        return new LogicalProject<>(generateFinalProjections(outputNames, branchProjections), plan);
    }

    private static Expression targetPresence(
            PaimonWriteTarget target, List<String> targetNameInPlan) {
        String primaryKey = target.getTable().primaryKeys().get(0);
        List<String> parts = Lists.newArrayList(targetNameInPlan);
        parts.add(primaryKey);
        return new Not(new IsNull(new UnboundSlot(parts)));
    }

    private static LogicalPlan addTargetMatchCount(LogicalPlan plan,
            PaimonWriteTarget target, List<String> targetNameInPlan) {
        List<Expression> partitionKeys = new ArrayList<>();
        for (String primaryKey : target.getTable().primaryKeys()) {
            partitionKeys.add(findTargetSlot(plan, targetNameInPlan, primaryKey));
        }
        WindowExpression countMatches = new WindowExpression(
                new Count(partitionKeys.get(0)),
                partitionKeys, ImmutableList.of());
        return new LogicalWindow<>(ImmutableList.of(
                new Alias(countMatches, MATCH_COUNT)), plan);
    }

    private static Slot findTargetSlot(
            LogicalPlan plan, List<String> targetNameInPlan, String columnName) {
        List<Slot> matches = plan.getOutput().stream()
                .filter(slot -> slot.getName().equalsIgnoreCase(columnName))
                .filter(slot -> qualifierEndsWith(slot.getQualifier(), targetNameInPlan))
                .collect(ImmutableList.toImmutableList());
        if (matches.size() != 1) {
            throw new AnalysisException("Unable to resolve Paimon MERGE target column '"
                    + String.join(".", targetNameInPlan) + "." + columnName + "'");
        }
        return matches.get(0);
    }

    private static boolean qualifierEndsWith(List<String> qualifier, List<String> suffix) {
        if (qualifier.size() < suffix.size()) {
            return false;
        }
        int offset = qualifier.size() - suffix.size();
        for (int i = 0; i < suffix.size(); i++) {
            if (!qualifier.get(offset + i).equalsIgnoreCase(suffix.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static NamedExpression generateBranchLabel(
            PaimonRowChangeSpec.Merge merge, Expression targetPresent) {
        Expression matchedLabel = new NullLiteral();
        for (int i = merge.getMatchedClauses().size() - 1; i >= 0; i--) {
            MergeMatchedClause clause = merge.getMatchedClauses().get(i);
            if (i != merge.getMatchedClauses().size() - 1
                    && !clause.getCasePredicate().isPresent()) {
                throw new AnalysisException("Only the last matched clause may omit its condition");
            }
            Expression result = new IntegerLiteral(i);
            matchedLabel = clause.getCasePredicate().isPresent()
                    ? new If(clause.getCasePredicate().get(), result, matchedLabel) : result;
        }
        Expression notMatchedLabel = new NullLiteral();
        for (int i = merge.getNotMatchedClauses().size() - 1; i >= 0; i--) {
            MergeNotMatchedClause clause = merge.getNotMatchedClauses().get(i);
            if (i != merge.getNotMatchedClauses().size() - 1
                    && !clause.getCasePredicate().isPresent()) {
                throw new AnalysisException("Only the last not matched clause may omit its condition");
            }
            Expression result = new IntegerLiteral(i + merge.getMatchedClauses().size());
            notMatchedLabel = clause.getCasePredicate().isPresent()
                    ? new If(clause.getCasePredicate().get(), result, notMatchedLabel) : result;
        }
        return new UnboundAlias(new If(targetPresent, matchedLabel, notMatchedLabel), BRANCH_LABEL);
    }

    private static List<Expression> buildDeleteProjection(
            PaimonWriteTarget target, List<String> targetNameInPlan) {
        List<Expression> output = new ArrayList<>();
        output.add(new TinyIntLiteral(PaimonRowChangeOperation.DELETE));
        for (Column column : target.getSchema()) {
            output.add(targetSlot(targetNameInPlan, column.getName()));
        }
        return output;
    }

    private static List<Expression> buildUpdateProjection(PaimonWriteTarget target,
            PaimonRowChangeSpec.Merge merge, MergeMatchedClause clause) {
        Map<String, Expression> changes = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo assignment : clause.getAssignments()) {
            List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
            String column = parts.get(parts.size() - 1);
            if (changes.put(column, assignment.right()) != null) {
                throw new AnalysisException("Duplicate column name in Paimon MERGE UPDATE: " + column);
            }
        }
        List<Expression> output = new ArrayList<>();
        output.add(new TinyIntLiteral(PaimonRowChangeOperation.UPDATE));
        for (Column column : target.getSchema()) {
            output.add(changes.containsKey(column.getName())
                    ? changes.remove(column.getName())
                    : targetSlot(merge.getTargetNameInPlan(), column.getName()));
        }
        if (!changes.isEmpty()) {
            throw new AnalysisException("Unknown column in Paimon MERGE UPDATE: "
                    + String.join(", ", changes.keySet()));
        }
        return output;
    }

    private static List<Expression> buildInsertProjection(
            PaimonWriteTarget target, MergeNotMatchedClause clause) {
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
                throw new AnalysisException(
                        "Missing column in Paimon MERGE INSERT: " + column.getName());
            }
            output.add(value);
        }
        if (!values.isEmpty()) {
            throw new AnalysisException("Unknown column in Paimon MERGE INSERT: "
                    + String.join(", ", values.keySet()));
        }
        return output;
    }

    private static Expression unwrap(NamedExpression expression) {
        return expression instanceof Alias || expression instanceof UnboundAlias
                ? expression.child(0) : expression;
    }

    private static Expression targetSlot(List<String> targetNameInPlan, String column) {
        List<String> parts = Lists.newArrayList(targetNameInPlan);
        parts.add(column);
        return new UnboundSlot(parts);
    }

    private static List<NamedExpression> generateFinalProjections(
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
}
