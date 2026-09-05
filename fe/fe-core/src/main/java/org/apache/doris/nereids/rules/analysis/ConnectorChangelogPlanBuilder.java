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
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ShortCircuitIf;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.ConnectorChangelogRowChangeSpec;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Builds the operation-column plus full-row projection used by changelog-oriented connectors. */
public final class ConnectorChangelogPlanBuilder {
    public static final String OPERATION_COLUMN = "__DORIS_PAIMON_ROW_KIND__";
    public static final byte INSERT = 0;
    public static final byte UPDATE = 1;
    public static final byte DELETE = 2;
    private static final String BRANCH_LABEL = "__DORIS_CHANGELOG_BRANCH__";

    private ConnectorChangelogPlanBuilder() {
    }

    /** Builds a changelog plan for the requested connector row-level operation. */
    public static LogicalPlan build(List<Column> schema, List<String> primaryKeys,
            ConnectorChangelogRowChangeSpec spec, LogicalPlan child, CascadesContext context) {
        if (spec instanceof ConnectorChangelogRowChangeSpec.Update) {
            return buildUpdate(schema, (ConnectorChangelogRowChangeSpec.Update) spec, child, context);
        }
        if (spec instanceof ConnectorChangelogRowChangeSpec.Delete) {
            return buildDelete(schema, primaryKeys, (ConnectorChangelogRowChangeSpec.Delete) spec,
                    child, context);
        }
        if (spec instanceof ConnectorChangelogRowChangeSpec.Merge) {
            return new MergeBuilder(schema, primaryKeys,
                    (ConnectorChangelogRowChangeSpec.Merge) spec, child, context).build();
        }
        throw new AnalysisException("Unsupported connector changelog specification: "
                + spec.getClass().getSimpleName());
    }

    private static LogicalPlan buildUpdate(List<Column> schema,
            ConnectorChangelogRowChangeSpec.Update update, LogicalPlan child,
            CascadesContext context) {
        Map<String, Expression> changes = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo assignment : update.getAssignments()) {
            List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
            String name = parts.get(parts.size() - 1);
            if (changes.put(name, assignment.right()) != null) {
                throw new AnalysisException("Duplicate column name in connector UPDATE: " + name);
            }
        }
        ExpressionAnalyzer analyzer = analyzer(child, context);
        List<NamedExpression> projects = new ArrayList<>();
        projects.add(operation(UPDATE));
        for (Column column : schema) {
            Expression value = changes.remove(column.getName());
            if (value == null) {
                value = targetSlot(update.getTargetNameInPlan(), column.getName());
            }
            projects.add(bindColumn(analyzer, value, column));
        }
        if (!changes.isEmpty()) {
            throw new AnalysisException("Unknown column in connector UPDATE: "
                    + String.join(", ", changes.keySet()));
        }
        return new LogicalProject<>(projects, child);
    }

    private static LogicalPlan buildDelete(List<Column> schema, List<String> primaryKeys,
            ConnectorChangelogRowChangeSpec.Delete delete, LogicalPlan child,
            CascadesContext context) {
        ExpressionAnalyzer analyzer = analyzer(child, context);
        List<NamedExpression> projects = new ArrayList<>();
        projects.add(operation(DELETE));
        for (Column column : schema) {
            projects.add(bindColumn(analyzer,
                    targetSlot(delete.getTargetNameInPlan(), column.getName()), column));
        }
        LogicalProject<LogicalPlan> project = new LogicalProject<>(projects, child);
        if (!delete.shouldDeduplicateTargetRows()) {
            return project;
        }
        Set<String> keys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        keys.addAll(primaryKeys);
        List<Expression> groupBy = new ArrayList<>();
        List<NamedExpression> outputs = new ArrayList<>();
        Slot operation = project.getOutput().get(0);
        groupBy.add(operation);
        outputs.add(operation);
        for (int i = 0; i < schema.size(); i++) {
            Column column = schema.get(i);
            Slot value = project.getOutput().get(i + 1);
            if (keys.contains(column.getName())) {
                groupBy.add(value);
                outputs.add(value);
            } else {
                outputs.add(new Alias(new AnyValue(value), column.getName()));
            }
        }
        return new LogicalAggregate<>(groupBy, outputs, project);
    }

    private static Alias operation(byte value) {
        return new Alias(new TinyIntLiteral(value), OPERATION_COLUMN);
    }

    private static UnboundSlot targetSlot(List<String> qualifier, String column) {
        List<String> parts = new ArrayList<>(qualifier);
        parts.add(column);
        return new UnboundSlot(parts);
    }

    private static ExpressionAnalyzer analyzer(LogicalPlan plan, CascadesContext context) {
        return new ExpressionAnalyzer(plan, new Scope(plan.getOutput()), context, true, false);
    }

    private static Alias bindColumn(ExpressionAnalyzer analyzer, Expression expression, Column column) {
        Expression value = analyzer.analyze(expression);
        value = TypeCoercionUtils.castIfNotSameType(value, DataType.fromCatalogType(column.getType()));
        return new Alias(value, column.getName());
    }

    private static final class MergeBuilder {
        private final List<Column> schema;
        private final List<String> primaryKeys;
        private final ConnectorChangelogRowChangeSpec.Merge merge;
        private final LogicalPlan child;
        private final ExpressionAnalyzer analyzer;

        private MergeBuilder(List<Column> schema, List<String> primaryKeys,
                ConnectorChangelogRowChangeSpec.Merge merge, LogicalPlan child,
                CascadesContext context) {
            this.schema = schema;
            this.primaryKeys = primaryKeys;
            this.merge = merge;
            this.child = child;
            this.analyzer = analyzer(child, context);
        }

        private LogicalPlan build() {
            if (primaryKeys.isEmpty()) {
                throw new AnalysisException("Connector MERGE requires a primary-key table");
            }
            Alias branch = bindBranchLabel();
            Slot branchSlot = branch.toSlot();
            List<NamedExpression> branchOutputs = new ArrayList<>(child.getOutput());
            branchOutputs.add(branch);
            LogicalPlan selected = new LogicalProject<>(branchOutputs, child);
            selected = new LogicalFilter<>(
                    ImmutableSet.of(new Not(new org.apache.doris.nereids.trees.expressions.IsNull(branchSlot))),
                    selected);
            List<List<Expression>> branches = buildBranchProjections();
            if (!merge.getNotMatchedClauses().isEmpty()) {
                validateNotMatchedPrimaryKeys(branches);
            }
            List<NamedExpression> output = new ArrayList<>();
            for (int column = 0; column <= schema.size(); column++) {
                DataType type = column == 0
                        ? org.apache.doris.nereids.types.TinyIntType.INSTANCE
                        : DataType.fromCatalogType(schema.get(column - 1).getType());
                String name = column == 0 ? OPERATION_COLUMN : schema.get(column - 1).getName();
                Expression value = new NullLiteral(type);
                for (int index = branches.size() - 1; index >= 0; index--) {
                    Expression branchValue = TypeCoercionUtils.castIfNotSameType(
                            branches.get(index).get(column), type);
                    value = new ShortCircuitIf(new EqualTo(branchSlot, new IntegerLiteral(index)),
                            branchValue, value);
                }
                output.add(new Alias(value, name));
            }
            return addCardinalityChecks(new LogicalProject<>(output, selected));
        }

        private void validateNotMatchedPrimaryKeys(List<List<Expression>> branches) {
            Map<String, Slot> targetKeys = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (String key : primaryKeys) {
                targetKeys.put(key, findTargetSlot(key));
            }
            Set<Slot> targetSlots = child.getOutput().stream()
                    .filter(slot -> qualifierEndsWith(slot.getQualifier(), merge.getTargetNameInPlan()))
                    .collect(ImmutableSet.toImmutableSet());
            if (!(child instanceof LogicalJoin)) {
                throw new AnalysisException("Paimon MERGE input must be a logical join");
            }
            Expression onClause = ((LogicalJoin<?, ?>) child).getOnClauseCondition()
                    .orElseThrow(() -> new AnalysisException("Paimon MERGE requires an ON condition"));
            Map<String, Expression> sourceKeys = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (Expression conjunct : ExpressionUtils.extractConjunction(onClause)) {
                if (!(conjunct instanceof EqualTo)) {
                    throw invalidNotMatchedKeyCondition();
                }
                EqualTo equality = (EqualTo) conjunct;
                String leftKey = targetPrimaryKeyName(equality.left(), targetKeys);
                String rightKey = targetPrimaryKeyName(equality.right(), targetKeys);
                if ((leftKey == null) == (rightKey == null)) {
                    throw invalidNotMatchedKeyCondition();
                }
                String key = leftKey != null ? leftKey : rightKey;
                Expression source = leftKey != null ? equality.right() : equality.left();
                if (source.getInputSlots().isEmpty()
                        || source.getInputSlots().stream().anyMatch(targetSlots::contains)
                        || source.containsNondeterministic()
                        || sourceKeys.put(key, source) != null) {
                    throw invalidNotMatchedKeyCondition();
                }
            }
            if (sourceKeys.size() != targetKeys.size()) {
                throw invalidNotMatchedKeyCondition();
            }
            int firstInsert = merge.getMatchedClauses().size();
            for (int branch = firstInsert; branch < branches.size(); branch++) {
                for (Map.Entry<String, Expression> sourceKey : sourceKeys.entrySet()) {
                    int column = schemaIndex(sourceKey.getKey()) + 1;
                    DataType type = DataType.fromCatalogType(schema.get(column - 1).getType());
                    if (!TypeCoercionUtils.castIfNotSameType(branches.get(branch).get(column), type)
                            .equals(TypeCoercionUtils.castIfNotSameType(sourceKey.getValue(), type))) {
                        throw invalidNotMatchedKeyCondition();
                    }
                }
            }
        }

        private LogicalPlan addCardinalityChecks(LogicalProject<?> rowChanges) {
            List<Slot> outputs = rowChanges.getOutput();
            Slot operation = outputs.get(0);
            List<Expression> partitionKeys = new ArrayList<>();
            for (String key : primaryKeys) {
                partitionKeys.add(outputs.get(schemaIndex(key) + 1));
            }
            Expression isInsert = new EqualTo(operation, new TinyIntLiteral(INSERT));
            List<CardinalityCheck> checks = new ArrayList<>();
            if (!merge.getMatchedClauses().isEmpty()) {
                checks.add(CardinalityCheck.matched(isInsert));
            }
            if (!merge.getNotMatchedClauses().isEmpty()) {
                checks.add(CardinalityCheck.inserted(isInsert));
            }
            List<NamedExpression> markerOutputs = new ArrayList<>(outputs);
            for (CardinalityCheck check : checks) {
                markerOutputs.add(check.marker);
            }
            LogicalPlan plan = new LogicalProject<>(markerOutputs, rowChanges);
            List<Alias> counts = new ArrayList<>();
            for (CardinalityCheck check : checks) {
                counts.add(check.count(partitionKeys));
            }
            plan = new LogicalWindow<>(new ArrayList<>(counts), plan);
            ImmutableSet.Builder<Expression> assertions = ImmutableSet.builder();
            for (int i = 0; i < checks.size(); i++) {
                assertions.add(checks.get(i).assertion(counts.get(i)));
            }
            plan = new LogicalFilter<>(assertions.build(), plan);
            return new LogicalProject<>(new ArrayList<>(outputs), plan);
        }

        private int schemaIndex(String name) {
            for (int i = 0; i < schema.size(); i++) {
                if (schema.get(i).getName().equalsIgnoreCase(name)) {
                    return i;
                }
            }
            throw new AnalysisException("Unable to resolve connector row-change column '" + name + "'");
        }

        private String targetPrimaryKeyName(Expression expression, Map<String, Slot> targetKeys) {
            Expression unwrapped = expression;
            while (unwrapped instanceof Cast) {
                if (((Cast) unwrapped).isExplicitType()) {
                    return null;
                }
                unwrapped = unwrapped.child(0);
            }
            if (!(unwrapped instanceof Slot)) {
                return null;
            }
            Slot slot = (Slot) unwrapped;
            for (Map.Entry<String, Slot> key : targetKeys.entrySet()) {
                if (slot.getExprId().equals(key.getValue().getExprId())
                        && expression.getDataType().equals(key.getValue().getDataType())) {
                    return key.getKey();
                }
            }
            return null;
        }

        private AnalysisException invalidNotMatchedKeyCondition() {
            return new AnalysisException("Paimon MERGE with NOT MATCHED INSERT requires ON to contain "
                    + "only equality predicates for every target primary-key column and each INSERT "
                    + "to use the corresponding deterministic source expression");
        }

        private Alias bindBranchLabel() {
            Expression targetPresent = new Not(new org.apache.doris.nereids.trees.expressions.IsNull(
                    findTargetSlot(primaryKeys.get(0))));
            Expression matched = new NullLiteral(IntegerType.INSTANCE);
            for (int i = merge.getMatchedClauses().size() - 1; i >= 0; i--) {
                MergeMatchedClause clause = merge.getMatchedClauses().get(i);
                if (i != merge.getMatchedClauses().size() - 1 && !clause.getCasePredicate().isPresent()) {
                    throw new AnalysisException("Only the last matched clause may omit its condition");
                }
                Expression label = new IntegerLiteral(i);
                matched = clause.getCasePredicate().isPresent()
                        ? new ShortCircuitIf(clause.getCasePredicate().get(), label, matched) : label;
            }
            Expression notMatched = new NullLiteral(IntegerType.INSTANCE);
            for (int i = merge.getNotMatchedClauses().size() - 1; i >= 0; i--) {
                MergeNotMatchedClause clause = merge.getNotMatchedClauses().get(i);
                if (i != merge.getNotMatchedClauses().size() - 1
                        && !clause.getCasePredicate().isPresent()) {
                    throw new AnalysisException("Only the last not matched clause may omit its condition");
                }
                Expression label = new IntegerLiteral(i + merge.getMatchedClauses().size());
                notMatched = clause.getCasePredicate().isPresent()
                        ? new ShortCircuitIf(clause.getCasePredicate().get(), label, notMatched) : label;
            }
            return new Alias(analyzer.analyze(
                    new ShortCircuitIf(targetPresent, matched, notMatched)), BRANCH_LABEL);
        }

        private List<List<Expression>> buildBranchProjections() {
            List<List<Expression>> branches = new ArrayList<>();
            for (MergeMatchedClause clause : merge.getMatchedClauses()) {
                branches.add(clause.isDelete() ? deleteProjection() : updateProjection(clause));
            }
            for (MergeNotMatchedClause clause : merge.getNotMatchedClauses()) {
                branches.add(insertProjection(clause));
            }
            if (branches.isEmpty()) {
                throw new AnalysisException("Connector MERGE requires at least one WHEN clause");
            }
            for (List<Expression> branch : branches) {
                for (int i = 0; i < branch.size(); i++) {
                    branch.set(i, analyzer.analyze(branch.get(i)));
                }
            }
            return branches;
        }

        private List<Expression> deleteProjection() {
            List<Expression> output = new ArrayList<>();
            output.add(new TinyIntLiteral(DELETE));
            for (Column column : schema) {
                output.add(targetSlot(column.getName()));
            }
            return output;
        }

        private List<Expression> updateProjection(MergeMatchedClause clause) {
            Map<String, Expression> changes = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (EqualTo assignment : clause.getAssignments()) {
                List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
                String name = parts.get(parts.size() - 1);
                if (changes.put(name, assignment.right()) != null) {
                    throw new AnalysisException("Duplicate column name in connector MERGE UPDATE: " + name);
                }
            }
            List<Expression> output = new ArrayList<>();
            output.add(new TinyIntLiteral(UPDATE));
            for (Column column : schema) {
                output.add(changes.containsKey(column.getName())
                        ? changes.remove(column.getName()) : targetSlot(column.getName()));
            }
            if (!changes.isEmpty()) {
                throw new AnalysisException("Unknown column in connector MERGE UPDATE: "
                        + String.join(", ", changes.keySet()));
            }
            return output;
        }

        private List<Expression> insertProjection(MergeNotMatchedClause clause) {
            if (clause.getRow().size() != schema.size()) {
                throw new AnalysisException("Connector MERGE INSERT requires values for every table column");
            }
            Map<String, Expression> values = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            if (!clause.getColNames().isEmpty()) {
                if (clause.getColNames().size() != schema.size()) {
                    throw new AnalysisException("Connector MERGE INSERT requires every table column");
                }
                for (int i = 0; i < clause.getColNames().size(); i++) {
                    String column = clause.getColNames().get(i);
                    if (values.put(column, unwrap(clause.getRow().get(i))) != null) {
                        throw new AnalysisException("Duplicate column in connector MERGE INSERT: "
                                + column);
                    }
                }
            }
            List<Expression> output = new ArrayList<>();
            output.add(new TinyIntLiteral(INSERT));
            for (int i = 0; i < schema.size(); i++) {
                Column column = schema.get(i);
                Expression value = clause.getColNames().isEmpty()
                        ? unwrap(clause.getRow().get(i)) : values.remove(column.getName());
                if (value == null) {
                    throw new AnalysisException("Missing column in connector MERGE INSERT: "
                            + column.getName());
                }
                output.add(value);
            }
            if (!values.isEmpty()) {
                throw new AnalysisException("Unknown column in connector MERGE INSERT: "
                        + String.join(", ", values.keySet()));
            }
            return output;
        }

        private Slot findTargetSlot(String column) {
            List<Slot> matches = child.getOutput().stream()
                    .filter(slot -> slot.getName().equalsIgnoreCase(column))
                    .filter(slot -> qualifierEndsWith(slot.getQualifier(), merge.getTargetNameInPlan()))
                    .collect(java.util.stream.Collectors.toList());
            if (matches.size() != 1) {
                throw new AnalysisException("Unable to resolve connector MERGE target column '"
                        + String.join(".", merge.getTargetNameInPlan()) + "." + column + "'");
            }
            return matches.get(0);
        }

        private Expression targetSlot(String column) {
            List<String> parts = Lists.newArrayList(merge.getTargetNameInPlan());
            parts.add(column);
            return new UnboundSlot(parts);
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

        private static Expression unwrap(NamedExpression expression) {
            return expression instanceof Alias || expression instanceof UnboundAlias
                    ? expression.child(0) : expression;
        }

        private static final class CardinalityCheck {
            private final Alias marker;
            private final String countName;
            private final String errorMessage;

            private CardinalityCheck(Alias marker, String countName, String errorMessage) {
                this.marker = marker;
                this.countName = countName;
                this.errorMessage = errorMessage;
            }

            private static CardinalityCheck matched(Expression isInsert) {
                return new CardinalityCheck(new Alias(new ShortCircuitIf(isInsert,
                        new NullLiteral(BigIntType.INSTANCE), new BigIntLiteral(1)),
                        "__DORIS_CHANGELOG_MATCH_MARKER__"), "__DORIS_CHANGELOG_MATCH_COUNT__",
                        "Paimon MERGE matched one target row with multiple source rows");
            }

            private static CardinalityCheck inserted(Expression isInsert) {
                return new CardinalityCheck(new Alias(new ShortCircuitIf(isInsert,
                        new BigIntLiteral(1), new NullLiteral(BigIntType.INSTANCE)),
                        "__DORIS_CHANGELOG_INSERT_MARKER__"), "__DORIS_CHANGELOG_INSERT_COUNT__",
                        "Paimon MERGE attempted to insert multiple rows with the same primary key");
            }

            private Alias count(List<Expression> keys) {
                return new Alias(new WindowExpression(
                        new Count(marker.toSlot()), keys, ImmutableList.of()), countName);
            }

            private Expression assertion(Alias count) {
                return new AssertTrue(new LessThanEqual(count.toSlot(), new BigIntLiteral(1)),
                        new VarcharLiteral(errorMessage));
            }
        }
    }
}
