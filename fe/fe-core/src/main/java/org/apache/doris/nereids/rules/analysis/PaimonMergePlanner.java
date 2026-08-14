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
import org.apache.doris.datasource.paimon.PaimonRowChangeOperation;
import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
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
import org.apache.doris.nereids.trees.expressions.functions.scalar.ShortCircuitIf;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.PaimonRowChangeSpec;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TinyIntType;
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

/** Lowers one bound Paimon MERGE input into the changelog rows consumed by the sink. */
final class PaimonMergePlanner {
    private static final String BRANCH_LABEL = "__DORIS_PAIMON_MERGE_BRANCH__";
    private static final String MATCH_COUNT = "__DORIS_PAIMON_MERGE_MATCH_COUNT__";
    private static final String INSERT_COUNT = "__DORIS_PAIMON_MERGE_INSERT_COUNT__";
    private static final String MATCH_MARKER = "__DORIS_PAIMON_MERGE_MATCH_MARKER__";
    private static final String INSERT_MARKER = "__DORIS_PAIMON_MERGE_INSERT_MARKER__";

    private final PaimonWriteTarget target;
    private final PaimonRowChangeSpec.Merge merge;
    private final LogicalPlan child;
    private final ExpressionAnalyzer analyzer;
    private final RowChangeOutputLayout outputLayout;

    private PaimonMergePlanner(PaimonWriteTarget target, PaimonRowChangeSpec.Merge merge,
            LogicalPlan child, CascadesContext cascadesContext) {
        this.target = target;
        this.merge = merge;
        this.child = child;
        this.analyzer = new ExpressionAnalyzer(
                child, new Scope(child.getOutput()), cascadesContext, true, false);
        this.outputLayout = RowChangeOutputLayout.from(target);
    }

    static LogicalProject<?> build(PaimonWriteTarget target, PaimonRowChangeSpec.Merge merge,
            LogicalPlan child, CascadesContext cascadesContext) {
        return new PaimonMergePlanner(target, merge, child, cascadesContext).build();
    }

    private LogicalProject<?> build() {
        Alias branchLabel = bindBranchLabel();
        Slot branchLabelSlot = branchLabel.toSlot();
        List<NamedExpression> branchOutputs = new ArrayList<>(child.getOutput());
        branchOutputs.add(branchLabel);
        LogicalPlan selectedBranches = new LogicalProject<>(branchOutputs, child);
        selectedBranches = new LogicalFilter<>(
                ImmutableSet.of(new Not(new IsNull(branchLabelSlot))), selectedBranches);

        List<List<Expression>> branchProjections = buildBranchProjections();
        if (!merge.getNotMatchedClauses().isEmpty()) {
            validateNotMatchedPrimaryKeys(branchProjections);
        }
        LogicalProject<?> rowChanges = new LogicalProject<>(
                generateFinalProjections(branchProjections, branchLabelSlot), selectedBranches);
        return addCardinalityChecks(rowChanges);
    }

    private Alias bindBranchLabel() {
        String primaryKey = target.getTable().primaryKeys().get(0);
        Slot targetKey = findTargetSlot(primaryKey);
        Expression targetPresent = new Not(new IsNull(targetKey));
        return new Alias(analyzer.analyze(
                generateBranchLabel(targetPresent).child()), BRANCH_LABEL);
    }

    private List<List<Expression>> buildBranchProjections() {
        List<List<Expression>> branches = new ArrayList<>();
        for (MergeMatchedClause clause : merge.getMatchedClauses()) {
            branches.add(clause.isDelete()
                    ? buildDeleteProjection() : buildUpdateProjection(clause));
        }
        for (MergeNotMatchedClause clause : merge.getNotMatchedClauses()) {
            branches.add(buildInsertProjection(clause));
        }
        if (branches.isEmpty()) {
            throw new AnalysisException("Paimon MERGE requires at least one WHEN clause");
        }
        for (List<Expression> branch : branches) {
            for (int i = 0; i < branch.size(); i++) {
                branch.set(i, analyzer.analyze(branch.get(i)));
            }
        }
        return branches;
    }

    private void validateNotMatchedPrimaryKeys(List<List<Expression>> branchProjections) {
        Map<String, Slot> targetPrimaryKeys = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (String primaryKey : target.getTable().primaryKeys()) {
            targetPrimaryKeys.put(primaryKey, findTargetSlot(primaryKey));
        }
        Set<Slot> targetSlots = child.getOutput().stream()
                .filter(slot -> qualifierEndsWith(
                        slot.getQualifier(), merge.getTargetNameInPlan()))
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
            String leftKey = targetPrimaryKeyName(equality.left(), targetPrimaryKeys);
            String rightKey = targetPrimaryKeyName(equality.right(), targetPrimaryKeys);
            if ((leftKey == null) == (rightKey == null)) {
                throw invalidNotMatchedKeyCondition();
            }
            String primaryKey = leftKey != null ? leftKey : rightKey;
            Expression sourceKey = leftKey != null ? equality.right() : equality.left();
            if (sourceKey.getInputSlots().isEmpty()
                    || sourceKey.getInputSlots().stream().anyMatch(targetSlots::contains)
                    || sourceKey.containsNondeterministic()
                    || sourceKeys.put(primaryKey, sourceKey) != null) {
                throw invalidNotMatchedKeyCondition();
            }
        }
        if (sourceKeys.size() != targetPrimaryKeys.size()) {
            throw invalidNotMatchedKeyCondition();
        }

        int firstInsertBranch = merge.getMatchedClauses().size();
        for (int branch = firstInsertBranch; branch < branchProjections.size(); branch++) {
            for (Map.Entry<String, Expression> sourceKey : sourceKeys.entrySet()) {
                int column = outputLayout.columnIndex(sourceKey.getKey());
                Expression insertKey = branchProjections.get(branch).get(column);
                DataType keyType = outputLayout.dataType(column);
                if (!normalizeKeyExpression(insertKey, keyType)
                        .equals(normalizeKeyExpression(sourceKey.getValue(), keyType))) {
                    throw invalidNotMatchedKeyCondition();
                }
            }
        }
    }

    private LogicalProject<?> addCardinalityChecks(LogicalProject<?> rowChanges) {
        List<Slot> rowChangeOutputs = rowChanges.getOutput();
        Slot operation = rowChangeOutputs.get(outputLayout.operationIndex());
        List<Expression> partitionKeys = new ArrayList<>();
        for (String primaryKey : target.getTable().primaryKeys()) {
            partitionKeys.add(rowChangeOutputs.get(outputLayout.columnIndex(primaryKey)));
        }

        Expression isInsert = new EqualTo(
                operation, new TinyIntLiteral(PaimonRowChangeOperation.INSERT));
        List<CardinalityCheck> checks = new ArrayList<>();
        if (!merge.getMatchedClauses().isEmpty()) {
            checks.add(CardinalityCheck.matched(isInsert));
        }
        if (!merge.getNotMatchedClauses().isEmpty()) {
            checks.add(CardinalityCheck.inserted(isInsert));
        }

        List<NamedExpression> markerOutputs = new ArrayList<>(rowChangeOutputs);
        for (CardinalityCheck check : checks) {
            markerOutputs.add(check.marker);
        }
        LogicalPlan plan = new LogicalProject<>(markerOutputs, rowChanges);

        List<Alias> counts = new ArrayList<>();
        for (CardinalityCheck check : checks) {
            counts.add(check.count(partitionKeys));
        }
        List<NamedExpression> windowOutputs = new ArrayList<>(counts);
        plan = new LogicalWindow<>(windowOutputs, plan);

        ImmutableSet.Builder<Expression> assertions = ImmutableSet.builder();
        for (int i = 0; i < checks.size(); i++) {
            assertions.add(checks.get(i).assertion(counts.get(i)));
        }
        plan = new LogicalFilter<>(assertions.build(), plan);
        return new LogicalProject<>(new ArrayList<>(rowChangeOutputs), plan);
    }

    private Slot findTargetSlot(String columnName) {
        List<Slot> matches = child.getOutput().stream()
                .filter(slot -> slot.getName().equalsIgnoreCase(columnName))
                .filter(slot -> qualifierEndsWith(
                        slot.getQualifier(), merge.getTargetNameInPlan()))
                .collect(ImmutableList.toImmutableList());
        if (matches.size() != 1) {
            throw new AnalysisException("Unable to resolve Paimon MERGE target column '"
                    + String.join(".", merge.getTargetNameInPlan()) + "." + columnName + "'");
        }
        return matches.get(0);
    }

    private static String targetPrimaryKeyName(
            Expression expression, Map<String, Slot> targetPrimaryKeys) {
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
        for (Map.Entry<String, Slot> primaryKey : targetPrimaryKeys.entrySet()) {
            if (slot.getExprId().equals(primaryKey.getValue().getExprId())
                    && expression.getDataType().equals(primaryKey.getValue().getDataType())) {
                return primaryKey.getKey();
            }
        }
        return null;
    }

    private static Expression normalizeKeyExpression(Expression expression, DataType dataType) {
        return TypeCoercionUtils.castIfNotSameType(expression, dataType);
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

    private static AnalysisException invalidNotMatchedKeyCondition() {
        return new AnalysisException("Paimon MERGE with NOT MATCHED INSERT requires ON to contain "
                + "only equality predicates for every target primary-key column and each INSERT "
                + "to use the corresponding deterministic source expression");
    }

    private Alias generateBranchLabel(Expression targetPresent) {
        Expression matchedLabel = new NullLiteral(IntegerType.INSTANCE);
        for (int i = merge.getMatchedClauses().size() - 1; i >= 0; i--) {
            MergeMatchedClause clause = merge.getMatchedClauses().get(i);
            if (i != merge.getMatchedClauses().size() - 1
                    && !clause.getCasePredicate().isPresent()) {
                throw new AnalysisException("Only the last matched clause may omit its condition");
            }
            Expression result = new IntegerLiteral(i);
            matchedLabel = clause.getCasePredicate().isPresent()
                    ? new ShortCircuitIf(clause.getCasePredicate().get(), result, matchedLabel) : result;
        }
        Expression notMatchedLabel = new NullLiteral(IntegerType.INSTANCE);
        for (int i = merge.getNotMatchedClauses().size() - 1; i >= 0; i--) {
            MergeNotMatchedClause clause = merge.getNotMatchedClauses().get(i);
            if (i != merge.getNotMatchedClauses().size() - 1
                    && !clause.getCasePredicate().isPresent()) {
                throw new AnalysisException("Only the last not matched clause may omit its condition");
            }
            Expression result = new IntegerLiteral(i + merge.getMatchedClauses().size());
            notMatchedLabel = clause.getCasePredicate().isPresent()
                    ? new ShortCircuitIf(clause.getCasePredicate().get(), result, notMatchedLabel) : result;
        }
        return new Alias(new ShortCircuitIf(targetPresent, matchedLabel, notMatchedLabel), BRANCH_LABEL);
    }

    private List<Expression> buildDeleteProjection() {
        List<Expression> output = new ArrayList<>();
        output.add(new TinyIntLiteral(PaimonRowChangeOperation.DELETE));
        for (Column column : target.getSchema()) {
            output.add(targetSlot(column.getName()));
        }
        return output;
    }

    private List<Expression> buildUpdateProjection(MergeMatchedClause clause) {
        Map<String, Expression> changes = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo assignment : clause.getAssignments()) {
            List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
            String columnName = parts.get(parts.size() - 1);
            if (changes.put(columnName, assignment.right()) != null) {
                throw new AnalysisException(
                        "Duplicate column name in Paimon MERGE UPDATE: " + columnName);
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

    private List<Expression> buildInsertProjection(MergeNotMatchedClause clause) {
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
                if (values.put(clause.getColNames().get(i),
                        unwrap(clause.getRow().get(i))) != null) {
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

    private Expression targetSlot(String column) {
        List<String> parts = Lists.newArrayList(merge.getTargetNameInPlan());
        parts.add(column);
        return new UnboundSlot(parts);
    }

    private List<NamedExpression> generateFinalProjections(
            List<List<Expression>> branches, Slot branchLabel) {
        List<NamedExpression> output = new ArrayList<>();
        for (int column = 0; column < branches.get(0).size(); column++) {
            Expression value = generateFinalExpression(
                    column, branches, branchLabel, outputLayout.dataType(column));
            output.add(new Alias(value, outputLayout.name(column)));
        }
        return output;
    }

    private static Expression generateFinalExpression(
            int column, List<List<Expression>> branches, Slot branchLabel, DataType dataType) {
        Expression value = new NullLiteral(dataType);
        for (int branch = branches.size() - 1; branch >= 0; branch--) {
            Expression branchValue = new Cast(branches.get(branch).get(column), dataType);
            value = new ShortCircuitIf(new EqualTo(branchLabel,
                    new IntegerLiteral(branch)), branchValue, value);
        }
        return value;
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
            Alias marker = new Alias(new ShortCircuitIf(isInsert,
                    new NullLiteral(BigIntType.INSTANCE), new BigIntLiteral(1)), MATCH_MARKER);
            return new CardinalityCheck(marker, MATCH_COUNT,
                    "Paimon MERGE matched one target row with multiple source rows");
        }

        private static CardinalityCheck inserted(Expression isInsert) {
            Alias marker = new Alias(new ShortCircuitIf(isInsert,
                    new BigIntLiteral(1), new NullLiteral(BigIntType.INSTANCE)), INSERT_MARKER);
            return new CardinalityCheck(marker, INSERT_COUNT,
                    "Paimon MERGE attempted to insert multiple rows with the same primary key");
        }

        private Alias count(List<Expression> partitionKeys) {
            return new Alias(new WindowExpression(
                    new Count(marker.toSlot()), partitionKeys, ImmutableList.of()), countName);
        }

        private Expression assertion(Alias count) {
            return new AssertTrue(
                    new LessThanEqual(count.toSlot(), new BigIntLiteral(1)),
                    new VarcharLiteral(errorMessage));
        }
    }

    private static final class RowChangeOutputLayout {
        private final List<String> names;
        private final List<DataType> types;
        private final Map<String, Integer> columnIndexes;

        private RowChangeOutputLayout(List<String> names, List<DataType> types,
                Map<String, Integer> columnIndexes) {
            this.names = names;
            this.types = types;
            this.columnIndexes = columnIndexes;
        }

        private static RowChangeOutputLayout from(PaimonWriteTarget target) {
            ImmutableList.Builder<String> names = ImmutableList.builder();
            ImmutableList.Builder<DataType> types = ImmutableList.builder();
            Map<String, Integer> columnIndexes = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            names.add(PaimonRowChangeOperation.OPERATION_COLUMN);
            types.add(TinyIntType.INSTANCE);
            int outputIndex = 1;
            for (Column column : target.getSchema()) {
                names.add(column.getName());
                types.add(DataType.fromCatalogType(column.getType()));
                columnIndexes.put(column.getName(), outputIndex++);
            }
            return new RowChangeOutputLayout(names.build(), types.build(), columnIndexes);
        }

        private int operationIndex() {
            return 0;
        }

        private int columnIndex(String columnName) {
            Integer index = columnIndexes.get(columnName);
            if (index == null) {
                throw new AnalysisException(
                        "Unable to resolve Paimon row-change column '" + columnName + "'");
            }
            return index;
        }

        private String name(int index) {
            return names.get(index);
        }

        private DataType dataType(int index) {
            return types.get(index);
        }
    }
}
