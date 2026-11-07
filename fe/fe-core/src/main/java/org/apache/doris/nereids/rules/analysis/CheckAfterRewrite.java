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
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotNotFromChildren;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.expressions.functions.window.WindowFunction;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Generate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * some check need to do after analyze whole plan.
 */
public class CheckAfterRewrite extends OneAnalysisRuleFactory {

    @Override
    public Rule build() {
        return any().then(plan -> {
            checkAllSlotReferenceFromChildren(plan);
            checkUnexpectedExpression(plan);
            checkMetricTypeIsUsedCorrectly(plan);
            checkMatchIsUsedCorrectly(plan);
            checkSearchIsUsedCorrectly(plan);
            return null;
        }).toRule(RuleType.CHECK_ANALYSIS);
    }

    private void checkUnexpectedExpression(Plan plan) {
        boolean isGenerate = plan instanceof Generate;
        boolean isAgg = plan instanceof LogicalAggregate;
        boolean isRepeat = plan instanceof LogicalRepeat;
        boolean isWindow = plan instanceof LogicalWindow;
        boolean notAggAndWindow = !isAgg && !isWindow;

        for (Expression expression : plan.getExpressions()) {
            expression.foreach(expr -> {
                if (expr instanceof SubqueryExpr) {
                    throw new AnalysisException("Subquery is not allowed in " + plan.getType());
                } else if (!isGenerate && expr instanceof TableGeneratingFunction) {
                    throw new AnalysisException("table generating function is not allowed in " + plan.getType());
                } else if (notAggAndWindow && expr instanceof AggregateFunction) {
                    throw new AnalysisException("aggregate function is not allowed in " + plan.getType());
                } else if (!isRepeat && expr instanceof GroupingScalarFunction) {
                    throw new AnalysisException("grouping scalar function is not allowed in " + plan.getType());
                } else if (!isWindow && (expr instanceof WindowExpression || expr instanceof WindowFunction)) {
                    throw new AnalysisException("analytic function is not allowed in " + plan.getType());
                }
            });
        }
    }

    /**
     *
     * used for test
     * after applying a rule, use this method to check whether all slots are from children
     */
    public void checkTreeAllSlotReferenceFromChildren(Plan plan) {
        checkAllSlotReferenceFromChildren(plan);
        for (Plan child : plan.children()) {
            checkTreeAllSlotReferenceFromChildren(child);
        }
    }

    private void checkAllSlotReferenceFromChildren(Plan plan) {
        Set<Slot> inputSlots = plan.getInputSlots();
        RoaringBitmap childrenOutput = plan.getChildrenOutputExprIdBitSet();

        Set<Slot> notFromChildren = Sets.newHashSet();
        for (Slot inputSlot : inputSlots) {
            if (!childrenOutput.contains(inputSlot.getExprId().asInt())) {
                notFromChildren.add(inputSlot);
            }
        }

        if (notFromChildren.isEmpty()) {
            return;
        }
        if (plan instanceof LogicalRepeat) {
            LogicalRepeat repeat = (LogicalRepeat) plan;
            notFromChildren.remove(repeat.getGroupingId().get());
        }
        notFromChildren = removeValidSlotsNotFromChildren(notFromChildren, childrenOutput);
        if (!notFromChildren.isEmpty()) {
            if (plan.arity() != 0 && plan.child(0) instanceof LogicalAggregate) {
                throw new AnalysisException(String.format(plan.getClass().getSimpleName() + " expression %s must"
                        + " appear in the GROUP BY clause or be used in an aggregate function",
                        notFromChildren.stream()
                                .map(slot -> "'" + slot.getName() + "'")
                                .collect(Collectors.joining(", "))));
            } else {
                throw new AnalysisException(String.format(
                        "Input slot(s) not in child's output: %s in plan: %s\nchild output is: %s\nplan tree:\n%s",
                        StringUtils.join(notFromChildren.stream().map(ExpressionTrait::toString)
                                .collect(Collectors.toSet()), ", "),
                        plan,
                        plan.children().stream()
                                .flatMap(child -> child.getOutput().stream())
                                .collect(Collectors.toSet()),
                        plan.treeString()));
            }
        }
    }

    private Set<Slot> removeValidSlotsNotFromChildren(Set<Slot> slots, RoaringBitmap childrenOutput) {
        return slots.stream()
                .filter(expr -> {
                    return !(expr instanceof SlotNotFromChildren);
                })
                .collect(Collectors.toSet());
    }

    private void checkMetricTypeIsUsedCorrectly(Plan plan) {
        if (plan instanceof LogicalAggregate) {
            LogicalAggregate<?> agg = (LogicalAggregate<?>) plan;
            for (Expression groupBy : agg.getGroupByExpressions()) {
                if (groupBy.getDataType().isObjectType() || groupBy.getDataType().isVarBinaryType()) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
            }
        } else if (plan instanceof LogicalSort) {
            LogicalSort<?> sort = (LogicalSort<?>) plan;
            for (OrderKey orderKey : sort.getOrderKeys()) {
                if (orderKey.getExpr().getDataType().isObjectType()) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
            }
        } else if (plan instanceof LogicalTopN) {
            LogicalTopN<?> topN = (LogicalTopN<?>) plan;
            for (OrderKey orderKey : topN.getOrderKeys()) {
                if (orderKey.getExpr().getDataType().isObjectType()) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
            }
        } else if (plan instanceof LogicalWindow) {
            ((LogicalWindow<?>) plan).getWindowExpressions().forEach(a -> {
                if (!(a instanceof Alias && ((Alias) a).child() instanceof WindowExpression)) {
                    return;
                }
                WindowExpression windowExpression = (WindowExpression) ((Alias) a).child();
                if (windowExpression.getOrderKeys().stream().anyMatch((
                        orderKey -> orderKey.getDataType().isObjectType()))) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
                if (windowExpression.getPartitionKeys().stream().anyMatch((
                        partitionKey -> partitionKey.getDataType().isObjectType()))) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
            });
        } else if (plan instanceof LogicalJoin) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
            for (Expression conjunct : join.getHashJoinConjuncts()) {
                if (comparesVarBinary(conjunct)) {
                    throw new AnalysisException(
                            "varbinary type could not in join equal conditions: " + conjunct.toSql());
                }
            }
            for (Expression conjunct : join.getMarkJoinConjuncts()) {
                if (comparesVarBinary(conjunct)) {
                    throw new AnalysisException(
                            "varbinary type could not in join equal conditions: " + conjunct.toSql());
                }
            }
        }
    }

    // BE cannot hash or compare VARBINARY values, but a join condition may compare strings cast from VARBINARY,
    // such as CAST(X'FF' AS STRING) = s, so only the compared operands are checked.
    private static boolean comparesVarBinary(Expression conjunct) {
        return conjunct.anyMatch(e -> e instanceof ComparisonPredicate && ((ComparisonPredicate) e).children()
                .stream().anyMatch(operand -> operand.getDataType().isVarBinaryType()));
    }

    private void checkSearchIsUsedCorrectly(Plan plan) {
        for (Expression expression : plan.getExpressions()) {
            if (expression.anyMatch(expr -> expr instanceof Search)) {
                throw new AnalysisException("search() could not be bound to an OLAP table scan");
            }
            expression.foreach(expr -> {
                if (!(expr instanceof SearchExpression)) {
                    return;
                }
                if (!(plan instanceof LogicalFilter) || !(plan.child(0) instanceof LogicalOlapScan)) {
                    throw new AnalysisException("search() is only supported in filters directly on an OLAP table scan");
                }
                LogicalOlapScan scan = (LogicalOlapScan) plan.child(0);
                SearchExpression search = (SearchExpression) expr;
                for (Expression child : search.children()) {
                    SlotReference slot;
                    if (child instanceof SlotReference) {
                        slot = (SlotReference) child;
                    } else if (child instanceof ElementAt && child.child(0) instanceof SlotReference
                            && child.child(1) instanceof StringLiteral) {
                        slot = (SlotReference) child.child(0);
                    } else {
                        throw new AnalysisException("search() fields must reference OLAP scan columns directly");
                    }
                    // VariantSubPathPruning replaces ElementAt with a slot that retains its parent column.
                    checkInvertedIndexExists(scan.getTable(), slot.getOriginalColumn().get().getName(),
                            search.getDslString(), child instanceof ElementAt || slot.hasSubColPath());
                }
            });
        }
    }

    /**
     * Ensure the column referenced by a Lucene-syntax SEARCH predicate has an inverted index.
     * Without this check the BE path would silently fall back to an empty bitmap (i.e. all FALSE),
     * which is indistinguishable from "no rows matched" to the user. Throw at planning time so the
     * behavior is consistent with referencing a non-existent column.
     *
     * @param table         table backing the LogicalOlapScan
     * @param columnName    column name (parent column name when isVariantParent)
     * @param dsl           original DSL, used in the error message
     * @param isVariantParent true when {@code columnName} is the parent of a variant subcolumn
     *                        access (e.g. {@code msg.body}); for that case any INVERTED index on
     *                        the parent column is accepted because the concrete subcolumn binding
     *                        is resolved per-segment in BE.
     */
    private void checkInvertedIndexExists(OlapTable table, String columnName, String dsl,
            boolean isVariantParent) {
        Column column = table.getColumn(columnName);
        if (column == null) {
            // Field existence is already validated during SEARCH binding; if we reach here the schema
            // changed concurrently. Surface a clear error rather than fall through.
            throw new AnalysisException(String.format(
                    "Column '%s' not found in table '%s' for search: %s",
                    columnName, table.getName(), dsl));
        }

        if (isVariantParent) {
            for (Index index : table.getIndexes()) {
                if (index.getIndexType() != IndexType.INVERTED) {
                    continue;
                }
                List<String> columns = index.getColumns();
                if (columns != null && !columns.isEmpty()
                        && columnName.equalsIgnoreCase(columns.get(0))) {
                    return;
                }
            }
        } else if (table.getInvertedIndex(column, null) != null) {
            return;
        }

        throw new AnalysisException(String.format(
                "Field '%s' has no inverted index, cannot be used in search: %s. "
                        + "Create an inverted index on the column first "
                        + "(ALTER TABLE ... ADD INDEX ... USING INVERTED).",
                columnName, dsl));
    }

    private void checkMatchIsUsedCorrectly(Plan plan) {
        for (Expression expression : plan.getExpressions()) {
            if (expression instanceof Match) {
                if (plan instanceof LogicalFilter && plan.child(0) instanceof LogicalOlapScan) {
                    return;
                } else {
                    throw new AnalysisException(String.format(
                            "Not support match in %s in plan: %s, only support in olapScan filter",
                            plan.child(0), plan));
                }
            }
        }
    }
}
