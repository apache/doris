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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotNotFromChildren;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.window.WindowFunction;
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
            CheckAnalysis.checkKeyTypes(plan);
            checkMatchIsUsedCorrectly(plan);
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
            ((LogicalSort<?>) plan).getOrderKeys().forEach(key -> checkOrderKey(key.getExpr()));
        } else if (plan instanceof LogicalTopN) {
            ((LogicalTopN<?>) plan).getOrderKeys().forEach(key -> checkOrderKey(key.getExpr()));
        } else if (plan instanceof LogicalWindow) {
            ((LogicalWindow<?>) plan).getWindowExpressions().forEach(a -> {
                if (!(a instanceof Alias && ((Alias) a).child() instanceof WindowExpression)) {
                    return;
                }
                WindowExpression windowExpression = (WindowExpression) ((Alias) a).child();
                windowExpression.getOrderKeys().forEach(key -> checkOrderKey(key.child()));
                if (windowExpression.getPartitionKeys().stream().anyMatch((
                        partitionKey -> partitionKey.getDataType().isObjectType()))) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
            });
        } else if (plan instanceof LogicalJoin) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
            join.getHashJoinConjuncts().forEach(CheckAfterRewrite::checkJoinKey);
            join.getMarkJoinConjuncts().forEach(CheckAfterRewrite::checkJoinKey);
        }
    }

    private static void checkOrderKey(Expression key) {
        if (key.getDataType().isObjectType()) {
            throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
        }
        CheckAnalysis.checkSpatialOrderKey(key);
    }

    private static void checkJoinKey(Expression conjunct) {
        if (comparesVarBinary(conjunct)) {
            throw new AnalysisException("varbinary type could not in join equal conditions: " + conjunct.toSql());
        }
        if (conjunct instanceof EqualPredicate && conjunct.children().stream()
                .anyMatch(operand -> operand.getDataType().toCatalogDataType().typeContainsSpatialType())) {
            throw new AnalysisException("JOIN equality does not support spatial keys: " + conjunct.toSql());
        }
    }

    // BE cannot hash or compare VARBINARY values, but a join condition may compare strings cast from VARBINARY,
    // such as CAST(X'FF' AS STRING) = s, so only the compared operands are checked.
    private static boolean comparesVarBinary(Expression conjunct) {
        return conjunct.anyMatch(e -> e instanceof ComparisonPredicate && ((ComparisonPredicate) e).children()
                .stream().anyMatch(operand -> operand.getDataType().isVarBinaryType()));
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
