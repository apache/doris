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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.FuncDeps;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Eliminate group by key based on fd item information.
 * such as:
 *  for a -> b, we can get:
 *          group by a, b, c  => group by a, c
 *
 * When a group-by key is FD-redundant but still needed in the output,
 * it is wrapped with any_value() and assigned a fresh ExprId.
 * Upper plan references are rewritten via ExprIdRewriter so that
 * all ancestor nodes see the new ExprIds.
 */
public class EliminateGroupByKey extends DefaultPlanRewriter<Map<ExprId, ExprId>> implements CustomRewriter {
    private ExprIdRewriter exprIdReplacer;

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        if (!plan.containsType(Aggregate.class)) {
            return plan;
        }
        Map<ExprId, ExprId> replaceMap = new HashMap<>();
        ExprIdRewriter.ReplaceRule replaceRule = new ExprIdRewriter.ReplaceRule(replaceMap, false);
        exprIdReplacer = new ExprIdRewriter(replaceRule, jobContext);
        return plan.accept(this, replaceMap);
    }

    @Override
    public Plan visit(Plan plan, Map<ExprId, ExprId> replaceMap) {
        plan = visitChildren(this, plan, replaceMap);
        plan = exprIdReplacer.rewriteExpr(plan, replaceMap);
        return plan;
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> proj, Map<ExprId, ExprId> replaceMap) {
        proj = visitChildren(this, proj, replaceMap);

        // Find the Aggregate child, possibly through a Filter
        Plan child = proj.child(0);
        LogicalAggregate<? extends Plan> agg;
        boolean hasFilter = child instanceof LogicalFilter;
        if (hasFilter && child.child(0) instanceof LogicalAggregate) {
            agg = (LogicalAggregate<? extends Plan>) child.child(0);
        } else if (child instanceof LogicalAggregate) {
            agg = (LogicalAggregate<? extends Plan>) child;
        } else {
            return exprIdReplacer.rewriteExpr(proj, replaceMap);
        }

        // Don't transform if source repeat is present
        if (agg.getSourceRepeat().isPresent()) {
            return exprIdReplacer.rewriteExpr(proj, replaceMap);
        }

        // Compute requireOutput: slots needed by the Project (and Filter, if present)
        Set<Slot> requireOutput = new HashSet<>(proj.getInputSlots());
        if (hasFilter) {
            requireOutput.addAll(child.getInputSlots());
        }

        // Transform the aggregate
        EliminateResult result = eliminateGroupByKeyWithMap(agg, requireOutput);
        if (!result.changed) {
            return exprIdReplacer.rewriteExpr(proj, replaceMap);
        }

        // Merge into the global replaceMap so that all ancestor nodes get rewritten
        replaceMap.putAll(result.replaceMap);

        // Rebuild the child chain with the new aggregate,
        // and rewrite the Filter (if present) and Project expressions
        Plan newChild;
        if (hasFilter) {
            Plan updatedFilter = child.withChildren(result.newAgg);
            newChild = exprIdReplacer.rewriteExpr(updatedFilter, replaceMap);
        } else {
            newChild = result.newAgg;
        }
        Plan newProj = exprIdReplacer.rewriteExpr(proj.withChildren(newChild), replaceMap);
        return newProj;
    }

    /** Result of eliminateGroupByKey: the new aggregate and a map of old->new ExprIds. */
    private static class EliminateResult {
        final LogicalAggregate<Plan> newAgg;
        final Map<ExprId, ExprId> replaceMap;
        final boolean changed;

        EliminateResult(LogicalAggregate<Plan> newAgg, Map<ExprId, ExprId> replaceMap, boolean changed) {
            this.newAgg = newAgg;
            this.replaceMap = replaceMap;
            this.changed = changed;
        }
    }

    EliminateResult eliminateGroupByKeyWithMap(LogicalAggregate<? extends Plan> agg, Set<Slot> requireOutput) {
        FindResult result = findCanBeRemovedExpressionsInternal(agg, requireOutput,
                agg.child().getLogicalProperties().getTrait());
        Set<Expression> removeExpression = result.removeExpression;
        Set<Expression> wrapWithAnyValue = result.wrapWithAnyValue;

        List<Expression> newGroupExpression = new ArrayList<>();
        for (Expression expression : agg.getGroupByExpressions()) {
            if (!removeExpression.contains(expression)
                    && !wrapWithAnyValue.contains(expression)) {
                newGroupExpression.add(expression);
            }
        }
        List<NamedExpression> newOutput = new ArrayList<>();
        Map<ExprId, ExprId> replaceMap = new HashMap<>();
        boolean changed = !removeExpression.isEmpty() || !wrapWithAnyValue.isEmpty();
        for (NamedExpression expression : agg.getOutputExpressions()) {
            if (removeExpression.contains(expression)) {
                continue;
            }
            if (wrapWithAnyValue.contains(expression)) {
                // expression is FD-redundant but needed in output: wrap with any_value
                // Use fresh ExprId (auto-generated by Alias) to avoid ExprId collision,
                // and record the mapping for rewriting upper plan references.
                Alias newAlias = new Alias(new AnyValue(expression.toSlot()), expression.getName());
                replaceMap.put(expression.getExprId(), newAlias.getExprId());
                expression = newAlias;
            }
            newOutput.add(expression);
        }
        return new EliminateResult(agg.withGroupByAndOutput(newGroupExpression, newOutput), replaceMap, changed);
    }

    /**
     * Return expressions that can be completely removed from both group-by and output.
     * Kept for backward compatibility with external callers (e.g. PushDownAggThroughJoinOnPkFk).
     */
    public static Set<Expression> findCanBeRemovedExpressions(LogicalAggregate<? extends Plan> agg,
            Set<Slot> requireOutput, DataTrait dataTrait) {
        FindResult result = findCanBeRemovedExpressionsInternal(agg, requireOutput, dataTrait);
        return new HashSet<>(result.removeExpression);
    }

    /** Result of findCanBeRemovedExpressionsInternal: two sets of expressions. */
    private static class FindResult {
        final Set<Expression> removeExpression;   // remove from group-by and output
        final Set<Expression> wrapWithAnyValue;   // remove from group-by, wrap with ANY_VALUE in output

        FindResult(Set<Expression> removeExpression, Set<Expression> wrapWithAnyValue) {
            this.removeExpression = removeExpression;
            this.wrapWithAnyValue = wrapWithAnyValue;
        }
    }

    private static FindResult findCanBeRemovedExpressionsInternal(LogicalAggregate<? extends Plan> agg,
            Set<Slot> requireOutput, DataTrait dataTrait) {
        Map<Expression, Set<Slot>> groupBySlots = new HashMap<>();
        Set<Slot> validSlots = new HashSet<>();
        for (Expression expression : agg.getGroupByExpressions()) {
            groupBySlots.put(expression, expression.getInputSlots());
            validSlots.addAll(expression.getInputSlots());
        }

        FuncDeps funcDeps = dataTrait.getAllValidFuncDeps(validSlots);
        if (funcDeps.isEmpty()) {
            return new FindResult(new HashSet<>(), new HashSet<>());
        }

        Set<Set<Slot>> minGroupBySlots = funcDeps.eliminateDeps(new HashSet<>(groupBySlots.values()), requireOutput);
        Set<Expression> removeExpression = new HashSet<>();
        Set<Expression> wrapWithAnyValue = new HashSet<>();
        for (Entry<Expression, Set<Slot>> entry : groupBySlots.entrySet()) {
            if (!minGroupBySlots.contains(entry.getValue())) {
                // FD redundant: can remove from group-by
                if (!requireOutput.containsAll(entry.getValue())) {
                    // Not needed in output either: remove completely
                    removeExpression.add(entry.getKey());
                } else {
                    // Still needed in output: remove from group-by, wrap with ANY_VALUE in output
                    wrapWithAnyValue.add(entry.getKey());
                }
            }
        }
        return new FindResult(removeExpression, wrapWithAnyValue);
    }
}
