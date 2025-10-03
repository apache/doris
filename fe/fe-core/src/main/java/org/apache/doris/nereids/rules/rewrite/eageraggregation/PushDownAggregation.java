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

package org.apache.doris.nereids.rules.rewrite.eageraggregation;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * push down aggregation
 */
public class PushDownAggregation extends DefaultPlanRewriter<JobContext> implements CustomRewriter {
    private static final Logger LOG = LoggerFactory.getLogger(PushDownAggregation.class);

    public final EagerAggRewriter writer = new EagerAggRewriter();

    private final Set<Class> pushDownAggFunctionSet = Sets.newHashSet(
            Sum.class,
            Count.class,
            Avg.class,
            Max.class,
            Min.class);

    private final Set<Class> acceptNodeType = Sets.newHashSet(
            LogicalProject.class,
            LogicalFilter.class,
            LogicalRelation.class,
            LogicalJoin.class);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        int mode = ConnectContext.get().getSessionVariable().eagerAggregationMode;
        if (mode < 0) {
            return plan;
        } else {
            return plan.accept(this, jobContext);
        }
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, JobContext context) {
        Plan newChild = agg.child().accept(this, context);
        if (newChild != agg.child()) {
            // TODO : push down upper aggregations
            return agg.withChildren(newChild);
        }

        if (agg.getSourceRepeat().isPresent()) {
            return agg;
        }

        List<AggregateFunction> aggFunctions = new ArrayList<>();

        Map<Avg, Divide> avgToSumCountMap = new HashMap<>();
        for (AggregateFunction aggFunction : agg.getAggregateFunctions()) {
            if (pushDownAggFunctionSet.contains(aggFunction.getClass())
                    && !aggFunction.isDistinct()
                    && (!(aggFunction instanceof Count) || (!((Count) aggFunction).isCountStar()))) {
                if (aggFunction instanceof Avg) {
                    DataType targetType = aggFunction.getDataType();
                    Sum sum = new Sum(aggFunction.child(0));
                    Count count = new Count(aggFunction.child(0));
                    if (!aggFunctions.contains(sum)) {
                        aggFunctions.add(sum);
                    }
                    if (!aggFunctions.contains(count)) {
                        aggFunctions.add(count);
                    }
                    Expression castSum = targetType.equals(sum.getDataType()) ? sum : new Cast(sum, targetType);
                    Expression castCount = targetType.equals(count.getDataType()) ? count : new Cast(count, targetType);
                    avgToSumCountMap.put((Avg) aggFunction,
                            new Divide(castSum, castCount));
                } else {
                    aggFunctions.add(aggFunction);
                }
            } else {
                return agg;
            }
        }

        if (!checkSubTreePattern(agg.child())) {
            return agg;
        }

        List<NamedExpression> groupKeys = new ArrayList<>();
        for (Expression groupKey : agg.getGroupByExpressions()) {
            if (groupKey instanceof SlotReference) {
                groupKeys.add((SlotReference) groupKey);
            } else {
                if (SessionVariable.isFeDebug()) {
                    throw new RuntimeException("PushDownAggregation failed: agg is not normalized\n "
                            + agg.treeString());
                } else {
                    return agg;
                }
            }
        }

        PushDownAggContext pushDownContext = new PushDownAggContext(new ArrayList<>(aggFunctions),
                groupKeys);
        try {
            Plan child = agg.child().accept(writer, pushDownContext);
            if (child != agg.child()) {
                // agg has been pushed down, rewrite agg output expressions
                // before: agg[sum(A), by (B)]->join(C=D)->scan(T1[A...])
                // after: agg[sum(x), by(B)]->join(C=D)->agg[sum(A) as x, by(B,C)]->scan(T1[A...])
                List<NamedExpression> newOutputExpressions = new ArrayList<>();
                for (NamedExpression ne : agg.getOutputExpressions()) {
                    if (ne instanceof SlotReference) {
                        newOutputExpressions.add(ne);
                    } else {
                        Expression rewriteAvgExpr = ExpressionUtils.replace(ne, avgToSumCountMap);
                        NamedExpression replaceAliasExpr = (NamedExpression) rewriteAvgExpr
                                .rewriteDownShortCircuit(e -> {
                                    Alias alias = pushDownContext.getAliasMap().get(e);
                                    if (alias != null) {
                                        AggregateFunction aggFunction = (AggregateFunction) e;
                                        return aggFunction.withChildren(alias.toSlot());
                                    } else {
                                        return e;
                                    }
                                });
                        newOutputExpressions.add(replaceAliasExpr);
                    }
                }
                return agg.withAggOutputChild(newOutputExpressions, child);
            }
        } catch (RuntimeException e) {
            LOG.info("PushDownAggregation failed: " + e.getMessage() + "\n" + agg.treeString());
        }
        return agg;
    }

    private boolean checkSubTreePattern(Plan root) {
        return containsPushDownJoin(root)
                && isSPJ(root);
    }

    private boolean containsPushDownJoin(Plan root) {
        if (root instanceof LogicalJoin && !((LogicalJoin) root).isMarkJoin()) {
            return true;
        }
        if (root.children().isEmpty()) {
            return false;
        }
        return root.children().stream().anyMatch(this::containsPushDownJoin);
    }

    private boolean isSPJ(Plan root) {
        boolean accepted = acceptNodeType.stream()
                .anyMatch(clazz -> clazz.isAssignableFrom(root.getClass()));
        if (!accepted) {
            return false;
        }
        for (Plan child : root.children()) {
            if (!isSPJ(child)) {
                return false;
            }
        }
        return true;
    }
}
