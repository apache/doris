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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.exceptions.TransformException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.logical.SemiToInner.SemiToInnerContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Transform semi join to inner join.
 * TODO: only support semi -> inner + gby pattern currently
 * Will support pure inner and gby + inner patterns.
 * Will put into cost based transformation framework.
 */
public class SemiToInner extends DefaultPlanRewriter<SemiToInnerContext> implements CustomRewriter {

    /**
     * Types of semi join to inner join pattern.
     */
    public enum TransformPattern {
        INNER,
        GBY_INNER,
        INNER_GBY,
        UNKNOWN
    }

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, new SemiToInnerContext(false, TransformPattern.UNKNOWN,
                                                                new HashSet<>(), new ArrayList<>(),
                                        Optional.empty()));
    }

    @Override
    public Plan visit(Plan plan, SemiToInnerContext context) {
        plan = super.visit(plan, context);
        if (context.visited) {
            if (context.pattern == TransformPattern.INNER_GBY) {
                return transformToInnerGby((LogicalPlan) plan, context);
            } else if (context.pattern == TransformPattern.INNER) {
                return transformToInner((LogicalPlan) plan, context);
            } else if (context.pattern == TransformPattern.GBY_INNER) {
                return transformToGbyInner((LogicalPlan) plan, context);
            } else {
                return plan;
            }
        } else {
            return plan;
        }
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin join, SemiToInnerContext context) {
        if (join.getJoinType() == JoinType.LEFT_SEMI_JOIN) {
            context.pattern = getTransformPattern(join);
            if (context.pattern == TransformPattern.INNER_GBY) {
                context.visited = true;
                context.subQueryCombinedHavingExpr = join.getSubQueryCombinedHavingExpr();
                context.groupByExpressions = getAllUniqueColumnsInJoin((LogicalPlan) join.left());
                return new LogicalJoin<>(JoinType.INNER_JOIN, join.getHashJoinConjuncts(),
                    join.getOtherJoinConjuncts(),
                    JoinHint.NONE,
                    join.getMarkJoinSlotReference(),
                    Optional.empty(),
                    (LogicalPlan) join.left(), (LogicalPlan) join.right());
            } else {
                return join;
            }
        } else {
            return join;
        }
    }

    /**
     * Semi join to inner transformation context
     */
    public static class SemiToInnerContext {
        boolean visited;
        TransformPattern pattern;
        Set<Slot> outerDependantSlots;
        List<Expression> groupByExpressions;
        Optional<Expression> subQueryCombinedHavingExpr;

        public SemiToInnerContext(boolean visited,
                                  TransformPattern pattern,
                                  Set<Slot> outerDependantSlots,
                                  List<Expression> groupByExpressions,
                                  Optional<Expression> subQueryCombinedHavingExpr) {
            this.visited = visited;
            this.pattern = pattern;
            this.outerDependantSlots = outerDependantSlots;
            this.groupByExpressions = groupByExpressions;
            this.subQueryCombinedHavingExpr = subQueryCombinedHavingExpr;
        }
    }

    private TransformPattern getTransformPattern(LogicalJoin join) {
        // only support inner gby pattern currently.
        if (join.getSubQueryCombinedHavingExpr().isPresent()) {
            return TransformPattern.INNER_GBY;
        } else {
            return TransformPattern.UNKNOWN;
        }
    }

    private LogicalPlan transformToInnerGby(LogicalPlan plan, SemiToInnerContext context) {
        if (!(plan instanceof LogicalProject || plan instanceof LogicalFilter)) {
            LogicalPlan topOperator = plan;
            LogicalPlan childOperator = (LogicalPlan) topOperator.child(0);
            context.outerDependantSlots = topOperator.getInputSlots();

            LogicalProject project;
            if (childOperator instanceof LogicalProject) {
                // merge project has been applied
                project = (LogicalProject) childOperator;
                LogicalFilter filter = (LogicalFilter) project.child(0);
                project = new LogicalProject(filter.getOutput(), filter);
            } else {
                LogicalFilter filter = (LogicalFilter) childOperator;
                project = new LogicalProject(filter.getOutput(), filter);
            }
            // build aggr on new project
            Set<Expression> combinedGbyExprs = new HashSet<>();
            Set<Expression> outputExprsAfterGby = new HashSet<>();
            // group by item exprs
            combinedGbyExprs.addAll(context.groupByExpressions);
            combinedGbyExprs.addAll(context.outerDependantSlots);

            // build (sum(...) as alias) expr
            Expression aggrExprFromHavingExpr = extractAggrExprFromHavingExpr(context.subQueryCombinedHavingExpr.get());
            Alias aggrExprAlias = new Alias(aggrExprFromHavingExpr, aggrExprFromHavingExpr.toSql());
            // group by output use alias expr
            outputExprsAfterGby.addAll(combinedGbyExprs);
            outputExprsAfterGby.add(aggrExprAlias);

            Set<Expression> havingExprAliasList = new HashSet<>();
            // having filter use alias = 0
            EqualTo havingExpr = new EqualTo(aggrExprAlias.toSlot(), new BigIntLiteral(0));
            havingExprAliasList.add(havingExpr);

            LogicalAggregate newAggr = new LogicalAggregate(new ArrayList<>(combinedGbyExprs),
                    new ArrayList<>(outputExprsAfterGby), project);
            LogicalFilter havingFilter = new LogicalFilter(havingExprAliasList, newAggr);
            LogicalProject newProject = new LogicalProject(new ArrayList<>(combinedGbyExprs), havingFilter);
            context.visited = false;

            return (LogicalPlan) topOperator.withChildren(newProject);
        } else {
            return plan;
        }
    }

    private LogicalPlan transformToInner(LogicalPlan plan, SemiToInnerContext context) {
        return plan;
    }

    private LogicalPlan transformToGbyInner(LogicalPlan plan, SemiToInnerContext context) {
        return plan;
    }

    private Expression extractAggrExprFromHavingExpr(Expression havingExpr) {
        return ((EqualTo) havingExpr).child(0);
    }

    private List<Expression> getAllUniqueColumnsInJoin(LogicalPlan plan) {
        List<LogicalOlapScan> allTsPlan = new ArrayList<>();
        List<Expression> allUniqueColumnsInJoin = new ArrayList<>();
        findAllTsFromLogicalJoin(plan, allTsPlan);
        if (allTsPlan.isEmpty()) {
            throw new TransformException("unexpected plan during semi to inner transformation");
        } else {
            for (LogicalOlapScan ts : allTsPlan) {
                OlapTable table = ts.getTable();
                List<Column> ukList = table.getKeysType() == KeysType.UNIQUE_KEYS
                        ? table.getKeyList() : ImmutableList.of();
                List<Slot> slots = ts.getOutput();
                for (Column col : ukList) {
                    String columnName = col.getName();
                    Expression columnExpr = getColumnExpressionFromName(slots, columnName);
                    if (!allUniqueColumnsInJoin.contains(columnExpr)) {
                        allUniqueColumnsInJoin.add(columnExpr);
                    }
                }
            }
        }
        return allUniqueColumnsInJoin;
    }

    private void findAllTsFromLogicalJoin(LogicalPlan plan, List<LogicalOlapScan> allTsPlan) {
        if (plan instanceof LogicalOlapScan) {
            allTsPlan.add((LogicalOlapScan) plan);
        } else {
            for (Plan p : plan.children()) {
                LogicalPlan lp = (LogicalPlan) p;
                findAllTsFromLogicalJoin(lp, allTsPlan);
            }
        }
    }

    private Expression getColumnExpressionFromName(List<Slot> slots, String columnName) {
        for (Slot slot : slots) {
            if (slot.getName().equals(columnName)) {
                return slot;
            }
        }
        throw new TransformException("failed to get column expr from column name");
    }
}
