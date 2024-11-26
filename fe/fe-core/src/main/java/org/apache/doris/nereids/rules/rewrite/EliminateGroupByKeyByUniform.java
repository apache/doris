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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * +--aggregate(group by a, b output a#0 ,b#1, max(c) as max(c)#2)
 * (a is uniform and not null: e.g. a is projection 2 as a in logicalProject)
 * ->
 * +--aggregate(group by b output b#1, any_value(a#0) as a#3, max(c)#2)
 * if output any_value(a#0) as a#0, the uniqueness of ExprId #0 is violated, because #0 is both any_value(a#0) and a#0
 * error will occurs in other module(e.g. mv rewrite).
 * As a result, new aggregate outputs #3 instead of #0, but upper plan refer slot #0,
 * therefore, all references to #0 in the upper plan need to be changed to #3.
 * use ExprIdRewriter to do this ExprId rewrite, and use CustomRewriter to rewrite upward。
 * */
public class EliminateGroupByKeyByUniform extends DefaultPlanRewriter<Map<ExprId, ExprId>> implements CustomRewriter {
    private ExprIdRewriter exprIdReplacer;

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        Optional<CTEId> cteId = jobContext.getCascadesContext().getCurrentTree();
        if (cteId.isPresent()) {
            return plan;
        }
        Map<ExprId, ExprId> replaceMap = new HashMap<>();
        ExprIdRewriter.ReplaceRule replaceRule = new ExprIdRewriter.ReplaceRule(replaceMap);
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
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Map<ExprId, ExprId> replaceMap) {
        aggregate = visitChildren(this, aggregate, replaceMap);
        aggregate = (LogicalAggregate<? extends Plan>) exprIdReplacer.rewriteExpr(aggregate, replaceMap);

        if (aggregate.getGroupByExpressions().isEmpty() || aggregate.getSourceRepeat().isPresent()) {
            return aggregate;
        }
        DataTrait aggChildTrait = aggregate.child().getLogicalProperties().getTrait();
        // Get the Group by column of agg. If there is a uniform one, delete the group by key.
        Set<Expression> removedExpression = new LinkedHashSet<>();
        List<Expression> newGroupBy = new ArrayList<>();
        for (Expression groupBy : aggregate.getGroupByExpressions()) {
            if (!(groupBy instanceof Slot)) {
                newGroupBy.add(groupBy);
                continue;
            }
            if (aggChildTrait.isUniformAndNotNull((Slot) groupBy)) {
                removedExpression.add(groupBy);
            } else {
                newGroupBy.add(groupBy);
            }
        }
        if (removedExpression.isEmpty()) {
            return aggregate;
        }
        // when newGroupBy is empty, need retain one expr in group by, otherwise the result may be wrong in empty table
        if (newGroupBy.isEmpty()) {
            Expression expr = removedExpression.iterator().next();
            newGroupBy.add(expr);
            removedExpression.remove(expr);
        }
        if (removedExpression.isEmpty()) {
            return aggregate;
        }
        List<NamedExpression> newOutputs = new ArrayList<>();
        // If this output appears in the removedExpression column, replace it with any_value
        for (NamedExpression output : aggregate.getOutputExpressions()) {
            if (output instanceof Slot) {
                if (removedExpression.contains(output)) {
                    Alias alias = new Alias(new AnyValue(false, output), output.getName());
                    newOutputs.add(alias);
                    replaceMap.put(output.getExprId(), alias.getExprId());
                } else {
                    newOutputs.add(output);
                }
            } else if (output instanceof Alias) {
                if (removedExpression.contains(output.child(0))) {
                    newOutputs.add(new Alias(
                            new AnyValue(false, output.child(0)), output.getName()));
                } else {
                    newOutputs.add(output);
                }
            } else {
                newOutputs.add(output);
            }
        }

        // Adjust the order of this new output so that aggregate functions are placed at the back
        // and non-aggregated functions are placed at the front.
        List<NamedExpression> aggFuncs = new ArrayList<>();
        List<NamedExpression> orderOutput = new ArrayList<>();
        for (NamedExpression output : newOutputs) {
            if (output.anyMatch(e -> e instanceof AggregateFunction)) {
                aggFuncs.add(output);
            } else {
                orderOutput.add(output);
            }
        }
        orderOutput.addAll(aggFuncs);
        return aggregate.withGroupByAndOutput(newGroupBy, orderOutput);
    }
}
