// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.CompareMode;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.AggregateFunction_;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.types.DataType;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Used to generate the merge agg node for distributed execution.
 */
public class AggregateDisassemble extends OneRewriteRuleFactory {

    @Override
    public Rule<Plan> build() {
        return logicalAggregate().when(p -> {
            LogicalAggregate logicalAggregation = p.getOperator();
            return !logicalAggregation.isDisassembled();
        }).thenApply(ctx -> {
            Plan plan = ctx.root;
            Operator operator = plan.getOperator();
            LogicalAggregate agg = (LogicalAggregate) operator;
            List<NamedExpression> outputExpressionList = agg.getOutputExpressionList();
            List<NamedExpression> intermediateAggExpressionList = Lists.newArrayList();
            for (NamedExpression namedExpression : outputExpressionList) {
                namedExpression = (NamedExpression) namedExpression.clone();
                List<AggregateFunction_> functionCallList =
                        namedExpression.collect(AggregateFunction.class::isInstance);
                for (AggregateFunction_ functionCall : functionCallList) {
                    FunctionName functionName = new FunctionName(functionCall.getName());
                    List<Expression> expressionList = functionCall.getArguments();
                    List<Type> staleTypeList = expressionList.stream().map(Expression::getDataType)
                            .map(DataType::toCatalogDataType).collect(Collectors.toList());
                    Function staleFuncDesc = new Function(functionName, staleTypeList,
                            functionCall.getDataType().toCatalogDataType(),
                            // I think an aggregate function will never have a variable length parameters
                            false);
                    Function staleFunc = Catalog.getCurrentCatalog()
                            .getFunction(staleFuncDesc, CompareMode.IS_IDENTICAL);
                    Preconditions.checkArgument(staleFunc instanceof AggregateFunction);
                    AggregateFunction staleAggFunc = (AggregateFunction) staleFunc;
                    Type staleIntermediateType = staleAggFunc.getIntermediateType();
                    Type staleRetType = staleAggFunc.getReturnType();
                    if (staleIntermediateType != null && !staleIntermediateType.equals(staleRetType)) {
                        functionCall.setIntermediate(DataType.convertFromCatalogDataType(staleIntermediateType));
                    }
                }
                intermediateAggExpressionList.add(namedExpression);
            }
            LogicalAggregate localAgg = new LogicalAggregate(
                    agg.getGroupByExprList().stream().map(Expression::clone).collect(Collectors.toList()),
                    intermediateAggExpressionList,
                    true
            );

            Plan childPlan = plan(localAgg, plan.child(0));
            List<Slot> stalePlanOutputSlotList = plan.getOutput();
            List<Slot> childOutputSlotList = childPlan.getOutput();
            int childOutputSize = stalePlanOutputSlotList.size();
            Preconditions.checkState(childOutputSize == childOutputSlotList.size());
            Map<Slot, Slot> staleToNew = new HashMap<>();
            for (int i = 0; i < stalePlanOutputSlotList.size(); i++) {
                staleToNew.put(stalePlanOutputSlotList.get(i), childOutputSlotList.get(i));
            }
            List<Expression> groupByExpressionList = agg.getGroupByExprList();
            for (int i = 0; i < groupByExpressionList.size(); i++) {
                replaceSlot(staleToNew, groupByExpressionList, groupByExpressionList.get(i), i);
            }
            List<NamedExpression> mergeOutputExpressionList = agg.getOutputExpressionList();
            for (int i = 0; i < mergeOutputExpressionList.size(); i++) {
                replaceSlot(staleToNew, mergeOutputExpressionList, mergeOutputExpressionList.get(i), i);
            }
            LogicalAggregate mergeAgg = new LogicalAggregate(
                    groupByExpressionList,
                    mergeOutputExpressionList,
                    true
            );
            return plan(mergeAgg, childPlan);
        }).toRule(RuleType.AGGREGATE_DISASSEMBLE);
    }

    @SuppressWarnings("unchecked")
    private <T extends Expression> void replaceSlot(Map<Slot, Slot> staleToNew,
            List<T> expressionList, Expression root, int index) {
        if (index != -1) {
            if (root instanceof Slot) {
                Slot v = staleToNew.get(root);
                if (v == null) {
                    return;
                }
                expressionList.set(index, (T) v);
                return;
            }
        }
        List<Expression> children = root.children();
        for (int i = 0; i < children.size(); i++) {
            Expression cur = children.get(i);
            if (!(cur instanceof Slot)) {
                replaceSlot(staleToNew, expressionList, cur, -1);
                continue;
            }
            Expression v = staleToNew.get(cur);
            if (v == null) {
                continue;
            }
            children.set(i, v);
        }
    }
}
