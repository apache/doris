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
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.plans.logical.LogicalAggregation;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.analysis.FunctionParams;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Arithmetic;
import org.apache.doris.nereids.trees.expressions.BetweenPredicate;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExpressionVisitor;
import org.apache.doris.nereids.trees.expressions.FunctionCall;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
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
        return logicalAggregation().when(p -> {
            LogicalAggregation logicalAggregation = p.getOperator();
            return !logicalAggregation.isDisassembled();
        }).thenApply(ctx -> {
            Plan plan = ctx.root;
            Operator operator = plan.getOperator();
            LogicalAggregation agg = (LogicalAggregation) operator;
            List<NamedExpression> outputExpressionList = agg.getOutputExpressions();
            List<NamedExpression> intermediateAggExpressionList = agg.getOutputExpressions();
            for (NamedExpression namedExpression : outputExpressionList) {
                namedExpression = (NamedExpression) namedExpression.clone();
                List<FunctionCall> functionCallList = FindFunctionCall.find(namedExpression);
                for (FunctionCall functionCall: functionCallList) {
                    FunctionName functionName = functionCall.getFnName();
                    FunctionParams functionParams = functionCall.getFnParams();
                    List<Expression> expressionList = functionParams.getExpression();
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
                        functionCall.setRetType(DataType.convertFromCatalogDataType(staleIntermediateType));
                    }
                }
                intermediateAggExpressionList.add(namedExpression);
            }
            LogicalAggregation localAgg = new LogicalAggregation(
                    agg.getGroupByExpressions(),
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
            List<Expression> groupByExpressionList = agg.getGroupByExpressions();
            for (int i = 0; i < groupByExpressionList.size(); i++) {
                replaceSlot(staleToNew, groupByExpressionList, groupByExpressionList.get(i), i);
            }
            List<NamedExpression> mergeOutputExpressionList = agg.getOutputExpressions();
            for (int i = 0; i < mergeOutputExpressionList.size(); i++) {
                replaceSlot(staleToNew, mergeOutputExpressionList, mergeOutputExpressionList.get(i), i);
            }
            LogicalAggregation mergeAgg = new LogicalAggregation(
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

    public static class FindFunctionCall extends ExpressionVisitor<Void, List<FunctionCall>> {

        private static final FindFunctionCall functionCall = new FindFunctionCall();

        public static List<FunctionCall> find(Expression expression) {
            List<FunctionCall> functionCallList = new ArrayList<>();
            functionCall.visit(expression, functionCallList);
            return functionCallList;
        }

        @Override
        public Void visit(Expression expr, List<FunctionCall> context) {
            if (expr instanceof FunctionCall) {
                context.add((FunctionCall) expr);
                return null;
            }
            for (Expression child : expr.children()) {
                child.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitFunctionCall(FunctionCall function, List<FunctionCall> context) {
            context.add(function);
            return null;
        }

    }

}
