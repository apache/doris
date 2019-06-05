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

package org.apache.doris.optimizer.rule.implementation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.base.OptColumnRef;
import org.apache.doris.optimizer.base.OptItemProperty;
import org.apache.doris.optimizer.base.OptPhysicalProperty;
import org.apache.doris.optimizer.operator.*;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.rule.RuleCallContext;

import java.util.List;

public class AggToHashAggRule extends ImplemetationRule {

    public static AggToHashAggRule INSTANCE = new AggToHashAggRule();

    private AggToHashAggRule() {
        super(OptRuleType.RULE_IMP_AGG_TO_HASH_AGG,
                OptExpression.create(
                        new OptLogicalAggregate(),
                        OptExpression.create(new OptPatternLeaf()),
                        OptExpression.create(new OptPatternMultiTree())
                ));
    }


    @Override
    public void transform(RuleCallContext call) {
        final OptExpression expr = call.getOrigin();
        Preconditions.checkArgument(expr.getInput(0) != null
                        && expr.getInput(1) != null,
                "Aggregate does't have child," + "it's not physical operator.");

        // Only need one Agg if child is distributed in one node.
        final OptPhysicalProperty childProperty = (OptPhysicalProperty) expr.getInput(0).getProperty();
        if (childProperty.getDistributionSpec().isSingle()) {
            implementSingleNodeAggExpr(call);
            return;
        }

        // Multi agg distinct
        OptItemProperty itemChildProperty = null;
        if (expr.getInput(1).getProperty() != null) {
            itemChildProperty = (OptItemProperty) expr.getInput(1).getProperty();
            if (itemChildProperty.isDistinctAgg()) {
                implementDistinctAgg(call, itemChildProperty);
                return;
            }
        }

        implementOneStageAgg(call);
    }

    private void implementSingleNodeAggExpr(RuleCallContext call) {
        final OptLogicalAggregate operator = (OptLogicalAggregate)call.getOrigin().getOp();
        final OptPhysicalHashAggregate aggregate =
                OptPhysicalHashAggregate.createLocalAggregate(operator.getGroupBy());
        final OptExpression aggregateExpr = OptExpression.create(
                aggregate,
                call.getOrigin().getInputs());
        call.addNewExpr(aggregateExpr);
    }

    private void implementDistinctAgg(RuleCallContext call, OptItemProperty childProperty) {
        if (childProperty.isMultiAggDistinct()) {
            implementOneStageAgg(call);
        } else if (childProperty.isDistinctAgg()) {
            implementTwoStageAgg(call);
        } else {
            Preconditions.checkState(false);
        }
    }

    private OptExpression createLocalAggExpr(RuleCallContext call) {
        final OptExpression originExpr = call.getOrigin();
        final OptLogicalAggregate originAgg = (OptLogicalAggregate)originExpr.getOp();
        final OptPhysicalHashAggregate localAggregate =
                OptPhysicalHashAggregate.createLocalAggregate(originAgg.getGroupBy());
        return OptExpression.create(localAggregate, originExpr.getInputs());
    }

    private void implementOneStageAgg(RuleCallContext call) {
        // Local Agg
        final List<OptExpression> mergeAggInputs = Lists.newArrayList();
        mergeAggInputs.add(createLocalAggExpr(call));

        // Merge agg project list.
        final OptExpression mergeAggProjectList = createMergeAggProjectListExpr(call);
        mergeAggInputs.add(mergeAggProjectList);

        final OptLogicalAggregate logicalAggregate = (OptLogicalAggregate)call.getOrigin().getOp();
        OptExpression newTopAggExpr;
        if (logicalAggregate.getGroupBy().isEmpty()) {
            newTopAggExpr = OptExpression.create(
                    OptPhysicalHashAggregate.createGlobalAggregate(null),
                    mergeAggInputs);
        }  else {
            newTopAggExpr = OptExpression.create(
                    OptPhysicalHashAggregate.createGlobalAggregate(logicalAggregate.getGroupBy()),
                    mergeAggInputs);
        }

        call.addNewExpr(newTopAggExpr);
    }

    private OptExpression createDistinctLocalAggExpr(RuleCallContext call) {
        final OptExpression originExpr = call.getOrigin();
        final OptLogicalAggregate originAgg = (OptLogicalAggregate)originExpr.getOp();
        final List<OptColumnRef> groupBy = Lists.newArrayList();
        groupBy.addAll(originAgg.getGroupBy());
        groupBy.addAll(originAgg.getDistinctColumns());
        final OptPhysicalHashAggregate localAggregate =
                OptPhysicalHashAggregate.createLocalAggregate(groupBy);
        return OptExpression.create(localAggregate, originExpr.getInputs());
    }

    private void implementTwoStageAgg(RuleCallContext call) {
        // Local Agg
        final List<OptExpression> intermediateInputs = Lists.newArrayList();
        final OptExpression localAggExpr = createDistinctLocalAggExpr(call);
        intermediateInputs.add(localAggExpr);

        // Intermediate Agg
        final OptPhysicalHashAggregate logcalAgg = (OptPhysicalHashAggregate)localAggExpr.getOp();
        final OptPhysicalHashAggregate intermediateAgg =
                OptPhysicalHashAggregate.createIntermidiateAggregate(logcalAgg.getGroupBys());
        final OptExpression intermediateAggExpr = OptExpression.create(intermediateAgg, intermediateInputs);

        // Top local agg inputs
        final List<OptExpression> topLocalAggInputs = Lists.newArrayList();
        topLocalAggInputs.add(intermediateAggExpr);
        final OptExpression mergeAggProjectList = createMergeAggProjectListExpr(call);
        topLocalAggInputs.add(mergeAggProjectList);

        // Top local agg
        final OptPhysicalHashAggregate topLogcalAgg = OptPhysicalHashAggregate.createLocalAggregate(null);
        final OptExpression topLogalAggExpr = OptExpression.create(topLogcalAgg, topLocalAggInputs);

        final OptLogicalAggregate logicalAggregate = (OptLogicalAggregate)call.getOrigin().getOp();
        OptExpression newTopAggExpr;
        if (logicalAggregate.getGroupBy().isEmpty()) {
            // Global agg
            final OptPhysicalHashAggregate globalAgg = OptPhysicalHashAggregate.createGlobalAggregate(null);
            newTopAggExpr = OptExpression.create(globalAgg, topLogalAggExpr);
        } else {
            newTopAggExpr = topLogalAggExpr;
        }

        call.addNewExpr(newTopAggExpr);
    }

    private OptExpression createMergeAggProjectListExpr(RuleCallContext call) {
        final OptItemProjectList mergeProjectList = new OptItemProjectList();
        final List<OptExpression> mergeProjectListChildren = Lists.newArrayList();
        for (OptExpression projectEleExpr : call.getOrigin().getInput(1).getInputs()) {
            // Merge Function call's params.
            final OptItemProjectElement projectElement = (OptItemProjectElement) projectEleExpr.getOp();
            final OptItemColumnRef columnRef = new OptItemColumnRef((projectElement.getColumn()));
            final OptExpression newProjectEleExpr = OptExpression.create(columnRef);

            // Merge Function
            final OptItemFunctionCall functionCall =
                    (OptItemFunctionCall) projectEleExpr.getInput(0).getOp();
            final FunctionCallExpr mergeFunction =
                    (FunctionCallExpr) functionCall.getFunction().clone();
            final OptItemFunctionCall mergeFunctionCall = new OptItemFunctionCall(mergeFunction);
            final OptExpression newFunctionCallExpr = OptExpression.create(mergeFunctionCall, newProjectEleExpr);

            // Merge project element
            final OptColumnRef outputColumnMergeFunction =
                    call.getColumnRefFactor().create(mergeFunction.getFn().getReturnType());
            final OptItemProjectElement mergeFunctionElement = new OptItemProjectElement(outputColumnMergeFunction);

            // Merge project expr
            final OptExpression mergeFunctionElementExpr = OptExpression.create(mergeFunctionElement, newFunctionCallExpr);
            mergeProjectListChildren.add(mergeFunctionElementExpr);
        }
        return OptExpression.create(mergeProjectList, mergeProjectListChildren);
    }
}
