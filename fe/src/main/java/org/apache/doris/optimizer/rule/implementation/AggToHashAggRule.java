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

import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.*;
import org.apache.doris.optimizer.rule.OptRuleType;

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
    public void transform(OptExpression expr, List<OptExpression> newExprs) {
//        final OptLogicalAggregate operator = (OptLogicalAggregate) expr.getOp();
//        if (operator.isDuplicate()) {
//            return;
//        }
//
//        Preconditions.checkArgument(expr.getInput(0) != null
//                && expr.getInput(0).getOp() instanceof OptPhysical, "Aggregate does't have child," +
//                "it's not physical operator.");
//
//        final OptPhysicalHashAggregate aggregate = new OptPhysicalHashAggregate(operator.getGroupBy(),
//                OptPhysical.HashAggStage.Agg);
//        final OptExpression aggregateExpr = OptExpression.create(
//                aggregate,
//                expr.getInputs());
//        final OptPhysicalProperty childProperty = (OptPhysicalProperty) expr.getInput(0).getProperty();
//        if (childProperty.getDistributionSpec().isSingle()) {
//            newExprs.add(aggregateExpr);
//        } else {
//            final AggregateInfo mergeInfo = operator.getAggInfo().getMergeAggInfo();
//            final List<OptExpression> mergeInputs = Lists.newArrayList();
//            mergeInputs.add(aggregateExpr);
//            final StmtToExpressionConverter convertor = new StmtToExpressionConverter();
//            for (FunctionCallExpr func : mergeInfo.getAggregateExprs()) {
//                mergeInputs.add(convertor.convertExpr(func));
//            }
//            final OptColumnRefSet mergeGroupBy = new OptColumnRefSet();
//            for (Expr e : mergeInfo.getGroupingExprs()) {
//                ItemUtils.collectSlotId(e, mergeGroupBy, true);
//            }
//            final OptPhysicalHashAggregate merge =
//                    new OptPhysicalHashAggregate(mergeGroupBy, OptOperator.HashAggStage.Merge);
//            final OptExpression mergeExpr = OptExpression.create(
//                    merge,
//                    mergeInputs);
//            newExprs.add(mergeExpr);
//        }

    }
}
