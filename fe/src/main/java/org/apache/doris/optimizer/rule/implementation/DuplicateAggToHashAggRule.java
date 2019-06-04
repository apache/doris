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
import org.apache.doris.optimizer.operator.OptLogicalAggregate;
import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.operator.OptPatternMultiTree;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.rule.RuleCallContext;

public class DuplicateAggToHashAggRule extends ImplemetationRule {

    public static DuplicateAggToHashAggRule INSTANCE = new DuplicateAggToHashAggRule();

    private DuplicateAggToHashAggRule() {
        super(OptRuleType.RULE_IMP_AGG_TO_HASH_AGG,
                OptExpression.create(
                        new OptLogicalAggregate(),
                        OptExpression.create(new OptPatternLeaf()),
                        OptExpression.create(new OptPatternMultiTree())
                ));
    }

//    @Override
//    public void transform(OptExpression expr, List<OptExpression> newExprs) {
//        final OptLogicalAggregate operator = (OptLogicalAggregate) expr.getOp();
//        if (!operator.isDuplicate()) {
//            return;
//        }
//
//        final OptExpression child = expr.getInput(0);
//        Preconditions.checkNotNull(child, "Aggregate must have child.");
//
//        final OptPhysicalHashAggregate intermediate = new OptPhysicalHashAggregate(operator.getGroupBy(),
//                OptPhysical.HashAggStage.Intermediate);
//        final OptExpression aggregateExpr = OptExpression.create(
//                intermediate,
//                expr.getInputs().get(0));
//
//        final List<OptExpression> mergeChildren = Lists.newArrayList();
//        mergeChildren.add(aggregateExpr);
//        for (int i = 1; i < expr.getInputs().size(); i++) {
//            mergeChildren.add(expr.getInput(i));
//        }
//
//        final OptPhysicalHashAggregate merge = new OptPhysicalHashAggregate(operator.getGroupBy(),
//                OptPhysical.HashAggStage.Merge);
//        final OptExpression mergeExpr = OptExpression.create(
//                merge,
//                mergeChildren);
//
//        newExprs.add(mergeExpr);
//    }

    @Override
    public void transform(RuleCallContext call) {

    }
}
