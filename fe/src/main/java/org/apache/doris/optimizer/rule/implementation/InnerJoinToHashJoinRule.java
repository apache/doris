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
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.*;
import org.apache.doris.optimizer.rule.OptRuleType;

import java.util.List;

public class InnerJoinToHashJoinRule extends ImplemetationRule {

    public static InnerJoinToHashJoinRule INSTANCE = new InnerJoinToHashJoinRule();

    private InnerJoinToHashJoinRule() {
        super(OptRuleType.RULE_IMP_EQ_JOIN_TO_HASH_JOIN,
                OptExpression.create(
                        new OptLogicalInnerJoin(),
                        OptExpression.create(new OptPatternLeaf()),
                        OptExpression.create(new OptPatternLeaf()),
                        OptExpression.create(new OptPatternMultiTree())
                ));
    }

    @Override
    public void transform(OptExpression expr, List<OptExpression> newExprs) {
//        final List<OptExpression> eqConjuncts = Lists.newArrayList();
//        final List<OptExpression> otherConjuncts = Lists.newArrayList();
//        final List<OptExpression> conjuncts = Lists.newArrayList();
//        for (int i = 2; i < expr.getInputs().size(); i++) {
//            final OptExpression child = expr.getInput(i);
//            if (child.isEqConjunct()) {
//                eqConjuncts.add(child);
//            } else if (child.isOtherConjunct()) {
//                otherConjuncts.add(child);
//            } else if (child.isConjunct()) {
//                conjuncts.add(child);
//            } else  {
//                Preconditions.checkArgument(false,
//                        "Join should't have other type scalar except conjunct.");
//            }
//        }
//        if (eqConjuncts.size() < 1) {
//            return;
//        }
//
//        final OptExpression outerChild = expr.getInput(0);
//        final OptExpression innerChild = expr.getInput(1);
//        Preconditions.checkNotNull(outerChild);
//        Preconditions.checkNotNull(innerChild);
//
//        final OptExpression newExpr = OptExpression.create(
//                new OptPhysicalHashJoin(eqConjuncts, otherConjuncts, conjuncts),
//                expr.getInputs());
//        newExprs.add(newExpr);
    }
}
