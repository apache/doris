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

package org.apache.doris.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.OptLogicalJoin;
import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.rule.RuleCallContext;

import java.util.List;

public class JoinAssociativityRule extends ExplorationRule {

    public static JoinAssociativityRule INSTANCE = new JoinAssociativityRule();

    private JoinAssociativityRule() {
        super(OptRuleType.RULE_EXP_JOIN_ASSOCIATIVITY,
                OptExpression.create(new OptLogicalJoin(),
                        OptExpression.create(new OptLogicalJoin(),
                                OptExpression.create(new OptPatternLeaf()),
                                OptExpression.create(new OptPatternLeaf())),
                        OptExpression.create(new OptPatternLeaf())));
    }

    @Override
    public boolean isCompatible(OptRuleType type) {
        if (type == this.type()) {
            return false;
        }
        return true;
    }

    @Override
    public void transform(RuleCallContext call) {
        final OptExpression originExpr = call.getOrigin();
        final OptExpression outerChildExpr = originExpr.getInput(0);
        final OptExpression innerChildExpr = originExpr.getInput(1);
        Preconditions.checkNotNull(outerChildExpr);
        Preconditions.checkNotNull(innerChildExpr);

        final OptExpression topJoinEqualConjunctExpr = originExpr.getInput(2);
        final OptExpression topJoinNonequalConjunctExpr = originExpr.getInput(3);
        final List<OptExpression> equalConjunctExprs = Lists.newArrayList();
        final List<OptExpression> nonequalConjunctExprs = Lists.newArrayList();
        for (OptExpression child : topJoinEqualConjunctExpr.getInputs()) {
            equalConjunctExprs.add(child);
        }
        for (OptExpression child : topJoinNonequalConjunctExpr.getInputs()) {
            nonequalConjunctExprs.add(child);
        }

        final OptExpression outerChildJoinOuterChild = outerChildExpr.getInput(0);
        final OptExpression outerChildJoinInnerChild = outerChildExpr.getInput(1);
        Preconditions.checkNotNull(outerChildJoinOuterChild);
        Preconditions.checkNotNull(outerChildJoinInnerChild);

        final OptExpression bottomJoinEqualConjunctExpr = originExpr.getInput(2);
        final OptExpression bottomJoinNonequalConjunctExpr = originExpr.getInput(3);
        for (OptExpression child : bottomJoinEqualConjunctExpr.getInputs()) {
            equalConjunctExprs.add(child);
        }
        for (OptExpression child : bottomJoinNonequalConjunctExpr.getInputs()) {
            nonequalConjunctExprs.add(child);
        }

        final OptExpression newLeftChildJoin = OptExpression.create(
                new OptLogicalJoin(),
                outerChildJoinOuterChild,
                innerChildExpr);
        final OptExpression newTopJoin = OptExpression.create(
                new OptLogicalJoin(),
                newLeftChildJoin,
                outerChildJoinInnerChild);
        call.addNewExpr(newTopJoin);
    }


//    private void assignJoinConjunct(
//            OptExpression joinExpr, List<OptExpression> equalConjunctExprs, List<OptExpression> nonequalConjunctExprs) {
//        final OptExpression outerChildExpr = joinExpr.getInput(0);
//        final OptExpression innerChildExpr = joinExpr.getInput(1);
//        final OptLogicalProperty outerChildProperty = outerChildExpr.getProperty();
//        final OptLogicalProperty innerChildProperty = innerChildExpr.getProperty();
//        for (OptExpression conjunctExpr : equalConjunctExprs) {
//            final OptItemProperty property = conjunctExpr.getItemProperty();
//            final OptColumnRefSet columns = new OptColumnRefSet();
//            columns.include(property.getUsedColumns());
//            columns.include(property.getGeneratedColumns());
//
//
//
//        }
//        equalConjunctExprs.stream().map()
//    }
}
