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

public class JoinCommutativityRule extends ExplorationRule {

    public static JoinCommutativityRule INSTANCE = new JoinCommutativityRule();

    private JoinCommutativityRule() {
        super(OptRuleType.RULE_EXP_JOIN_COMMUTATIVITY,
                OptExpression.create(new OptLogicalJoin(),
                        OptExpression.create(new OptPatternLeaf()),
                        OptExpression.create(new OptPatternLeaf())));
    }

    public boolean isCompatible(OptRuleType type) {
        if (type == this.type()) {
            return false;
        }
        return true;
    }

    @Override
    public void transform(RuleCallContext call) {
        final OptExpression originExpr = call.getOrigin();
        final OptExpression outerChild = originExpr.getInput(0);
        final OptExpression innerChild = originExpr.getInput(1);
        Preconditions.checkNotNull(outerChild, "Join's outer child can't be null.");
        Preconditions.checkNotNull(innerChild, "Join's inner child can't be null.");

        final List<OptExpression> newChildrens = Lists.newArrayList();
        newChildrens.add(innerChild);
        newChildrens.add(outerChild);
        for (int i = 2; i < originExpr.getInputs().size(); i++) {
            newChildrens.add(originExpr.getInput(i));
        }

        call.addNewExpr(OptExpression.create(new OptLogicalJoin(), newChildrens));
    }
}
