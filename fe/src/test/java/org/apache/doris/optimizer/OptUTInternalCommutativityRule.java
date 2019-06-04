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

package org.apache.doris.optimizer;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.rule.RuleCallContext;
import org.apache.doris.optimizer.rule.transformation.ExplorationRule;

public class OptUTInternalCommutativityRule extends ExplorationRule {

    public static OptUTInternalCommutativityRule INSTANCE = new OptUTInternalCommutativityRule();

    private OptUTInternalCommutativityRule() {
        super(OptRuleType.RULE_EXP_UT_COMMUTATIVITY,
                OptExpression.create(new OptLogicalUTInternalNode(),
                        OptExpression.create(new OptPatternLeaf()),
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
        final OptExpression leftChild = originExpr.getInput(0);
        final OptExpression rightChild = originExpr.getInput(1);
        Preconditions.checkNotNull(leftChild);
        Preconditions.checkNotNull(rightChild);

        // TODO children's tuple need to exchange.
        final OptExpression newJoinExpr = OptExpression.create(new OptLogicalUTInternalNode(),
                rightChild, leftChild);
        call.addNewExpr(newJoinExpr);
    }
}
