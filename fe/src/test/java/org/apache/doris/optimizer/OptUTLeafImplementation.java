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

import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.rule.RuleCallContext;
import org.apache.doris.optimizer.rule.implementation.ImplemetationRule;

public class OptUTLeafImplementation extends ImplemetationRule {

    public static OptUTLeafImplementation INSTANCE = new OptUTLeafImplementation();

    private OptUTLeafImplementation() {
        super(OptRuleType.RULE_IMP_UT_LEAF,
                OptExpression.create(
                        new OptLogicalUTLeafNode()));
    }

    @Override
    public void transform(RuleCallContext call) {
        final OptExpression newExpr = OptExpression.create(new OptPhysicalUTLeafNode());
        call.addNewExpr(newExpr);
    }
}
