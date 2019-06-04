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
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.base.OptPhysicalProperty;
import org.apache.doris.optimizer.operator.OptLogicalUnion;
import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.operator.OptPhysicalHashAggregate;
import org.apache.doris.optimizer.operator.OptPhysicalUnionAll;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.rule.RuleCallContext;

public class UnionRule extends ImplemetationRule {
    public static UnionRule INSTANCE = new UnionRule();

    public UnionRule() {
        super(OptRuleType.RULE_IMP_UNION,
                OptExpression.create(
                        new OptPhysicalUnionAll(),
                        OptExpression.create(new OptPatternLeaf()),
                        OptExpression.create(new OptPatternLeaf())
                ));
    }

    @Override
    public void transform(RuleCallContext call) {
        final OptExpression originExpr = call.getOrigin();
        Preconditions.checkArgument(originExpr.getInputs().size() == 2,
                "Union can only have tow children.");
        final OptLogicalUnion operator = (OptLogicalUnion) originExpr.getOp();
        if (operator.isUnionAll()) {
            final OptPhysicalUnionAll union = new OptPhysicalUnionAll();
            final OptExpression unionExpr = OptExpression.create(union, originExpr.getInputs());
            call.addNewExpr(unionExpr);
        } else {
            final OptPhysicalProperty outerProperty = (OptPhysicalProperty) originExpr.getProperty();
            final OptPhysicalProperty innerProperty = (OptPhysicalProperty) originExpr.getProperty();
            final OptPhysicalHashAggregate aggregate =
                    OptPhysicalHashAggregate.createLocalAggregate(operator.getGroupBy());
            final OptExpression aggregateExpr = OptExpression.create(aggregate, originExpr.getInputs());
            if (outerProperty.getDistributionSpec().isSingleSatisfySingle(innerProperty.getDistributionSpec())) {
                final OptPhysicalUnionAll union = new OptPhysicalUnionAll();
                final OptExpression unionExpr = OptExpression.create(union, aggregateExpr);
                call.addNewExpr(unionExpr);
            } else {
                final OptPhysicalHashAggregate mergeAggregate =
                        OptPhysicalHashAggregate.createGlobalAggregate(operator.getGroupBy());
                final OptExpression mergeAggregateExpr = OptExpression.create(mergeAggregate, aggregateExpr);
                final OptPhysicalUnionAll union = new OptPhysicalUnionAll();
                final OptExpression unionExpr = OptExpression.create(union, mergeAggregateExpr);
                call.addNewExpr(unionExpr);
            }
        }
    }
}
