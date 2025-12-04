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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.hint.JoinSkewInfo;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**bind skew hint in DistributeHint*/
@DependsRules({
        LeadingJoin.class
})
public class BindSkewExpr extends BindExpression {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.BINDING_SKEW_EXPR.build(
                    logicalJoin().when(join -> join.getDistributeHint().getSkewInfo() != null)
                    .thenApply(this::bindSkewExpr))
        );
    }

    private LogicalJoin<Plan, Plan> bindSkewExpr(MatchingContext<LogicalJoin<Plan, Plan>> ctx) {
        LogicalJoin<Plan, Plan> join = ctx.root;
        CascadesContext cascadesContext = ctx.cascadesContext;
        SimpleExprAnalyzer analyzer = buildSimpleExprAnalyzer(join, cascadesContext, join.children());

        DistributeHint distributeHint = join.getDistributeHint();
        if (distributeHint.getSkewExpr() != null) {
            Expression skewExpr = analyzer.analyze(distributeHint.getSkewExpr());
            List<Expression> skewValues = new ArrayList<>();
            for (Expression skewValue : join.getDistributeHint().getSkewValues()) {
                skewValue = TypeCoercionUtils.castIfNotSameType(skewValue, skewExpr.getDataType());
                skewValues.add(skewValue);
            }
            distributeHint.setSkewInfo(new JoinSkewInfo(skewExpr, skewValues, false));
        }
        return join;
    }
}
