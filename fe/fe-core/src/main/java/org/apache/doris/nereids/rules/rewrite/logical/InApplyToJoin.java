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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;

/**
 * Convert InApply to LogicalJoin.
 * <p>
 * Not In -> LEFT_ANTI_JOIN
 * In -> LEFT_SEMI_JOIN
 */
public class InApplyToJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply().when(LogicalApply::isIn).then(apply -> {
            Expression predicate;
            if (apply.isCorrelated()) {
                predicate = ExpressionUtils.and(
                        new EqualTo(((InSubquery) apply.getSubqueryExpr()).getCompareExpr(),
                                apply.right().getOutput().get(0)),
                        apply.getCorrelationFilter().get());
            } else {
                predicate = new EqualTo(((InSubquery) apply.getSubqueryExpr()).getCompareExpr(),
                        apply.right().getOutput().get(0));
            }

            if (((InSubquery) apply.getSubqueryExpr()).isNot()) {
                return new LogicalJoin<>(JoinType.LEFT_ANTI_JOIN, Lists.newArrayList(),
                        ExpressionUtils.extractConjunction(predicate),
                        apply.left(), apply.right());
            } else {
                return new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN, Lists.newArrayList(),
                        ExpressionUtils.extractConjunction(predicate),
                        apply.left(), apply.right());
            }
        }).toRule(RuleType.IN_APPLY_TO_JOIN);
    }
}
