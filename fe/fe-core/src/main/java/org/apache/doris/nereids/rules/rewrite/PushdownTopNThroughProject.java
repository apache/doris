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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * PushdownTopNThroughProject.
 */
public class PushdownTopNThroughProject extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalTopN(logicalProject())
                .then(topN -> {
                    LogicalProject<Plan> project = topN.child();
                    List<OrderKey> newOrderKeys = topN.getOrderKeys().stream()
                            .map(orderKey -> orderKey.withExpression(
                                    ExpressionUtils.replace(orderKey.getExpr(), project.getAliasToProducer()))).collect(
                                    ImmutableList.toImmutableList());
                    return project.withChildren(
                            new LogicalTopN<>(newOrderKeys, topN.getLimit(), topN.getOffset(), project.child()));
                }).toRule(RuleType.PUSH_TOP_N_THROUGH_PROJECT);
    }
}
