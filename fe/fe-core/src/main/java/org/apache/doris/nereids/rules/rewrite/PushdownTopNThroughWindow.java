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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * PushdownTopNThroughWindow push down the TopN through the Window and generate the PartitionTopN.
 */
public class PushdownTopNThroughWindow implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            // topn -> window
            logicalTopN(logicalWindow()).then(topn -> {
                LogicalWindow<Plan> window = topn.child();
                ExprId windowExprId = getExprID4WindowFunc(window);
                if (windowExprId == null) {
                    return topn;
                }

                if (!checkTopNForPartitionLimitPushDown(topn, windowExprId)) {
                    return topn;
                }
                long partitionLimit = topn.getLimit() + topn.getOffset();
                Optional<Plan> newWindow = window.pushPartitionLimitThroughWindow(partitionLimit, true);
                if (!newWindow.isPresent()) {
                    return topn;
                }
                return topn.withChildren(newWindow.get());
            }).toRule(RuleType.PUSH_TOP_N_THROUGH_WINDOW),

            // topn -> projection -> window
            logicalTopN(logicalProject(logicalWindow())).then(topn -> {
                LogicalProject<LogicalWindow<Plan>> project = topn.child();
                LogicalWindow<Plan> window = project.child();
                ExprId windowExprId = getExprID4WindowFunc(window);
                if (windowExprId == null) {
                    return topn;
                }

                if (!checkTopNForPartitionLimitPushDown(topn, windowExprId)) {
                    return topn;
                }
                long partitionLimit = topn.getLimit() + topn.getOffset();
                Optional<Plan> newWindow = window.pushPartitionLimitThroughWindow(partitionLimit, true);
                if (!newWindow.isPresent()) {
                    return topn;
                }
                return topn.withChildren(project.withChildren(newWindow.get()));
            }).toRule(RuleType.PUSH_TOP_N_THROUGH_PROJECT_WINDOW)
        );
    }

    private ExprId getExprID4WindowFunc(LogicalWindow<?> window) {
        List<NamedExpression> windowExprs = window.getWindowExpressions();
        if (windowExprs.size() != 1) {
            return null;
        }
        NamedExpression windowExpr = windowExprs.get(0);
        if (windowExpr.children().size() != 1 || !(windowExpr.child(0) instanceof WindowExpression)) {
            return null;
        }
        return windowExpr.getExprId();
    }

    private boolean checkTopNForPartitionLimitPushDown(LogicalTopN<?> topn, ExprId slotRefID) {
        List<OrderKey> orderKeys = topn.getOrderKeys();
        if (orderKeys.size() != 1) {
            return false;
        }

        OrderKey orderkey = orderKeys.get(0);
        if (!orderkey.isAsc()) {
            return false;
        }

        Expression orderKeyExpr = orderkey.getExpr();
        if (!(orderKeyExpr instanceof SlotReference)) {
            return false;
        }

        return ((SlotReference) orderKeyExpr).getExprId() == slotRefID;
    }
}
