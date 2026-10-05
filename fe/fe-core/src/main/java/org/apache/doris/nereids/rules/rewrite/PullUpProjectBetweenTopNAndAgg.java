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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * try to reduce shuffle cost of topN operator, used to optimize plan after applying Compress_materialize
 *
 * topn(orderKey=[a])
 *   --> project(a+1 as x, a+2 as y, a)
 *      --> any(output(a))
 * =>
 * project(a+1 as x, a+2 as y, a)
 *  --> topn(orderKey=[a])
 *    --> any(output(a))
 *
 */
public class PullUpProjectBetweenTopNAndAgg extends OneRewriteRuleFactory {
    public static final Logger LOG = LogManager.getLogger(PullUpProjectBetweenTopNAndAgg.class);

    @Override
    public Rule build() {
        return logicalTopN(logicalProject(logicalAggregate()))
                .when(topN -> ConnectContext.get() != null
                        && ConnectContext.get().getSessionVariable().enableCompressMaterialize)
                .then(topN -> adjust(topN)).toRule(RuleType.ADJUST_TOPN_PROJECT);
    }

    private Plan adjust(LogicalTopN<? extends Plan> topN) {
        LogicalProject<Plan> project = (LogicalProject<Plan>) topN.child();
        Set<Slot> projectInputSlots = project.getInputSlots();
        Map<SlotReference, SlotReference> keyAsKey = new HashMap<>();
        for (NamedExpression proj : project.getProjects()) {
            if (proj instanceof Alias && ((Alias) proj).child(0) instanceof SlotReference) {
                keyAsKey.put((SlotReference) ((Alias) proj).toSlot(), (SlotReference) ((Alias) proj).child());
            }
        }
        boolean match = true;
        List<OrderKey> newOrderKeys = new ArrayList<>();
        for (OrderKey orderKey : topN.getOrderKeys()) {
            Expression orderExpr = orderKey.getExpr();
            if (orderExpr instanceof SlotReference) {
                if (projectInputSlots.contains(orderExpr)) {
                    newOrderKeys.add(orderKey);
                } else if (keyAsKey.containsKey(orderExpr)) {
                    newOrderKeys.add(orderKey.withExpression(keyAsKey.get(orderExpr)));
                } else {
                    match = false;
                    break;
                }
            } else {
                match = false;
                break;
            }
        }
        if (match) {
            if (project.getProjects().size() >= project.getInputSlots().size()) {
                topN = topN.withChildren(project.children()).withOrderKeys(newOrderKeys);
                project = (LogicalProject<Plan>) project.withChildren(topN);
                return project;
            }
        }
        return topN;
    }
}
