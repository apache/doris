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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AggregationNode.java
// and modified by Doris

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate.TopnPushInfo;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.qe.ConnectContext;

import org.apache.hadoop.util.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Add SortInfo to Agg. This SortInfo is used as boundary, not used to sort elements.
 * example
 * sql: select count(*) from orders group by o_clerk order by o_clerk limit 1;
 * plan: topn(1) -> aggGlobal -> shuffle -> aggLocal -> scan
 * optimization: aggLocal and aggGlobal only need to generate the smallest row with respect to o_clerk.
 *
 * TODO: the following case is not covered:
 * sql: select sum(o_shippriority) from orders group by o_clerk limit 1;
 * plan: limit -> aggGlobal -> shuffle -> aggLocal -> scan
 * aggGlobal may receive partial aggregate results, and hence is not supported now
 * instance1: input (key=2, v=1) => localAgg => (2, 1) => aggGlobal inst1 => (2, 1)
 * instance2: input (key=1, v=1), (key=2, v=2) => localAgg inst2 => (1, 1)
 * (2,1),(1,1) => limit => may output (2, 1), which is not complete, missing (2, 2) in instance2
 *
 *TOPN:
 *  Precondition: topn orderkeys are the prefix of group keys
 *  TODO: topnKeys could be subset of groupKeys. This will be implemented in future
 *  Pattern 2-phase agg:
 *     topn -> aggGlobal -> distribute -> aggLocal
 *     =>
 *     topn(n) -> aggGlobal(topn=n) -> distribute -> aggLocal(topn=n)
 *  Pattern 1-phase agg:
 *     topn->agg->Any(not agg) -> topn -> agg(topn=n) -> any
 *
 *  LIMIT:
 *  Pattern 1: limit->agg(1phase)->any
 *  Pattern 2: limit->agg(global)->gather->agg(local)
 */
public class PushTopnToAgg extends PlanPostProcessor {
    @Override
    public Plan visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, CascadesContext ctx) {
        topN.child().accept(this, ctx);
        if (ConnectContext.get().getSessionVariable().topnOptLimitThreshold <= topN.getLimit() + topN.getOffset()) {
            return topN;
        }
        Plan topnChild = topN.child();
        if (topnChild instanceof PhysicalProject) {
            topnChild = topnChild.child(0);
        }
        if (topnChild instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate<? extends Plan> upperAgg = (PhysicalHashAggregate<? extends Plan>) topnChild;
            List<OrderKey> orderKeys = tryGenerateOrderKeyByGroupKeyAndTopnKey(topN, upperAgg);
            if (!orderKeys.isEmpty()) {

                if (upperAgg.getAggPhase().isGlobal() && upperAgg.getAggMode() == AggMode.BUFFER_TO_RESULT) {
                    upperAgg.setTopnPushInfo(new TopnPushInfo(
                            orderKeys,
                            topN.getLimit() + topN.getOffset()));
                    if (upperAgg.child() instanceof PhysicalDistribute
                            && upperAgg.child().child(0) instanceof PhysicalHashAggregate) {
                        PhysicalHashAggregate<? extends Plan> bottomAgg =
                                (PhysicalHashAggregate<? extends Plan>) upperAgg.child().child(0);
                        bottomAgg.setTopnPushInfo(new TopnPushInfo(
                                orderKeys,
                                topN.getLimit() + topN.getOffset()));
                    }
                } else if (upperAgg.getAggPhase().isLocal() && upperAgg.getAggMode() == AggMode.INPUT_TO_RESULT) {
                    // one phase agg
                    upperAgg.setTopnPushInfo(new TopnPushInfo(
                            orderKeys,
                            topN.getLimit() + topN.getOffset()));
                }
            }
        }
        return topN;
    }

    /**
     return true, if topn order-key is prefix of agg group-key, ignore asc/desc and null_first
     TODO order-key can be subset of group-key. BE does not support now.
     */
    private List<OrderKey> tryGenerateOrderKeyByGroupKeyAndTopnKey(PhysicalTopN<? extends Plan> topN,
            PhysicalHashAggregate<? extends Plan> agg) {
        List<OrderKey> orderKeys = Lists.newArrayListWithCapacity(agg.getGroupByExpressions().size());
        if (topN.getOrderKeys().size() > agg.getGroupByExpressions().size()) {
            return orderKeys;
        }
        List<Expression> topnKeys = topN.getOrderKeys().stream()
                .map(OrderKey::getExpr).collect(Collectors.toList());
        for (int i = 0; i < topN.getOrderKeys().size(); i++) {
            // prefix check
            if (!topnKeys.get(i).equals(agg.getGroupByExpressions().get(i))) {
                return Lists.newArrayList();
            }
            orderKeys.add(topN.getOrderKeys().get(i));
        }
        for (int i = topN.getOrderKeys().size(); i < agg.getGroupByExpressions().size(); i++) {
            orderKeys.add(new OrderKey(agg.getGroupByExpressions().get(i), true, false));
        }
        return orderKeys;
    }

    @Override
    public Plan visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, CascadesContext ctx) {
        limit.child().accept(this, ctx);
        if (ConnectContext.get().getSessionVariable().topnOptLimitThreshold <= limit.getLimit() + limit.getOffset()) {
            return limit;
        }
        Plan limitChild = limit.child();
        if (limitChild instanceof PhysicalProject) {
            limitChild = limitChild.child(0);
        }
        if (limitChild instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate<? extends Plan> upperAgg = (PhysicalHashAggregate<? extends Plan>) limitChild;
            if (upperAgg.getAggPhase().isGlobal() && upperAgg.getAggMode() == AggMode.BUFFER_TO_RESULT) {
                Plan child = upperAgg.child();
                Plan grandChild = child.child(0);
                if (child instanceof PhysicalDistribute
                        && ((PhysicalDistribute<?>) child).getDistributionSpec() instanceof DistributionSpecGather
                        && grandChild instanceof PhysicalHashAggregate) {
                    upperAgg.setTopnPushInfo(new TopnPushInfo(
                            generateOrderKeyByGroupKey(upperAgg),
                            limit.getLimit() + limit.getOffset()));
                    PhysicalHashAggregate<? extends Plan> bottomAgg =
                            (PhysicalHashAggregate<? extends Plan>) grandChild;
                    bottomAgg.setTopnPushInfo(new TopnPushInfo(
                            generateOrderKeyByGroupKey(bottomAgg),
                            limit.getLimit() + limit.getOffset()));
                }
            } else if (upperAgg.getAggMode() == AggMode.INPUT_TO_RESULT) {
                // 1-phase agg
                upperAgg.setTopnPushInfo(new TopnPushInfo(
                        generateOrderKeyByGroupKey(upperAgg),
                        limit.getLimit() + limit.getOffset()));
            }
        }
        return limit;
    }

    private List<OrderKey> generateOrderKeyByGroupKey(PhysicalHashAggregate<? extends Plan> agg) {
        return agg.getGroupByExpressions().stream()
                .map(key -> new OrderKey(key, true, false))
                .collect(Collectors.toList());
    }
}
