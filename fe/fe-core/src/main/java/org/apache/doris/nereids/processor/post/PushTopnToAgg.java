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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate.TopnPushInfo;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.qe.ConnectContext;

/**
 * Add TopNInfo to Agg. This TopNInfo is used as boundary, not used to sort elements.
 * example
 * sql: select count(*) from orders group by o_clerk order by o_clerk limit 1;
 * plan: topn(1) -> aggGlobal -> shuffle -> aggLocal -> scan
 * optimization: aggLocal and aggGlobal only need to generate the smallest row with respect to o_clerk.
 *
 * This rule only applies to the patterns
 * 1. topn->project->agg, or
 * 2. topn->agg
 * that
 * 1. orderKeys and groupkeys are one-one mapping
 * 2. aggregate is not scalar agg
 * Refer to LimitAggToTopNAgg rule.
 */
public class PushTopnToAgg extends PlanPostProcessor {
    @Override
    public Plan visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, CascadesContext ctx) {
        topN.child().accept(this, ctx);
        if (ConnectContext.get().getSessionVariable().topnOptLimitThreshold <= topN.getLimit() + topN.getOffset()
                && !ConnectContext.get().getSessionVariable().pushTopnToAgg) {
            return topN;
        }
        Plan topNChild = topN.child();
        if (topNChild instanceof PhysicalProject) {
            topNChild = topNChild.child(0);
        }
        if (topNChild instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate<? extends Plan> upperAgg = (PhysicalHashAggregate<? extends Plan>) topNChild;
            if (isGroupKeyIdenticalToOrderKey(topN, upperAgg)) {
                upperAgg.setTopnPushInfo(new TopnPushInfo(
                        topN.getOrderKeys(),
                        topN.getLimit() + topN.getOffset()));
                if (upperAgg.child() instanceof PhysicalDistribute
                        && upperAgg.child().child(0) instanceof PhysicalHashAggregate) {
                    PhysicalHashAggregate<? extends Plan> bottomAgg =
                            (PhysicalHashAggregate<? extends Plan>) upperAgg.child().child(0);
                    if (isGroupKeyIdenticalToOrderKey(topN, bottomAgg)) {
                        bottomAgg.setTopnPushInfo(new TopnPushInfo(
                                topN.getOrderKeys(),
                                topN.getLimit() + topN.getOffset()));
                    }
                } else if (upperAgg.child() instanceof PhysicalHashAggregate) {
                    // multi-distinct plan
                    PhysicalHashAggregate<? extends Plan> bottomAgg =
                            (PhysicalHashAggregate<? extends Plan>) upperAgg.child();
                    if (isGroupKeyIdenticalToOrderKey(topN, bottomAgg)) {
                        bottomAgg.setTopnPushInfo(new TopnPushInfo(
                                topN.getOrderKeys(),
                                topN.getLimit() + topN.getOffset()));
                    }
                }
            }
        }
        return topN;
    }

    private boolean isGroupKeyIdenticalToOrderKey(PhysicalTopN<? extends Plan> topN,
                                                                PhysicalHashAggregate<? extends Plan> agg) {
        if (topN.getOrderKeys().size() != agg.getGroupByExpressions().size()) {
            return false;
        }
        for (int i = 0; i < agg.getGroupByExpressions().size(); i++) {
            Expression groupByKey = agg.getGroupByExpressions().get(i);
            Expression orderKey = topN.getOrderKeys().get(i).getExpr();
            if (!groupByKey.equals(orderKey)) {
                return false;
            }
        }
        return true;
    }
}
