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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.TopnFilterPushDownVisitor.PushDownContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.types.DataType;

/**
 * topN opt
 * refer to:
 * <a href="https://github.com/apache/doris/pull/15558">...</a>
 * <a href="https://github.com/apache/doris/pull/15663">...</a>
 *
 * // [deprecated] only support simple case: select ... from tbl [where ...] order by ... limit ...
 */

public class TopNScanOpt extends PlanPostProcessor {
    @Override
    public PhysicalTopN<? extends Plan> visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, CascadesContext ctx) {
        topN.child().accept(this, ctx);
        if (checkTopN(topN)) {
            TopnFilterPushDownVisitor pusher = new TopnFilterPushDownVisitor(ctx.getTopnFilterContext());
            PhysicalHashAggregate<? extends Plan> aggregateSource = findAggregateSource(topN);
            AbstractPlan source = aggregateSource == null ? topN : aggregateSource;
            Expression probeExpr = aggregateSource == null
                    ? topN.getOrderKeys().get(0).getExpr()
                    : aggregateSource.getGroupByExpressions().get(0);
            TopnFilterPushDownVisitor.PushDownContext pushdownContext = new PushDownContext(topN,
                    source, probeExpr,
                    topN.getOrderKeys().get(0).isNullFirst());
            boolean pushed = source.accept(pusher, pushdownContext);
            if (!pushed && aggregateSource != null) {
                pushdownContext = new PushDownContext(topN, topN,
                        topN.getOrderKeys().get(0).getExpr(),
                        topN.getOrderKeys().get(0).isNullFirst());
                topN.accept(pusher, pushdownContext);
            }
        }
        return topN;
    }

    private PhysicalHashAggregate<? extends Plan> findAggregateSource(
            PhysicalTopN<? extends Plan> topN) {
        Plan topNChild = topN.child();
        if (topNChild instanceof PhysicalProject) {
            topNChild = topNChild.child(0);
        }
        if (!(topNChild instanceof PhysicalHashAggregate)) {
            return null;
        }

        PhysicalHashAggregate<? extends Plan> upperAggregate =
                (PhysicalHashAggregate<? extends Plan>) topNChild;
        if (upperAggregate.getTopnPushInfo() == null) {
            return null;
        }

        Plan aggregateChild = upperAggregate.child();
        if (aggregateChild instanceof PhysicalDistribute) {
            aggregateChild = aggregateChild.child(0);
        }
        if (aggregateChild instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate<? extends Plan> lowerAggregate =
                    (PhysicalHashAggregate<? extends Plan>) aggregateChild;
            if (lowerAggregate.getTopnPushInfo() != null) {
                return lowerAggregate;
            }
            return null;
        }
        return upperAggregate;
    }

    boolean checkTopN(TopN topN) {
        if (!(topN instanceof PhysicalTopN)) {
            return false;
        }
        if (((PhysicalTopN) topN).getSortPhase() != SortPhase.LOCAL_SORT) {
            return false;
        }

        if (topN.getOrderKeys().isEmpty()) {
            return false;
        }

        DataType firstKeyType = topN.getOrderKeys().get(0).getExpr().getDataType();

        return isSupportedTopNRuntimeFilterType(firstKeyType);
    }

    private boolean isSupportedTopNRuntimeFilterType(DataType dataType) {
        return dataType.isBooleanType()
                || dataType.isIntegralType()
                || dataType.isDecimalLikeType()
                || dataType.isStringLikeType()
                || dataType.isDateLikeType()
                || dataType.isTimeType()
                || dataType.isIPType()
                || dataType.isVarBinaryType();
    }

}
