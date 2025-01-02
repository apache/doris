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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.qe.ConnectContext;

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
            TopnFilterPushDownVisitor.PushDownContext pushdownContext = new PushDownContext(topN,
                    topN.getOrderKeys().get(0).getExpr(),
                    topN.getOrderKeys().get(0).isNullFirst());
            topN.accept(pusher, pushdownContext);
        }
        return topN;
    }

    boolean checkTopN(TopN topN) {
        if (!(topN instanceof PhysicalTopN) && !(topN instanceof PhysicalDeferMaterializeTopN)) {
            return false;
        }
        if (topN instanceof PhysicalTopN
                && ((PhysicalTopN) topN).getSortPhase() != SortPhase.LOCAL_SORT) {
            return false;
        } else {
            if (topN instanceof PhysicalDeferMaterializeTopN
                    && ((PhysicalDeferMaterializeTopN) topN).getSortPhase() != SortPhase.LOCAL_SORT) {
                return false;
            }
        }

        if (topN.getOrderKeys().isEmpty()) {
            return false;
        }

        // topn opt
        long topNOptLimitThreshold = getTopNOptLimitThreshold();
        if (topNOptLimitThreshold == -1 || topN.getLimit() > topNOptLimitThreshold) {
            return false;
        }

        Expression firstKey = topN.getOrderKeys().get(0).getExpr();

        if (firstKey.getDataType().isFloatType()
                || firstKey.getDataType().isDoubleType()) {
            return false;
        }
        return true;
    }

    @Override
    public Plan visitPhysicalDeferMaterializeTopN(PhysicalDeferMaterializeTopN<? extends Plan> topN,
            CascadesContext ctx) {
        topN.child().accept(this, ctx);
        if (checkTopN(topN)) {
            TopnFilterPushDownVisitor pusher = new TopnFilterPushDownVisitor(ctx.getTopnFilterContext());
            TopnFilterPushDownVisitor.PushDownContext pushdownContext = new PushDownContext(topN,
                    topN.getOrderKeys().get(0).getExpr(),
                    topN.getOrderKeys().get(0).isNullFirst());
            topN.accept(pusher, pushdownContext);
        }
        return topN;
    }

    private long getTopNOptLimitThreshold() {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
            return ConnectContext.get().getSessionVariable().topnFilterLimitThreshold;
        }
        return -1;
    }
}
