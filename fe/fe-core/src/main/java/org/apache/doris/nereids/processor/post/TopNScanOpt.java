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
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
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
        // if firstKey's column is not present, it means the firstKey is not an original column from scan node
        // for example: "select cast(k1 as INT) as id from tbl1 order by id limit 2;" the firstKey "id" is
        // a cast expr which is not from tbl1 and its column is not present.
        // On the other hand "select k1 as id from tbl1 order by id limit 2;" the firstKey "id" is just an alias of k1
        // so its column is present which is valid for topN optimize
        // see Alias::toSlot() method to get how column info is passed around by alias of slotReference
        Expression firstKey = topN.getOrderKeys().get(0).getExpr();
        if (!firstKey.isColumnFromTable()) {
            return false;
        }

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
            return ConnectContext.get().getSessionVariable().topnOptLimitThreshold;
        }
        return -1;
    }

    private boolean supportPhysicalRelations(PhysicalRelation relation) {
        return relation instanceof PhysicalOlapScan
                || relation instanceof PhysicalOdbcScan
                || relation instanceof PhysicalEsScan
                || relation instanceof PhysicalFileScan
                || relation instanceof PhysicalJdbcScan
                || relation instanceof PhysicalDeferMaterializeOlapScan;
    }
}
