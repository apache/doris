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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

/**
 * topN opt
 * refer to:
 * <a href="https://github.com/apache/doris/pull/15558">...</a>
 * <a href="https://github.com/apache/doris/pull/15663">...</a>
 */

public class TopNScanOpt extends PlanPostProcessor {

    @Override
    public PhysicalTopN<? extends Plan> visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, CascadesContext ctx) {
        Plan child = topN.child().accept(this, ctx);
        topN = rewriteTopN(topN);
        if (child != topN.child()) {
            topN = ((PhysicalTopN) topN.withChildren(child)).copyStatsAndGroupIdFrom(topN);
        }
        return topN;
    }

    @Override
    public Plan visitPhysicalDeferMaterializeTopN(PhysicalDeferMaterializeTopN<? extends Plan> topN,
            CascadesContext context) {
        Plan child = topN.child().accept(this, context);
        if (child != topN.child()) {
            topN = topN.withChildren(ImmutableList.of(child)).copyStatsAndGroupIdFrom(topN);
        }
        PhysicalTopN<? extends Plan> rewrittenTopN = rewriteTopN(topN.getPhysicalTopN());
        if (topN.getPhysicalTopN() != rewrittenTopN) {
            topN = topN.withPhysicalTopN(rewrittenTopN).copyStatsAndGroupIdFrom(topN);
        }
        return topN;
    }

    private PhysicalTopN<? extends Plan> rewriteTopN(PhysicalTopN<? extends Plan> topN) {
        Plan child = topN.child();
        if (topN.getSortPhase() != SortPhase.LOCAL_SORT) {
            return topN;
        }
        if (topN.getOrderKeys().isEmpty()) {
            return topN;
        }

        // topn opt
        long topNOptLimitThreshold = getTopNOptLimitThreshold();
        if (topNOptLimitThreshold == -1 || topN.getLimit() > topNOptLimitThreshold) {
            return topN;
        }
        // if firstKey's column is not present, it means the firstKey is not an original column from scan node
        // for example: "select cast(k1 as INT) as id from tbl1 order by id limit 2;" the firstKey "id" is
        // a cast expr which is not from tbl1 and its column is not present.
        // On the other hand "select k1 as id from tbl1 order by id limit 2;" the firstKey "id" is just an alias of k1
        // so its column is present which is valid for topN optimize
        // see Alias::toSlot() method to get how column info is passed around by alias of slotReference
        Expression firstKey = topN.getOrderKeys().get(0).getExpr();
        if (!firstKey.isColumnFromTable()) {
            return topN;
        }
        if (firstKey.getDataType().isStringLikeType()
                || firstKey.getDataType().isFloatType()
                || firstKey.getDataType().isDoubleType()) {
            return topN;
        }

        OlapScan olapScan;
        while (child instanceof Project || child instanceof Filter) {
            child = child.child(0);
        }
        if (!(child instanceof OlapScan)) {
            return topN;
        }
        olapScan = (OlapScan) child;

        if (olapScan.getTable().isDupKeysOrMergeOnWrite()) {
            return topN.withEnableRuntimeFilter(true);
        }

        return topN;
    }

    private long getTopNOptLimitThreshold() {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
            return ConnectContext.get().getSessionVariable().topnOptLimitThreshold;
        }
        return -1;
    }
}
