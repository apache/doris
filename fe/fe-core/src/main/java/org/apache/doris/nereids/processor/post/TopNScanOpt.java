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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.qe.ConnectContext;

/**
 * topN opt
 * refer to:
 * https://github.com/apache/doris/pull/15558
 * https://github.com/apache/doris/pull/15663
 */

public class TopNScanOpt extends PlanPostProcessor {
    @Override
    public PhysicalTopN visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, CascadesContext ctx) {
        topN.child().accept(this, ctx);
        Plan child = topN.child();
        if (topN.getSortPhase() != SortPhase.LOCAL_SORT) {
            return topN;
        }
        long threshold = getTopNOptLimitThreshold();
        if (threshold == -1 || topN.getLimit() > threshold) {
            return topN;
        }
        if (topN.getOrderKeys().isEmpty()) {
            return topN;
        }
        Expression firstKey = topN.getOrderKeys().get(0).getExpr();
        if (!(firstKey instanceof SlotReference)) {
            return topN;
        }
        if (firstKey.getDataType().isStringLikeType()
                || firstKey.getDataType().isFloatType()
                || firstKey.getDataType().isDoubleType()) {
            return topN;
        }
        while (child != null && (child instanceof Project || child instanceof Filter)) {
            child = child.child(0);
        }
        if (child instanceof PhysicalOlapScan) {
            PhysicalOlapScan scan = (PhysicalOlapScan) child;
            if (scan.getTable().isDupKeysOrMergeOnWrite()) {
                topN.setMutableState(PhysicalTopN.TOPN_RUNTIME_FILTER, true);
            }
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
