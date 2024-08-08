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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Implementation rule that convert logical top-n to physical top-n.
 */
public class LogicalTopNToPhysicalTopN extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalTopN().thenApplyMulti(ctx -> twoPhaseSort(ctx.root))
                .toRule(RuleType.LOGICAL_TOP_N_TO_PHYSICAL_TOP_N_RULE);
    }

    /**
     * before: logicalTopN(off, limit)
     * after:
     *     gatherTopN(limit, off, require gather)
     *     mergeTopN(limit, off, require gather) -> localTopN(off+limit, 0, require any)
     */
    private List<PhysicalTopN<? extends Plan>> twoPhaseSort(LogicalTopN<? extends Plan> logicalTopN) {
        PhysicalTopN<Plan> localSort = new PhysicalTopN<>(logicalTopN.getOrderKeys(),
                logicalTopN.getLimit() + logicalTopN.getOffset(), 0, SortPhase.LOCAL_SORT,
                logicalTopN.getLogicalProperties(), logicalTopN.child(0));
        int sortPhaseNum = 0;
        if (ConnectContext.get() != null) {
            sortPhaseNum = ConnectContext.get().getSessionVariable().sortPhaseNum;
        }
        if (sortPhaseNum == 1) {
            PhysicalTopN<Plan> onePhaseSort = new PhysicalTopN<>(logicalTopN.getOrderKeys(), logicalTopN.getLimit(),
                    logicalTopN.getOffset(), SortPhase.GATHER_SORT,
                    logicalTopN.getLogicalProperties(), localSort.child(0));
            return Lists.newArrayList(onePhaseSort);
        } else if (sortPhaseNum == 2) {
            PhysicalTopN<Plan> twoPhaseSort = new PhysicalTopN<>(logicalTopN.getOrderKeys(), logicalTopN.getLimit(),
                    logicalTopN.getOffset(), SortPhase.MERGE_SORT, logicalTopN.getLogicalProperties(), localSort);
            return Lists.newArrayList(twoPhaseSort);
        } else {
            PhysicalTopN<Plan> twoPhaseSort = new PhysicalTopN<>(logicalTopN.getOrderKeys(), logicalTopN.getLimit(),
                    logicalTopN.getOffset(), SortPhase.MERGE_SORT, logicalTopN.getLogicalProperties(), localSort);
            PhysicalTopN<Plan> onePhaseSort = new PhysicalTopN<>(logicalTopN.getOrderKeys(), logicalTopN.getLimit(),
                    logicalTopN.getOffset(), SortPhase.GATHER_SORT,
                    logicalTopN.getLogicalProperties(), localSort.child(0));
            return Lists.newArrayList(twoPhaseSort, onePhaseSort);
        }
    }
}
