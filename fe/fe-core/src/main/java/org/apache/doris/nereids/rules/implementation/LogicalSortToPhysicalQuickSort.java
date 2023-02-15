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
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Implementation rule that convert logical sort to physical sort.
 */
public class LogicalSortToPhysicalQuickSort extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalSort().thenApplyMulti(ctx -> twoPhaseSort(ctx.root))
                .toRule(RuleType.LOGICAL_SORT_TO_PHYSICAL_QUICK_SORT_RULE);
    }

    private List<PhysicalQuickSort<? extends Plan>> twoPhaseSort(LogicalSort logicalSort) {
        PhysicalQuickSort localSort = new PhysicalQuickSort(logicalSort.getOrderKeys(),
                logicalSort.getLogicalProperties(), logicalSort.child(0), SortPhase.LOCAL_SORT);
        PhysicalQuickSort twoPhaseSort = new PhysicalQuickSort<>(
                logicalSort.getOrderKeys(),
                logicalSort.getLogicalProperties(),
                localSort, SortPhase.MERGE_SORT);
        PhysicalQuickSort onePhaseSort = new PhysicalQuickSort<>(
                logicalSort.getOrderKeys(),
                logicalSort.getLogicalProperties(),
                localSort.child(0), SortPhase.GATHER_SORT);
        return Lists.newArrayList(twoPhaseSort, onePhaseSort);
    }
}
