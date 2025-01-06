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
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;

import java.util.Optional;

/**
 * implement defer materialize top n from logical to physical
 */
public class LogicalDeferMaterializeTopNToPhysicalDeferMaterializeTopN extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalDeferMaterializeTopN().thenApply(ctx -> {
            LogicalDeferMaterializeTopN<? extends Plan> topN = ctx.root;
            PhysicalTopN<? extends Plan> physicalTopN = (PhysicalTopN<? extends Plan>) new LogicalTopNToPhysicalTopN()
                    .build()
                    .transform(topN.getLogicalTopN(), ctx.cascadesContext)
                    .get(0);
            if (physicalTopN.getSortPhase() == SortPhase.MERGE_SORT) {
                return wrap(physicalTopN, topN, wrap((PhysicalTopN<? extends Plan>) physicalTopN.child(), topN,
                        ((PhysicalTopN<?>) physicalTopN.child()).child()));
            } else {
                return wrap(physicalTopN, topN, physicalTopN.child());
            }

        }).toRule(RuleType.LOGICAL_DEFER_MATERIALIZE_TOP_N_TO_PHYSICAL_DEFER_MATERIALIZE_TOP_N_RULE);
    }

    private PhysicalDeferMaterializeTopN<? extends Plan> wrap(PhysicalTopN<? extends Plan> physicalTopN,
            LogicalDeferMaterializeTopN<? extends Plan> logicalWrapped, Plan child) {
        return new PhysicalDeferMaterializeTopN<>(physicalTopN,
                logicalWrapped.getDeferMaterializeSlotIds(), logicalWrapped.getColumnIdSlot(),
                Optional.empty(), logicalWrapped.getLogicalProperties(), child);
    }
}
