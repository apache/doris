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
import org.apache.doris.nereids.trees.plans.PartitionPrunablePredicate;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Removes partition-prunable conjuncts after materialized-view rewrite.
 * See {@link PartitionPrunablePredicate} for why their removal is deferred.
 */
public class PrunePartitionPredicate extends PlanPostProcessor {

    @Override
    public Plan visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, CascadesContext context) {
        filter = (PhysicalFilter<? extends Plan>) super.visit(filter, context);
        Plan child = filter.child();
        if (!(child instanceof PhysicalOlapScan)) {
            return filter;
        }
        PhysicalOlapScan scan = (PhysicalOlapScan) child;
        Optional<PartitionPrunablePredicate> entryOpt = scan.getPartitionPrunablePredicates();
        if (!entryOpt.isPresent()) {
            return filter;
        }
        // Short-circuit point queries need equality conjuncts on all key columns kept in
        // the scan for distribution pruning and key tuple construction, see PointQueryExecutor.
        boolean skipPrunePredicate = context.getConnectContext().getSessionVariable().skipPrunePredicate
                || context.getStatementContext().isDelete()
                || context.getStatementContext().isShortCircuitQuery();
        if (skipPrunePredicate) {
            return filter;
        }
        Set<Expression> remaining = new LinkedHashSet<>(filter.getConjuncts());
        PartitionPrunablePredicate entry = entryOpt.get();
        boolean changed = remaining.removeAll(entry.getPrunableConjuncts());
        if (!changed) {
            return filter;
        }
        if (remaining.isEmpty()) {
            return scan;
        }
        return filter.withConjunctsAndChild(remaining, scan)
                .copyStatsAndGroupIdFrom((AbstractPhysicalPlan) filter);
    }

}
