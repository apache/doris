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
 * Removes partition-prunable conjuncts that were registered by {@link
 * org.apache.doris.nereids.rules.rewrite.PruneOlapScanPartition} but kept in
 * the logical plan during cascades. Doing the removal here, after
 * materialized-view rewrite has finished, ensures MV matching observes the
 * original predicates; otherwise the MV view-predicate may incorrectly cover
 * the dropped partition predicate and produce extra rows.
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
        boolean skipPrunePredicate = context.getConnectContext().getSessionVariable().skipPrunePredicate
                || context.getStatementContext().isDelete();
        if (skipPrunePredicate) {
            return filter;
        }
        Set<Expression> prunableConjuncts = entryOpt.get().getRewrittenPrunableConjuncts(scan);
        Set<Expression> remaining = new LinkedHashSet<>(filter.getConjuncts());
        boolean changed = remaining.removeAll(prunableConjuncts);
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
