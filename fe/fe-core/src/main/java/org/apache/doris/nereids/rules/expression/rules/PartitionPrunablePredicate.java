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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * Records that, on a specific physical {@link TableIdentifier} restricted to
 * {@link #selectedPartitionIds}, the {@link #prunableConjuncts} are guaranteed
 * to evaluate to TRUE for every surviving row.
 *
 * <p>The predicate is registered by {@link
 * org.apache.doris.nereids.rules.rewrite.PruneOlapScanPartition} but kept in
 * the logical filter during cascades. The actual removal happens later in
 * {@link org.apache.doris.nereids.processor.post.PrunePartitionPredicate} so
 * that materialized-view rewrite still sees the original predicates. Keeping
 * the predicate in the plan avoids the wrong-result problem in which the MV
 * view-predicate happens to cover the remaining conjuncts after the partition
 * predicate has been silently dropped.
 *
 * <p>Because rewrites between recording and removal may rebuild the scan with
 * fresh slot ids, {@link #snapshotPartitionSlots} captures the slots that
 * appear in the recorded conjuncts. The post-processor maps them onto the
 * actual scan's output slots by column name before performing the conjunct
 * removal.
 */
public class PartitionPrunablePredicate {
    private final TableIdentifier tableIdentifier;
    private final Set<Long> selectedPartitionIds;
    private final List<Slot> snapshotPartitionSlots;
    private final Set<Expression> prunableConjuncts;

    public PartitionPrunablePredicate(TableIdentifier tableIdentifier,
            Set<Long> selectedPartitionIds,
            List<Slot> snapshotPartitionSlots,
            Set<Expression> prunableConjuncts) {
        this.tableIdentifier = tableIdentifier;
        this.selectedPartitionIds = ImmutableSet.copyOf(selectedPartitionIds);
        this.snapshotPartitionSlots = ImmutableList.copyOf(snapshotPartitionSlots);
        this.prunableConjuncts = ImmutableSet.copyOf(prunableConjuncts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionPrunablePredicate that = (PartitionPrunablePredicate) o;
        return tableIdentifier.equals(that.tableIdentifier)
                && selectedPartitionIds.equals(that.selectedPartitionIds)
                && snapshotPartitionSlots.equals(that.snapshotPartitionSlots)
                && prunableConjuncts.equals(that.prunableConjuncts);

    }

    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public Set<Long> getSelectedPartitionIds() {
        return selectedPartitionIds;
    }

    public List<Slot> getSnapshotPartitionSlots() {
        return snapshotPartitionSlots;
    }

    public Set<Expression> getPrunableConjuncts() {
        return prunableConjuncts;
    }
}
