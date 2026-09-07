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

package org.apache.doris.mtmv.ivm.agg;

import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Normalized aggregate MV metadata passed from IVM normalize to aggregate delta/apply rewrite.
 *
 * <p>Function-specific expression logic stays in {@link IvmAggFunctionRegistry} and the aggregate processors it
 * dispatches to. The metadata classes referenced here live as standalone types in this package so each aggregate
 * concept has a stable home.
 */
public class IvmAggMeta {
    private final boolean scalarAgg;
    private final List<Slot> groupKeySlots;
    private final Slot groupCountSlot;
    private final List<IvmAggTarget> aggTargets;

    public IvmAggMeta(boolean scalarAgg, List<Slot> groupKeySlots,
            Slot groupCountSlot, List<IvmAggTarget> aggTargets) {
        this.scalarAgg = scalarAgg;
        this.groupKeySlots = ImmutableList.copyOf(groupKeySlots);
        this.groupCountSlot = Objects.requireNonNull(groupCountSlot);
        this.aggTargets = ImmutableList.copyOf(aggTargets);
    }

    /** True if this is a scalar aggregate (no GROUP BY). */
    public boolean isScalarAgg() {
        return scalarAgg;
    }

    /** The group-by key slots (empty for scalar aggregate). */
    public List<Slot> getGroupKeySlots() {
        return groupKeySlots;
    }

    /** The hidden slot for group-level count (__DORIS_IVM_AGG_COUNT_COL__). */
    public Slot getGroupCountSlot() {
        return groupCountSlot;
    }

    /** All aggregate targets with their hidden state mappings. */
    public List<IvmAggTarget> getAggTargets() {
        return aggTargets;
    }

    @Override
    public String toString() {
        return "IvmAggMeta{scalarAgg=" + scalarAgg
                + ", groupKeys=" + groupKeySlots
                + ", groupCount=" + groupCountSlot
                + ", targets=" + aggTargets + "}";
    }
}
