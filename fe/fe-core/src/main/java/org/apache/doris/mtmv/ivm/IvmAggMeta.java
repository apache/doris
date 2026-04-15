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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata describing the aggregate IVM structure of a materialized view.
 * Produced by IvmNormalizeMtmv when it processes a LogicalAggregate.
 * Consumed by IvmDeltaRewriter to generate the delta computation + apply commands.
 */
public class IvmAggMeta {

    /** Supported aggregate types for IVM. */
    public enum AggType {
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX
    }

    /**
     * Describes one aggregate target in the MV and its associated hidden state columns.
     */
    public static class AggTarget {
        private final int ordinal;
        private final AggType aggType;
        private final Slot visibleSlot;
        // Hidden state column slots, keyed by the AggType of the state they store.
        // For example, an AVG target has hidden SUM and COUNT slots keyed by AggType.SUM
        // and AggType.COUNT respectively. Only COUNT, SUM, MIN, MAX are valid keys here
        // (AVG is never a hidden-state kind — it is derived from SUM/COUNT).
        private final Map<AggType, Slot> hiddenStateSlots;
        // the expression(s) from the base scan that feed this aggregate
        // (empty for COUNT(*); may be Slot or compound Expression like v1+v2)
        private final List<Expression> exprArgs;

        public AggTarget(int ordinal, AggType aggType, Slot visibleSlot,
                Map<AggType, Slot> hiddenStateSlots, List<Expression> exprArgs) {
            this.ordinal = ordinal;
            this.aggType = Objects.requireNonNull(aggType);
            this.visibleSlot = Objects.requireNonNull(visibleSlot);
            this.hiddenStateSlots = ImmutableMap.copyOf(hiddenStateSlots);
            this.exprArgs = ImmutableList.copyOf(exprArgs);
        }

        public int getOrdinal() {
            return ordinal;
        }

        public AggType getAggType() {
            return aggType;
        }

        public Slot getVisibleSlot() {
            return visibleSlot;
        }

        /** Whether this is a COUNT(*) target (no expression arguments). */
        public boolean isCountStar() {
            return aggType == AggType.COUNT && exprArgs.isEmpty();
        }

        public Map<AggType, Slot> getHiddenStateSlots() {
            return hiddenStateSlots;
        }

        public Slot getHiddenStateSlot(AggType stateType) {
            return hiddenStateSlots.get(stateType);
        }

        /**
         * Returns the canonical column name for the given state type.
         *
         * <p>If a physical hidden slot exists, its name is returned; otherwise the name
         * is generated via {@link IvmUtil#ivmAggHiddenColumnName}.  This allows delta
         * sub-plan code to use a consistent name for both persisted and transient columns.
         */
        public String stateColumnName(AggType stateType) {
            Slot slot = hiddenStateSlots.get(stateType);
            return slot != null ? slot.getName()
                    : IvmUtil.ivmAggHiddenColumnName(ordinal, stateType.name());
        }

        public List<Expression> getExprArgs() {
            return exprArgs;
        }

        @Override
        public String toString() {
            return "AggTarget{ordinal=" + ordinal + ", type=" + aggType
                    + ", visible=" + visibleSlot.getName()
                    + ", hidden=" + hiddenStateSlots.keySet() + "}";
        }
    }

    private final boolean scalarAgg;
    private final List<Slot> groupKeySlots;
    private final Slot groupCountSlot;
    private final List<AggTarget> aggTargets;

    public IvmAggMeta(boolean scalarAgg, List<Slot> groupKeySlots,
            Slot groupCountSlot, List<AggTarget> aggTargets) {
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
    public List<AggTarget> getAggTargets() {
        return aggTargets;
    }

    @Override
    public String toString() {
        return "IvmAggMeta{scalar=" + scalarAgg
                + ", groupKeys=" + groupKeySlots.size()
                + ", targets=" + aggTargets + "}";
    }
}
