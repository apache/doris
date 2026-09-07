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

import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Describes one aggregate target in the MV and its associated hidden state columns.
 *
 * <p>This is the stable form of {@link IvmAggTargetSpec}. Normalize first creates an
 * {@link IvmAggTargetSpec} while it is still deciding which hidden aggregate outputs to append. Once the normalized
 * Aggregate output slots are available, the spec is converted to this class and stored in {@link IvmAggMeta}. Delta
 * and apply rewrite use this target to dispatch the matching processor and to resolve visible/hidden state slots.
 */
public class IvmAggTarget {
    private final int ordinal;
    private final IvmAggFunctionKind functionKind;
    private final Slot visibleSlot;
    // Persisted hidden state column slots. For example, an AVG target has hidden SUM and COUNT states.
    private final Map<IvmAggStateKey, Slot> hiddenStateSlots;
    // the expression(s) from the base scan that feed this aggregate
    // (empty for COUNT(*); may be Slot or compound Expression like v1+v2)
    private final List<Expression> exprArgs;

    public IvmAggTarget(int ordinal, IvmAggFunctionKind functionKind, Slot visibleSlot,
            Map<IvmAggStateKey, Slot> hiddenStateSlots, List<Expression> exprArgs) {
        this.ordinal = ordinal;
        this.functionKind = Objects.requireNonNull(functionKind);
        this.visibleSlot = Objects.requireNonNull(visibleSlot);
        this.hiddenStateSlots = ImmutableMap.copyOf(hiddenStateSlots);
        this.exprArgs = ImmutableList.copyOf(exprArgs);
    }

    public int getOrdinal() {
        return ordinal;
    }

    public IvmAggFunctionKind getFunctionKind() {
        return functionKind;
    }

    public Slot getVisibleSlot() {
        return visibleSlot;
    }

    /** Whether this is a COUNT(*) target (no expression arguments). */
    public boolean isCountStar() {
        return functionKind == IvmAggFunctionKind.COUNT && exprArgs.isEmpty();
    }

    public Map<IvmAggStateKey, Slot> getHiddenStateSlots() {
        return hiddenStateSlots;
    }

    public Slot getHiddenStateSlot(IvmAggStateKey stateKey) {
        return hiddenStateSlots.get(stateKey);
    }

    /**
     * Returns the canonical column name for the given state type.
     *
     * <p>If a physical hidden slot exists, its name is returned; otherwise the name
     * is generated via {@link IvmUtil#ivmAggHiddenColumnName}. This allows delta
     * sub-plan code to use a consistent name for both persisted and transient columns.
     */
    public String stateColumnName(IvmAggStateKey stateKey) {
        Slot slot = hiddenStateSlots.get(stateKey);
        return slot != null ? slot.getName()
                : IvmUtil.ivmAggHiddenColumnName(ordinal, stateKey.name());
    }

    public List<Expression> getExprArgs() {
        return exprArgs;
    }

    @Override
    public String toString() {
        return "IvmAggTarget{ordinal=" + ordinal + ", type=" + functionKind
                + ", visible=" + visibleSlot.getName()
                + ", hidden=" + hiddenStateSlots.keySet() + "}";
    }
}
