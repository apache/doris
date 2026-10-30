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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Intermediate target specification for one aggregate function produced while normalizing the aggregate.
 *
 * <p>The normalize rule receives aliases from the original aggregate. The spec keeps those visible aliases,
 * adds processor-owned hidden state aliases, and exposes the hidden aggregate outputs that must be appended to the
 * normalized Aggregate. After that Aggregate is rebuilt and its output slots are stable, the spec is converted into
 * an {@link IvmAggTarget}. Only {@link IvmAggTarget} is stored in {@link IvmAggMeta} for delta/apply rewrite.
 */
public class IvmAggTargetSpec {
    private final int ordinal;
    private final IvmAggFunctionKind functionKind;
    private final Alias visibleAlias;
    private final Map<IvmAggStateKey, Alias> hiddenStateAliases;
    private final Set<IvmAggStateKey> internalStateKeys;
    private final Map<IvmAggStateKey, Alias> derivedStateAliases;
    private final List<Expression> targetArguments;
    private final long minMaxTopK;

    public IvmAggTargetSpec(int ordinal, IvmAggFunctionKind functionKind, Alias visibleAlias,
            Map<IvmAggStateKey, Alias> hiddenStateAliases, Set<IvmAggStateKey> internalStateKeys,
            Map<IvmAggStateKey, Alias> derivedStateAliases,
            List<Expression> targetArguments, long minMaxTopK) {
        this.ordinal = ordinal;
        this.functionKind = Objects.requireNonNull(functionKind);
        this.visibleAlias = Objects.requireNonNull(visibleAlias);
        this.hiddenStateAliases = ImmutableMap.copyOf(hiddenStateAliases);
        this.internalStateKeys = ImmutableSet.copyOf(internalStateKeys);
        this.derivedStateAliases = ImmutableMap.copyOf(derivedStateAliases);
        this.targetArguments = ImmutableList.copyOf(targetArguments);
        this.minMaxTopK = minMaxTopK;
    }

    /** Hidden aggregate aliases that must be appended to the normalized aggregate output. */
    public List<NamedExpression> getHiddenAggregateOutputs() {
        return ImmutableList.copyOf(hiddenStateAliases.values());
    }

    /**
     * Names of the aggregate outputs computed only to derive another state column. They stay in the
     * aggregate, but the project above it does not project them, so they are not MV columns.
     */
    public Set<String> getInternalAggregateOutputNames() {
        ImmutableSet.Builder<String> names = ImmutableSet.builder();
        for (IvmAggStateKey stateKey : internalStateKeys) {
            names.add(hiddenStateAliases.get(stateKey).getName());
        }
        return names.build();
    }

    /**
     * Hidden state aliases computed by the project above the normalized aggregate, from the
     * aggregate's own state columns. They are not aggregate outputs.
     */
    public List<NamedExpression> getDerivedStateOutputs() {
        return ImmutableList.copyOf(derivedStateAliases.values());
    }

    /** Builds target metadata before the normalized Aggregate assigns final output slots. */
    public IvmAggTarget toPlaceholderTarget() {
        ImmutableMap.Builder<IvmAggStateKey, Slot> hiddenSlots = ImmutableMap.builder();
        for (Map.Entry<IvmAggStateKey, Alias> entry : hiddenStateAliases.entrySet()) {
            if (!internalStateKeys.contains(entry.getKey())) {
                hiddenSlots.put(entry.getKey(), entry.getValue().toSlot());
            }
        }
        for (Map.Entry<IvmAggStateKey, Alias> entry : derivedStateAliases.entrySet()) {
            hiddenSlots.put(entry.getKey(), entry.getValue().toSlot());
        }
        // No value-state carrier yet: normalize materializes one only if an upper layer drops the
        // visible column, which is not known while the spec is still being built.
        return new IvmAggTarget(ordinal, functionKind, visibleAlias.toSlot(), hiddenSlots.build(), null,
                visibleAlias.isNameFromChild(), targetArguments, minMaxTopK);
    }
}
