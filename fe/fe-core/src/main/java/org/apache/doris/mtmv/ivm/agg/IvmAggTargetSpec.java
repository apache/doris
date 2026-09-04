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

import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    private final List<Expression> targetArguments;

    public IvmAggTargetSpec(int ordinal, IvmAggFunctionKind functionKind, Alias visibleAlias,
            Map<IvmAggStateKey, Alias> hiddenStateAliases, List<Expression> targetArguments) {
        this.ordinal = ordinal;
        this.functionKind = Objects.requireNonNull(functionKind);
        this.visibleAlias = Objects.requireNonNull(visibleAlias);
        this.hiddenStateAliases = ImmutableMap.copyOf(hiddenStateAliases);
        this.targetArguments = ImmutableList.copyOf(targetArguments);
    }

    /** Hidden aggregate aliases that must be appended to the normalized aggregate output. */
    public List<NamedExpression> getHiddenAggregateOutputs() {
        return ImmutableList.copyOf(hiddenStateAliases.values());
    }

    /** Builds target metadata before the normalized Aggregate assigns final output slots. */
    public IvmAggTarget toPlaceholderTarget() {
        ImmutableMap.Builder<IvmAggStateKey, Slot> hiddenSlots = ImmutableMap.builder();
        for (Map.Entry<IvmAggStateKey, Alias> entry : hiddenStateAliases.entrySet()) {
            hiddenSlots.put(entry.getKey(), entry.getValue().toSlot());
        }
        return new IvmAggTarget(ordinal, functionKind, visibleAlias.toSlot(), hiddenSlots.build(), targetArguments);
    }
}
