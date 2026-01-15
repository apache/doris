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

package org.apache.doris.nereids.rules.rewrite.eageraggregation;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PushDownAggContext
 */
public class PushDownAggContext {
    public static final int BIG_JOIN_BUILD_SIZE = 400_000;
    private final List<AggregateFunction> aggFunctions;
    private final List<SlotReference> groupKeys;
    // using IdentityHashMap instead of HashMap is required by queries like tpc-ds q5
    private final IdentityHashMap<AggregateFunction, Alias> aliasMap;
    private final Set<Slot> aggFunctionsInputSlots;

    // cascadesContext is used for normalizeAgg
    private final CascadesContext cascadesContext;

    private final boolean passThroughBigJoin;

    /**
     * constructor
     */
    public PushDownAggContext(List<AggregateFunction> aggFunctions,
            List<SlotReference> groupKeys, Map<AggregateFunction, Alias> aliasMap, CascadesContext cascadesContext,
            boolean passThroughBigJoin) {
        this.groupKeys = groupKeys;
        this.aggFunctions = ImmutableList.copyOf(aggFunctions);
        this.cascadesContext = cascadesContext;

        IdentityHashMap<AggregateFunction, Alias> builtAliasMap = new IdentityHashMap<>();
        if (aliasMap == null) {
            for (AggregateFunction aggFunction : this.aggFunctions) {
                builtAliasMap.put(aggFunction, new Alias(aggFunction, aggFunction.getName()));
            }
        } else {
            for (AggregateFunction aggFunction : this.aggFunctions) {
                Alias alias = aliasMap.get(aggFunction);
                if (alias == null) {
                    alias = new Alias(aggFunction, aggFunction.getName());
                }
                builtAliasMap.put(aggFunction, alias);
            }
        }
        this.aliasMap = builtAliasMap;

        this.aggFunctionsInputSlots = aggFunctions.stream()
                .flatMap(aggFunction -> aggFunction.getInputSlots().stream())
                .filter(Slot.class::isInstance)
                .collect(ImmutableSet.toImmutableSet());
        this.passThroughBigJoin = passThroughBigJoin;
    }

    public PushDownAggContext passThroughBigJoin() {
        return new PushDownAggContext(aggFunctions, groupKeys, aliasMap, cascadesContext, true);
    }

    public IdentityHashMap<AggregateFunction, Alias> getAliasMap() {
        return aliasMap;
    }

    public List<AggregateFunction> getAggFunctions() {
        return aggFunctions;
    }

    public List<SlotReference> getGroupKeys() {
        return groupKeys;
    }

    public PushDownAggContext withGroupKeys(List<SlotReference> groupKeys) {
        return new PushDownAggContext(aggFunctions, groupKeys, aliasMap, cascadesContext, passThroughBigJoin);
    }

    public Set<Slot> getAggFunctionsInputSlots() {
        return aggFunctionsInputSlots;
    }

    public CascadesContext getCascadesContext() {
        return cascadesContext;
    }

    public boolean isPassThroughBigJoin() {
        return passThroughBigJoin;
    }

    @Override
    public String toString() {
        return "PushDownAggContext{"
                + "aggFunctions=" + aggFunctions
                + ", groupKeys=" + groupKeys
                + ", aliasMap=" + aliasMap
                + ", passThroughBigJoin=" + passThroughBigJoin
                + '}';
    }
}
