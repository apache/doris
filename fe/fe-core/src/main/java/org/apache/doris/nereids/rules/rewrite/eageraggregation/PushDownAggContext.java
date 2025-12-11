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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PushDownAggContext
 */
public class PushDownAggContext {
    private final List<AggregateFunction> aggFunctions;
    private final List<NamedExpression> groupKeys;
    private final Map<AggregateFunction, Alias> aliasMap;
    private final Set<Slot> aggFunctionsInputSlots;

    // the group keys that eventually used to generate aggregation node
    private final LinkedHashSet<SlotReference> finalGroupKeys = new LinkedHashSet<>();

    // cascadesContext is used for normalizeAgg
    private final CascadesContext cascadesContext;

    /**
     * constructor
     */
    public PushDownAggContext(List<AggregateFunction> aggFunctions,
            List<NamedExpression> groupKeys,
            CascadesContext cascadesContext) {
        this(aggFunctions, groupKeys, null, cascadesContext);
    }

    /**
     * constructor
     */
    public PushDownAggContext(List<AggregateFunction> aggFunctions,
            List<NamedExpression> groupKeys, Map<AggregateFunction, Alias> aliasMap, CascadesContext cascadesContext) {
        this.groupKeys = groupKeys;
        this.aggFunctions = ImmutableList.copyOf(aggFunctions);
        this.cascadesContext = cascadesContext;

        if (aliasMap == null) {
            ImmutableMap.Builder<AggregateFunction, Alias> aliasMapBuilder = ImmutableMap.builder();
            for (AggregateFunction aggFunction : this.aggFunctions) {
                Alias alias = new Alias(aggFunction, aggFunction.getName());
                aliasMapBuilder.put(aggFunction, alias);
            }
            this.aliasMap = aliasMapBuilder.build();
        } else {
            this.aliasMap = aliasMap;
        }

        this.aggFunctionsInputSlots = aggFunctions.stream()
                .flatMap(aggFunction -> aggFunction.getInputSlots().stream())
                .filter(Slot.class::isInstance)
                .collect(ImmutableSet.toImmutableSet());
    }

    public Map<AggregateFunction, Alias> getAliasMap() {
        return aliasMap;
    }

    public List<AggregateFunction> getAggFunctions() {
        return aggFunctions;
    }

    public List<NamedExpression> getGroupKeys() {
        return groupKeys;
    }

    public PushDownAggContext withGoupKeys(List<NamedExpression> groupKeys) {
        return new PushDownAggContext(aggFunctions, groupKeys, aliasMap, cascadesContext);
    }

    public Set<Slot> getAggFunctionsInputSlots() {
        return aggFunctionsInputSlots;
    }

    public LinkedHashSet<SlotReference> getFinalGroupKeys() {
        return finalGroupKeys;
    }

    public void addFinalGroupKey(SlotReference key) {
        this.finalGroupKeys.add(key);
    }

    public CascadesContext getCascadesContext() {
        return cascadesContext;
    }
}
