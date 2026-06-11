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
import org.apache.doris.nereids.rules.rewrite.eageraggregation.EagerAggHints.Action;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PushDownAggContext
 */
public class PushDownAggContext {
    // count(if(...)): if(...) push down as a whole
    // sum/min/max(if(truePart, elsePart)): if(...) can be split to sum(truePart) and sum(elsePart)
    public final boolean hasDecomposedAggIf;
    // When aggFunc(if(...)) is present, pushing down the null-supplemented side of the outer join is avoided.
    // This is because null values are highly error-prone,
    // so the push-down operation is not performed during hashCaseWhen.
    public final boolean hasCaseWhen;
    private final List<AggregateFunction> aggFunctions;
    private final List<SlotReference> groupKeys;
    private final HashMap<AggregateFunction, Alias> aliasMap;
    private final Set<Slot> aggFunctionsInputSlots;

    // cascadesContext is used for normalizeAgg
    private final CascadesContext cascadesContext;

    private final boolean passThroughBigJoin;

    // Bilateral push-down plumbing.
    //   - bilateralState: global, shared by every context in the rewrite invocation.
    private final BilateralState bilateralState;

    /**
     * full constructor used by the bilateral push-down path.
     */
    public PushDownAggContext(List<AggregateFunction> aggFunctions,
            List<SlotReference> groupKeys, Map<AggregateFunction, Alias> aliasMap, CascadesContext cascadesContext,
            boolean passThroughBigJoin, boolean hasDecomposedAggIf, boolean hasCaseWhen,
            BilateralState bilateralState) {
        this.groupKeys = groupKeys.stream().distinct().collect(Collectors.toList());
        this.aggFunctions = ImmutableList.copyOf(aggFunctions);
        this.cascadesContext = cascadesContext;

        HashMap<AggregateFunction, Alias> builtAliasMap = new HashMap<>();
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
        this.hasDecomposedAggIf = hasDecomposedAggIf;
        this.hasCaseWhen = hasCaseWhen;
        this.bilateralState = Objects.requireNonNull(bilateralState, "bilateralState cannot be null");
        for (Map.Entry<AggregateFunction, Alias> entry : this.aliasMap.entrySet()) {
            AggregateFunction aggFunction = entry.getKey();
            ExprId id = entry.getValue().getExprId();
            Optional<Action> hintAction = EagerAggHints.decide(aggFunction);
            hintAction.ifPresent(action -> bilateralState.putAction(id, action));
        }

    }

    /**
     * check validation
     * @return true, if groupKeys is not empty
     */
    public boolean isValid() {
        return !groupKeys.isEmpty();
    }

    public HashMap<AggregateFunction, Alias> getAliasMap() {
        return aliasMap;
    }

    public List<AggregateFunction> getAggFunctions() {
        return aggFunctions;
    }

    public List<SlotReference> getGroupKeys() {
        return groupKeys;
    }

    public PushDownAggContext withGroupKeys(List<SlotReference> groupKeys) {
        return new PushDownAggContext(aggFunctions, groupKeys, aliasMap,
                cascadesContext, passThroughBigJoin, hasDecomposedAggIf, hasCaseWhen,
                bilateralState);
    }

    /**
     * Derive a child context for one branch of a join during bilateral push-down.
     */
    public PushDownAggContext forOneBranch(List<AggregateFunction> branchAggFunctions,
            Map<AggregateFunction, Alias> branchAliasMap, List<SlotReference> groupKeys, boolean passThroughBigJoin) {
        return new PushDownAggContext(branchAggFunctions, groupKeys, branchAliasMap,
                cascadesContext, passThroughBigJoin, hasDecomposedAggIf, hasCaseWhen,
                bilateralState);
    }

    public BilateralState getBilateralState() {
        return bilateralState;
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
