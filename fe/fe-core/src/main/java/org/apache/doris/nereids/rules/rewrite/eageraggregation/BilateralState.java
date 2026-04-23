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

import org.apache.doris.nereids.rules.rewrite.eageraggregation.EagerAggHints.Action;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Global state for bilateral aggregate push-down. Shared across all
 * {@link PushDownAggContext} instances in one rewrite invocation.
 *
 * <p>State keyed by the {@code pushDownExprId} (the {@link ExprId} of the
 * {@link org.apache.doris.nereids.trees.expressions.Alias} that wraps the push-down aggregate
 * function candidate):
 * <ul>
 *   <li><b>value map idToPushedSlot</b>: pushDownExprId -&gt; value expression to use for this aggregate. When the
 *   aggregate is successfully pushed down, this is the Slot of the partial-aggregate output.
 *   When it is NOT pushed down, this is the original input column (for sum/min/max), the
 *   expression {@code if(col is null, 0, 1)} (for count(col)), or the literal {@code 1}
 *   (for count(*)).</li>
 *   <li><b>count map planToCountSlot</b>: rewritten subtree plan -&gt; optional subtree multiplicity slot.</li>
 * </ul>
 */
public class BilateralState {
    private final Map<ExprId, NamedExpression> idToPushedSlot = new HashMap<>();
    private final Map<ExprId, Boolean> idToActuallyPushed = new HashMap<>();
    private final Map<ExprId, EagerAggHints.Action> idToAction = new HashMap<>();
    private final IdentityHashMap<Plan, Optional<Slot>> planToCountSlot = new IdentityHashMap<>();

    public void putAction(ExprId pushDownExprId, EagerAggHints.Action action) {
        idToAction.put(pushDownExprId, action);
    }

    /**
     * Inherit the eager-agg hint action from the original aggregate alias to a rewritten alias.
     * This is used when project/union pushdown creates a new ExprId for the same logical
     * aggregate. If the rewritten aggregate already matched a direct hint on its own expression,
     * keep that direct match and do not overwrite it.
     */
    public void inheritActionIfAbsent(ExprId sourceExprId, ExprId targetExprId) {
        if (idToAction.containsKey(targetExprId)) {
            return;
        }
        Action action = idToAction.get(sourceExprId);
        if (action != null) {
            idToAction.put(targetExprId, action);
        }
    }

    public Action getAction(ExprId id) {
        return idToAction.get(id);
    }

    public boolean noAction() {
        return idToAction.isEmpty();
    }

    public NamedExpression getPushedAggFuncSlot(ExprId pushDownExprId) {
        return idToPushedSlot.get(pushDownExprId);
    }

    public boolean hasAggFuncOutput(ExprId pushDownExprId) {
        return idToPushedSlot.containsKey(pushDownExprId);
    }

    public boolean isAggFuncActuallyPushed(ExprId pushDownExprId) {
        return idToActuallyPushed.getOrDefault(pushDownExprId, false);
    }

    public void registerPushedAggFuncSlot(ExprId pushDownExprId, NamedExpression value) {
        idToPushedSlot.put(pushDownExprId, value);
        idToActuallyPushed.put(pushDownExprId, true);
    }

    public void registerAggFuncOutput(ExprId pushDownExprId, NamedExpression value, boolean actuallyPushed) {
        idToPushedSlot.put(pushDownExprId, value);
        idToActuallyPushed.put(pushDownExprId, actuallyPushed);
    }

    public void registerCountSlot(Plan plan, Slot countSlot) {
        planToCountSlot.put(plan, Optional.of(countSlot));
    }

    public void registerNoCountSlot(Plan plan) {
        planToCountSlot.put(plan, Optional.empty());
    }

    public Optional<Slot> getCountSlot(Plan plan) {
        return planToCountSlot.getOrDefault(plan, Optional.empty());
    }
}
