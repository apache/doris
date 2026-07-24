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

import org.apache.doris.mtmv.ivm.agg.IvmAggMeta;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Unified, mutable artifacts produced and consumed by the internal IVM rewrite flows.
 *
 * <p>Normalize rewrite populates the normalized plan, row-id determinism, aggregate metadata,
 * and plan signature. Incremental refresh rewrite consumes those artifacts to build the delta
 * plan, while full refresh consumes the plan signature when rebuilding the IVM baseline. The
 * result is statement-scoped and stored in {@code CascadesContext}.
 */
public class IvmRewriteResult {
    // insertion-ordered so row-ids appear in scan order
    private final Map<Slot, Boolean> rowIdDeterminism = new LinkedHashMap<>();
    private Plan normalizedPlan;
    private IvmAggMeta aggMeta;
    private IvmPlanSignature planSignature;
    private boolean normalizeRewritten;
    private boolean incrRefreshRewritten;

    public void addRowId(Slot rowIdSlot, boolean deterministic) {
        rowIdDeterminism.put(rowIdSlot, deterministic);
    }

    public Map<Slot, Boolean> getRowIdDeterminism() {
        return rowIdDeterminism;
    }

    /**
     * Returns true if the given row-id slot is tracked as deterministic.
     * Returns false if the slot is not tracked or is non-deterministic.
     */
    public boolean isDeterministic(Slot slot) {
        return Boolean.TRUE.equals(rowIdDeterminism.get(slot));
    }

    public Plan getNormalizedPlan() {
        return normalizedPlan;
    }

    public void setNormalizedPlan(Plan normalizedPlan) {
        this.normalizedPlan = normalizedPlan;
    }

    /** Returns the normalized aggregate MV metadata, or null if the MV is not an agg MV. */
    public IvmAggMeta getAggMeta() {
        return aggMeta;
    }

    public boolean isAggMv() {
        return aggMeta != null;
    }

    public void setAggMeta(IvmAggMeta aggMeta) {
        this.aggMeta = aggMeta;
    }

    public IvmPlanSignature getPlanSignature() {
        return planSignature;
    }

    public void setPlanSignature(IvmPlanSignature planSignature) {
        this.planSignature = planSignature;
    }

    public boolean isNormalizeRewritten() {
        return normalizeRewritten;
    }

    public void setNormalizeRewritten(boolean normalizeRewritten) {
        this.normalizeRewritten = normalizeRewritten;
    }

    public boolean isIncrRefreshRewritten() {
        return incrRefreshRewritten;
    }

    public void setIncrRefreshRewritten(boolean incrRefreshRewritten) {
        this.incrRefreshRewritten = incrRefreshRewritten;
    }
}
