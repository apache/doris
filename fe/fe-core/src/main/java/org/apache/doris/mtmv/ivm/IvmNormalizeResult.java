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
 * Holds IVM-related state produced during a Nereids run for an incremental MV.
 * Stored as Optional in CascadesContext — absent when IVM rewrite is not active.
 *
 * rowIdDeterminism: maps each injected row-id slot to whether it is deterministic.
 *   - deterministic (true):  MOW table — row-id = hash(unique keys), stable across refreshes
 *   - non-deterministic (false): DUP_KEYS table — row-id = random 128-bit per insert
 *
 * normalizedPlan: the plan tree after IvmNormalizeMtmv has injected row-id columns.
 *   Stored here so that IvmRefreshManager can retrieve it for external delta rewriting.
 */
public class IvmNormalizeResult {
    // insertion-ordered so row-ids appear in scan order
    private final Map<Slot, Boolean> rowIdDeterminism = new LinkedHashMap<>();
    private Plan normalizedPlan;
    private IvmAggMeta aggMeta;

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
}
