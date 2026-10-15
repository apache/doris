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

import org.apache.doris.catalog.KeysType;
import org.apache.doris.mtmv.ivm.agg.IvmAggMeta;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.List;
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
    private List<Slot> identityKeySlots;
    // The key type -- and with it the table model, DUP or UNIQUE + merge-on-write -- the MV this
    // statement builds is created with. Fixed while the MV is created and read back from its own
    // key model on every later statement, which is why it is carried per statement: CREATE has no
    // MV to read it from yet. See IvmNormalizeMTMV#judgeKeysType for what decides it.
    private KeysType keysType = KeysType.UNIQUE_KEYS;

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

    /** Returns the identity key slots computed during normalization, or null if not computed. */
    public List<Slot> getIdentityKeySlots() {
        return identityKeySlots;
    }

    public void setIdentityKeySlots(List<Slot> identityKeySlots) {
        this.identityKeySlots = identityKeySlots;
    }

    public KeysType getKeysType() {
        return keysType;
    }

    /**
     * Only the two key types an IVM MV can be built with are meaningful, and which one it is also
     * says which of the two table models it is: {@link KeysType#UNIQUE_KEYS} with merge-on-write,
     * maintained through the hidden row-id, or {@link KeysType#DUP_KEYS}, a detail table the refresh
     * only appends to. Defaults to the former, which is what every IVM MV got before this.
     */
    public void setKeysType(KeysType keysType) {
        boolean isIvmKeysType = keysType == KeysType.UNIQUE_KEYS || keysType == KeysType.DUP_KEYS;
        Preconditions.checkArgument(isIvmKeysType,
                "IVM MV key type must be UNIQUE_KEYS or DUP_KEYS, but is: %s", keysType);
        this.keysType = keysType;
    }
}
