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

package org.apache.doris.connector.spi.scan;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulates scan-node-level properties along with filter pushdown metadata.
 *
 * <p>Connectors that perform fine-grained conjunct pushdown (e.g., ES query DSL
 * building) return this from {@link ConnectorScanPlanProvider#getScanNodePropertiesResult}
 * to communicate both the scan properties and which conjuncts were NOT pushed down.</p>
 *
 * <p>The {@code notPushedConjunctIndices} set contains 0-based indices into the
 * AND children of the filter expression, in the same order as the conjuncts list.
 * Conjuncts whose indices are NOT in this set were successfully pushed down and
 * will be pruned from the scan node's conjunct list by the engine.</p>
 *
 * <p><b>This is the only residual protocol the engine acts on per conjunct</b>, and exactly one shipped
 * connector uses it (es). The other channel — returning a non-null remaining filter from
 * {@code ConnectorPushdownOps.applyFilter} — makes the engine keep every conjunct; see
 * {@code FilterApplicationResult.getRemainingFilter} and the {@code pushdown} package javadoc, Rule 5.
 * Because a pruned conjunct is not re-evaluated anywhere, only report an index as pushed when the
 * translation was EXACT: a widened pushdown that would merely cost extra BE work in the other channel
 * returns extra rows here.</p>
 */
public class ScanNodePropertiesResult {

    private final Map<String, String> properties;
    private final Set<Integer> notPushedConjunctIndices;
    private final boolean hasConjunctTracking;

    private ScanNodePropertiesResult(Map<String, String> properties,
            Set<Integer> notPushedConjunctIndices, boolean hasConjunctTracking) {
        this.properties = properties;
        this.notPushedConjunctIndices = notPushedConjunctIndices;
        this.hasConjunctTracking = hasConjunctTracking;
    }

    /**
     * Creates a result WITHOUT fine-grained conjunct tracking: the engine prunes nothing and every conjunct
     * is still evaluated on BE. This is the safe choice, and the right one unless the connector really did
     * translate individual conjuncts exactly.
     *
     * @param properties scan-node-level properties, keyed per {@link ScanNodePropertyKeys}
     */
    public static ScanNodePropertiesResult of(Map<String, String> properties) {
        return new ScanNodePropertiesResult(properties, null, false);
    }

    /**
     * Creates a result WITH explicit not-pushed conjunct tracking: every conjunct whose index is absent from
     * {@code notPushedConjunctIndices} is pruned from the scan node and never re-evaluated, so an empty set
     * claims "all conjuncts were pushed exactly". Only report an index as pushed when the translation was
     * exact — a widened pushdown returns extra rows here.
     *
     * @param properties               scan-node-level properties, keyed per {@link ScanNodePropertyKeys}
     * @param notPushedConjunctIndices indices of conjuncts that were NOT pushed down; empty set means all
     *                                 were pushed
     */
    public static ScanNodePropertiesResult withPushdownTracking(Map<String, String> properties,
            Set<Integer> notPushedConjunctIndices) {
        return new ScanNodePropertiesResult(properties, notPushedConjunctIndices, true);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Returns indices of conjuncts NOT pushed down, or empty set if all pushed.
     * Only valid when {@link #hasConjunctTracking()} is true.
     */
    public Set<Integer> getNotPushedConjunctIndices() {
        return notPushedConjunctIndices != null ? notPushedConjunctIndices : Collections.emptySet();
    }

    /**
     * Returns true if this result carries conjunct pushdown tracking.
     * False means no tracking — the engine should keep all conjuncts.
     */
    public boolean hasConjunctTracking() {
        return hasConjunctTracking;
    }
}
