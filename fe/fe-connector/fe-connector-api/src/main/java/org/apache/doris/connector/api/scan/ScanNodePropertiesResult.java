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

package org.apache.doris.connector.api.scan;

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
 */
public class ScanNodePropertiesResult {

    private final Map<String, String> properties;
    private final Set<Integer> notPushedConjunctIndices;
    private final boolean hasConjunctTracking;

    /**
     * Creates a result where no fine-grained conjunct tracking is provided.
     * The engine will NOT prune any conjuncts.
     */
    public ScanNodePropertiesResult(Map<String, String> properties) {
        this.properties = properties;
        this.notPushedConjunctIndices = null;
        this.hasConjunctTracking = false;
    }

    /**
     * Creates a result with explicit not-pushed conjunct tracking.
     * An empty set means ALL conjuncts were pushed down and should be pruned.
     *
     * @param properties              scan-node-level properties
     * @param notPushedConjunctIndices indices of conjuncts that were NOT pushed down;
     *                                 empty set means all were pushed
     */
    public ScanNodePropertiesResult(Map<String, String> properties,
            Set<Integer> notPushedConjunctIndices) {
        this.properties = properties;
        this.notPushedConjunctIndices = notPushedConjunctIndices;
        this.hasConjunctTracking = true;
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
