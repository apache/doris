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

package org.apache.doris.datasource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * FIX-CAST-PUSHDOWN (F9) impl-review F9-LIMITOPT-1 — guards
 * {@link PluginDrivenScanNode#effectiveSourceLimit}, which suppresses source-side LIMIT pushdown when
 * non-pushable (CAST) conjuncts were stripped from the filter.
 *
 * <p><b>Why this matters:</b> the F9 fix makes MaxCompute strip CAST conjuncts before pushdown, so
 * the connector sees a filter that no longer reflects them. If the real LIMIT were still pushed, the
 * source (e.g. MaxCompute's row-offset limit-split optimization, which fires on an empty/partition-only
 * filter) could return the first N rows without applying the stripped predicate; BE then re-evaluates
 * the CAST predicate only on those rows and silently UNDER-returns (BE can filter the returned rows
 * down, never recover rows the source never returned). Passing {@code -1} (no source limit) when a
 * conjunct was stripped mirrors legacy, which disabled limit-split whenever a non-partition-equality
 * (incl. CAST) predicate was present. BE still applies the LIMIT.</p>
 */
public class PluginDrivenScanNodeLimitStripTest {

    @Test
    public void strippedConjunctsSuppressSourceLimit() {
        // The load-bearing case: a CAST conjunct was stripped, so the source must NOT apply the LIMIT
        // (else under-return). Must return -1 regardless of the real limit.
        Assertions.assertEquals(-1L, PluginDrivenScanNode.effectiveSourceLimit(10L, true));
        Assertions.assertEquals(-1L, PluginDrivenScanNode.effectiveSourceLimit(1L, true));
    }

    @Test
    public void noStripPassesLimitThrough() {
        // No conjunct stripped -> the real limit flows to the source (legitimate limit pushdown,
        // e.g. limit-opt on a genuinely empty/partition-equality filter).
        Assertions.assertEquals(10L, PluginDrivenScanNode.effectiveSourceLimit(10L, false));
        Assertions.assertEquals(-1L, PluginDrivenScanNode.effectiveSourceLimit(-1L, false));
    }
}
