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

package org.apache.doris.datasource.scan;

import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Pins the two engine-only facts the generic scan node publishes to connectors: the pushed-down limit, and
 * whether the connector took ALL the filtering.
 *
 * <p>WHY they exist: a connector that can ask its source to stop early needs both, and it must not have to be
 * recognized by the engine to get them. Until this pair existed, the generic node made that decision itself by
 * matching one connector's file-format string — the exact pattern this node's own rule text forbids.</p>
 *
 * <p>WHY the values are stringly typed: they ride the same {@code Map<String, String>} property table the
 * connector already receives, which is what lets the fact be added without changing an SPI signature.</p>
 *
 * <p>The insertion ORDER (pruning conjuncts before the thrift delegation, so "everything was pushed" is read
 * off the pruned set) cannot be covered by a static helper test; it is argued in an ATTN comment at the call
 * site and left to review.</p>
 */
public class PluginDrivenScanNodePushdownFactsTest {

    private static Map<String, String> facts(long limit, boolean allPushed) {
        Map<String, String> props = new HashMap<>();
        PluginDrivenScanNode.injectPushdownFacts(props, limit, allPushed);
        return props;
    }

    @Test
    public void publishesTheLimitAndTheAllPushedFlag() {
        Map<String, String> props = facts(5L, true);
        Assertions.assertEquals("5", props.get(ScanNodePropertyKeys.SYNTHETIC_PUSHDOWN_LIMIT));
        Assertions.assertEquals("true", props.get(ScanNodePropertyKeys.SYNTHETIC_ALL_CONJUNCTS_PUSHED));
    }

    @Test
    public void publishesTheAbsentLimitAsANonPositiveValue() {
        // No LIMIT in the query is -1 on the node. A connector reads "not positive" as "no limit"; emitting
        // nothing at all would be indistinguishable from an old engine that never set the key.
        Assertions.assertEquals("-1", facts(-1L, true).get(ScanNodePropertyKeys.SYNTHETIC_PUSHDOWN_LIMIT));
    }

    @Test
    public void publishesFalseWhenFilteringRemains() {
        // The correctness-carrying half: a connector that stops its source early while the engine still has
        // conjuncts to apply loses rows.
        Assertions.assertEquals("false", facts(5L, false).get(ScanNodePropertyKeys.SYNTHETIC_ALL_CONJUNCTS_PUSHED));
    }

    @Test
    public void keysAreTheSharedContractNotLocalLiterals() {
        // Both sides reference these constants from the public module; the literals are pinned here so a
        // rename that misses one side is caught in this repo rather than by a connector silently reading null.
        Assertions.assertEquals("__pushdown_limit", ScanNodePropertyKeys.SYNTHETIC_PUSHDOWN_LIMIT);
        Assertions.assertEquals("__all_conjuncts_pushed", ScanNodePropertyKeys.SYNTHETIC_ALL_CONJUNCTS_PUSHED);
    }
}
