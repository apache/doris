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

package org.apache.doris.connector.paimon;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Tests PaimonConnector's FIX-4 cache knobs (CI 973411): the {@code meta.cache.paimon.table.ttl-second}
 * mapping to the generic schema-cache TTL override (Axis B). The data-snapshot cache itself is covered by
 * {@link PaimonLatestSnapshotCacheTest}; the end-to-end behavior is gated by the docker e2e.
 */
public class PaimonConnectorCacheTest {

    private static PaimonConnector connector(Map<String, String> props) {
        return new PaimonConnector(props, new RecordingConnectorContext());
    }

    private static Map<String, String> props(String ttl) {
        Map<String, String> m = new HashMap<>();
        if (ttl != null) {
            m.put(PaimonConnector.TABLE_CACHE_TTL_SECOND, ttl);
        }
        return m;
    }

    @Test
    public void schemaTtlOverrideAbsentWhenPropertyUnset() {
        // No meta.cache.paimon.table.ttl-second -> no override -> the catalog keeps the engine-default schema
        // cache TTL (the with-cache catalog: schema is cached). MUTATION: returning a value -> red.
        Assertions.assertEquals(OptionalLong.empty(),
                connector(Collections.emptyMap()).schemaCacheTtlSecondOverride());
    }

    @Test
    public void schemaTtlOverrideZeroDisablesSchemaCache() {
        // The no-cache catalog (meta.cache.paimon.table.ttl-second=0) must drive schema.cache.ttl-second=0 so
        // its schema is served FRESH (Test 2 / L112 of test_paimon_table_meta_cache). MUTATION: not mapping
        // ttl-second -> the no-cache catalog would serve stale schema -> red.
        Assertions.assertEquals(OptionalLong.of(0L), connector(props("0")).schemaCacheTtlSecondOverride());
    }

    @Test
    public void schemaTtlOverridePositiveIsPassedThrough() {
        Assertions.assertEquals(OptionalLong.of(3600L), connector(props("3600")).schemaCacheTtlSecondOverride());
    }

    @Test
    public void schemaTtlOverrideIgnoresUnparseableValue() {
        // A malformed value must not break catalog schema caching; fall back to no override (engine default).
        Assertions.assertEquals(OptionalLong.empty(), connector(props("not-a-number")).schemaCacheTtlSecondOverride());
    }
}
