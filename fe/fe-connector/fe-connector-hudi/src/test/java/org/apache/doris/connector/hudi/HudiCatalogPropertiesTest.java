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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.hms.HmsClientConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Unit tests for {@link HudiCatalogProperties} — the connector's single reader of the gateway catalog's
 * property map.
 */
class HudiCatalogPropertiesTest {

    @Test
    void bindsEveryKeyAndDefaults() {
        Map<String, String> m = HudiTestProperties.minimalMap();
        HudiCatalogProperties p = HudiCatalogProperties.of(m);
        Assertions.assertEquals(HudiTestProperties.METASTORE_URI, p.getMetastoreUri());
        Assertions.assertEquals(8, p.getHmsClientPoolSize());
        Assertions.assertFalse(p.isUseHiveSyncPartition());

        m.put(HudiCatalogProperties.HMS_CLIENT_POOL_SIZE, "16");
        m.put(HudiCatalogProperties.USE_HIVE_SYNC_PARTITION, "true");
        HudiCatalogProperties set = HudiCatalogProperties.of(m);
        Assertions.assertEquals(16, set.getHmsClientPoolSize());
        Assertions.assertTrue(set.isUseHiveSyncPartition());
    }

    // The gateway may spell the metastore URI either way; hive accepts both and the sibling receives
    // hive's map verbatim, so a catalog written with the short form must not lose its metastore here.
    @Test
    void uriShortFormIsTheFallbackAndTheLongFormWins() {
        Map<String, String> shortOnly = new LinkedHashMap<>();
        shortOnly.put(HudiCatalogProperties.URI, "thrift://short:9083");
        Assertions.assertEquals("thrift://short:9083",
                HudiCatalogProperties.of(shortOnly).getMetastoreUri());

        Map<String, String> both = new LinkedHashMap<>();
        both.put(HudiCatalogProperties.URI, "thrift://short:9083");
        both.put(HudiCatalogProperties.HIVE_METASTORE_URIS, "thrift://long:9083");
        Assertions.assertEquals("thrift://long:9083",
                HudiCatalogProperties.of(both).getMetastoreUri());
    }

    @Test
    void missingMetastoreUriFailsNamingTheKey() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> HudiCatalogProperties.of(new LinkedHashMap<>()));
        Assertions.assertEquals("HMS URI ('" + HudiCatalogProperties.HIVE_METASTORE_URIS
                + "') is required for Hudi connector", e.getMessage());
    }

    @Test
    void blankMetastoreUriCountsAsMissing() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(HudiCatalogProperties.HIVE_METASTORE_URIS, "   ");
        Assertions.assertThrows(IllegalArgumentException.class, () -> HudiCatalogProperties.of(m));
    }

    // Guards the migration decision recorded on getHmsClientPoolSize(): a malformed pool size is
    // refused rather than silently falling back to 8 as the pre-migration helper did. It is the one
    // behaviour change of this migration, so it gets a test rather than living only in a PR description.
    @Test
    void malformedPoolSizeIsRejectedInsteadOfFallingBackToTheDefault() {
        Map<String, String> m = HudiTestProperties.minimalMap();
        m.put(HudiCatalogProperties.HMS_CLIENT_POOL_SIZE, "8x");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> HudiCatalogProperties.of(m));
        Assertions.assertTrue(e.getMessage().contains(HudiCatalogProperties.HMS_CLIENT_POOL_SIZE),
                "the error must name the offending key, got: " + e.getMessage());
    }

    @Test
    void partitionBatchSizeAcceptsSurroundingWhitespace() {
        Map<String, String> m = HudiTestProperties.minimalMap();
        m.put(HmsClientConfig.PARTITION_BATCH_SIZE_KEY, " 5000 ");
        HudiCatalogProperties p = HudiCatalogProperties.of(m);
        HmsClientConfig config = new HmsClientConfig(p.getRaw(), p.getHmsClientPoolSize());
        Assertions.assertEquals(5000, config.getPartitionBatchSize());
    }

    // Guards DESIGN D3(2): the map this connector receives is the gateway catalog's whole map — hive's
    // keys, the engine's and storage's all live in it — and ALTER CATALOG merges properties, so it can
    // overwrite a key but never remove one. Refusing an unrecognized name would create a catalog that no
    // statement could repair.
    @Test
    void unknownKeysAreTolerated() {
        Map<String, String> m = HudiTestProperties.minimalMap();
        m.put("some_future_key", "x");
        m.put("s3.endpoint", "http://minio:9000");
        m.put("type", "hms");
        Assertions.assertDoesNotThrow(() -> HudiCatalogProperties.of(m));
    }

    // Guards DESIGN D3(1): the connector is rebuilt lazily on every refresh and again on an FE replaying
    // the edit log, so of() must be a pure function of its input — no I/O, no mutation of the caller's map.
    @Test
    void ofIsPureAndRepeatable() {
        Map<String, String> m = HudiTestProperties.minimalMap();
        m.put(HudiCatalogProperties.HMS_CLIENT_POOL_SIZE, "16");
        Map<String, String> before = new LinkedHashMap<>(m);

        HudiCatalogProperties first = HudiCatalogProperties.of(m);
        HudiCatalogProperties second = HudiCatalogProperties.of(m);

        Assertions.assertEquals(before, m, "of() must not mutate the caller's map");
        Assertions.assertEquals(first.getMetastoreUri(), second.getMetastoreUri());
        Assertions.assertEquals(first.getHmsClientPoolSize(), second.getHmsClientPoolSize());
        Assertions.assertEquals(first.getRaw(), second.getRaw());
    }

    @Test
    void rawIsAnImmutableCopyOfTheInput() {
        Map<String, String> m = HudiTestProperties.minimalMap();
        HudiCatalogProperties p = HudiCatalogProperties.of(m);
        m.put("added_afterwards", "v");
        Assertions.assertFalse(p.getRaw().containsKey("added_afterwards"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> p.getRaw().put("k", "v"));
    }
}
