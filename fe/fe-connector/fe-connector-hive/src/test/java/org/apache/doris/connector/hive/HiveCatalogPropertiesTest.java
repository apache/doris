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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.hms.HmsClientConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Tests {@link HiveCatalogProperties} — the typed holder for everything a user writes in
 * {@code CREATE CATALOG} for an hms catalog.
 */
class HiveCatalogPropertiesTest {

    @Test
    void bindsEveryKeyAndDefaults() {
        Map<String, String> m = HiveTestProperties.mapWith(
                HiveCatalogProperties.HMS_CLIENT_POOL_SIZE, "16",
                HiveCatalogProperties.ENABLE_HMS_EVENTS_INCREMENTAL_SYNC, "true",
                HiveCatalogProperties.HMS_EVENTS_BATCH_SIZE_PER_RPC, "100",
                HiveCatalogProperties.IGNORE_ABSENT_PARTITIONS, "false",
                HiveCatalogProperties.ENABLE_MAPPING_VARBINARY, "true",
                HiveCatalogProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "true",
                HiveCatalogProperties.RECURSIVE_DIRECTORIES, "false",
                HiveCatalogProperties.STAGING_DIR, "/tmp/mine");
        HiveCatalogProperties p = HiveCatalogProperties.of(m);
        Assertions.assertEquals(HiveTestProperties.METASTORE_URI, p.getMetastoreUri());
        Assertions.assertEquals(16, p.getHmsClientPoolSize());
        Assertions.assertTrue(p.isHmsEventsIncrementalSyncEnabled());
        Assertions.assertEquals(100, p.getHmsEventsBatchSizePerRpc());
        Assertions.assertFalse(p.isIgnoreAbsentPartitions());
        Assertions.assertTrue(p.isEnableMappingVarbinary());
        Assertions.assertTrue(p.isEnableMappingTimestampTz());
        Assertions.assertFalse(p.isRecursiveDirectories());
        Assertions.assertEquals("/tmp/mine", p.getStagingDir());
    }

    @Test
    void defaultsMatchTheLegacyHandWrittenReaders() {
        HiveCatalogProperties p = HiveTestProperties.minimal();
        Assertions.assertEquals(8, p.getHmsClientPoolSize());
        Assertions.assertFalse(p.isHmsEventsIncrementalSyncEnabled());
        Assertions.assertEquals(500, p.getHmsEventsBatchSizePerRpc());
        // The tolerant default: a partition whose location vanished is skipped with a warning.
        Assertions.assertTrue(p.isIgnoreAbsentPartitions());
        Assertions.assertFalse(p.isEnableMappingVarbinary());
        Assertions.assertFalse(p.isEnableMappingTimestampTz());
        Assertions.assertTrue(p.isRecursiveDirectories());
        Assertions.assertEquals("/tmp/.doris_staging", p.getStagingDir());
    }

    // ===== the metastore URI, its short form, and what reaches the HMS client =====

    @Test
    void uriShortFormIsAccepted() {
        Map<String, String> m = HiveTestProperties.minimalMap();
        m.remove(HiveCatalogProperties.HIVE_METASTORE_URIS);
        m.put(HiveCatalogProperties.URI, "thrift://short:9083");
        Assertions.assertEquals("thrift://short:9083", HiveCatalogProperties.of(m).getMetastoreUri());
    }

    @Test
    void canonicalSpellingWinsOverTheShortForm() {
        Map<String, String> m = HiveTestProperties.mapWith(HiveCatalogProperties.URI, "thrift://short:9083");
        Assertions.assertEquals(HiveTestProperties.METASTORE_URI,
                HiveCatalogProperties.of(m).getMetastoreUri());
    }

    /**
     * Guards the fix for a real defect: {@code HmsConfHelper} copies the property map verbatim into a
     * {@code HiveConf}, which only knows {@code hive.metastore.uris}. A catalog written with the short
     * form used to pass the presence check and then connect nowhere. The properties handed to the HMS
     * client must therefore always carry the canonical key.
     */
    @Test
    void hmsClientPropertiesAlwaysCarryTheCanonicalUriKey() {
        Map<String, String> m = HiveTestProperties.minimalMap();
        m.remove(HiveCatalogProperties.HIVE_METASTORE_URIS);
        m.put(HiveCatalogProperties.URI, "thrift://short:9083");
        Map<String, String> forClient = HiveCatalogProperties.of(m).getHmsClientProperties();
        Assertions.assertEquals("thrift://short:9083",
                forClient.get(HiveCatalogProperties.HIVE_METASTORE_URIS));
        // The short form is still carried: sibling connectors read it straight from this map.
        Assertions.assertEquals("thrift://short:9083", forClient.get(HiveCatalogProperties.URI));
    }

    @Test
    void hmsClientPropertiesKeepEveryOtherKeyVerbatim() {
        Map<String, String> m = HiveTestProperties.mapWith("hadoop.security.authentication", "kerberos",
                "s3.endpoint", "http://minio:9000");
        Map<String, String> forClient = HiveCatalogProperties.of(m).getHmsClientProperties();
        Assertions.assertEquals("kerberos", forClient.get("hadoop.security.authentication"));
        Assertions.assertEquals("http://minio:9000", forClient.get("s3.endpoint"));
        Assertions.assertEquals(HiveTestProperties.METASTORE_URI,
                forClient.get(HiveCatalogProperties.HIVE_METASTORE_URIS));
    }

    @Test
    void missingMetastoreUriFailsNamingTheKey() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> HiveCatalogProperties.of(new java.util.LinkedHashMap<>()));
        Assertions.assertTrue(e.getMessage().contains(HiveCatalogProperties.HIVE_METASTORE_URIS),
                e.getMessage());
    }

    @Test
    void blankMetastoreUriCountsAsMissing() {
        Map<String, String> m = HiveTestProperties.minimalMap();
        m.put(HiveCatalogProperties.HIVE_METASTORE_URIS, "   ");
        Assertions.assertThrows(IllegalArgumentException.class, () -> HiveCatalogProperties.of(m));
    }

    // ===== the three rules of of() =====

    /**
     * Guards DESIGN D3(2): ALTER CATALOG merges properties — it can overwrite a key but never remove
     * one — and the same map carries engine keys and storage keys. A key refused here would leave a
     * catalog no statement could repair.
     */
    @Test
    void unknownKeysAreTolerated() {
        Map<String, String> m = HiveTestProperties.mapWith(
                "some_future_key", "x",
                "s3.endpoint", "http://minio:9000",
                "type", "hms");
        Assertions.assertDoesNotThrow(() -> HiveCatalogProperties.of(m));
    }

    /** Guards DESIGN D3(1): of() runs on every connector rebuild, so it must be pure and idempotent. */
    @Test
    void ofIsIdempotent() {
        Map<String, String> m = HiveTestProperties.mapWith(HiveCatalogProperties.HMS_CLIENT_POOL_SIZE, "3");
        HiveCatalogProperties first = HiveCatalogProperties.of(m);
        HiveCatalogProperties second = HiveCatalogProperties.of(m);
        Assertions.assertEquals(first.getMetastoreUri(), second.getMetastoreUri());
        Assertions.assertEquals(first.getHmsClientPoolSize(), second.getHmsClientPoolSize());
        Assertions.assertEquals(first.getRaw(), second.getRaw());
    }

    // ===== numeric keys: strict now, silently defaulted before =====

    /**
     * The legacy {@code HiveConnectorProperties.getInt} swallowed the NumberFormatException and used
     * the default. A typed holder refuses the value instead; ALTER CATALOG repairs such a catalog.
     */
    @Test
    void malformedPoolSizeIsRefused() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HiveTestProperties.with(HiveCatalogProperties.HMS_CLIENT_POOL_SIZE, "abc"));
    }

    @Test
    void malformedEventBatchSizeIsRefused() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HiveTestProperties.with(HiveCatalogProperties.HMS_EVENTS_BATCH_SIZE_PER_RPC, "abc"));
    }

    @Test
    void partitionBatchSizeAcceptsSurroundingWhitespace() {
        Map<String, String> m = HiveTestProperties.mapWith(
                HmsClientConfig.PARTITION_BATCH_SIZE_KEY, " 5000 ");
        HiveCatalogProperties p = HiveCatalogProperties.of(m);
        HmsClientConfig config = new HmsClientConfig(p.getHmsClientProperties(), p.getHmsClientPoolSize());
        Assertions.assertEquals(5000, config.getPartitionBatchSize());
    }

    /** Booleans never throw on either side of the migration: both spell out to {@code parseBoolean}. */
    @Test
    void malformedBooleanStaysTolerantAndReadsFalse() {
        Assertions.assertFalse(
                HiveTestProperties.with(HiveCatalogProperties.ENABLE_HMS_EVENTS_INCREMENTAL_SYNC, "yes")
                        .isHmsEventsIncrementalSyncEnabled());
    }

    // ===== the create-time-only door =====

    /**
     * A removed metastore type must be reported by name. The check runs before the required-URI rule
     * because a glue/dlf catalog carries no {@code hive.metastore.uris} — the URI message would
     * otherwise shadow the one that explains what actually happened.
     */
    @Test
    void removedMetastoreTypeIsReportedInsteadOfTheMissingUri() {
        Map<String, String> m = new java.util.LinkedHashMap<>();
        m.put(HiveCatalogProperties.METASTORE_TYPE, "glue");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> HiveCatalogProperties.of(m));
        Assertions.assertTrue(e.getMessage().contains("glue"), e.getMessage());
        Assertions.assertFalse(e.getMessage().contains("is required"), e.getMessage());
    }

    /**
     * The two meta-cache TTL knobs are checked at CREATE/ALTER only. A stored catalog that carries a
     * bad one was created before the check existed and runs today, so refusing it in of() — which runs
     * on every rebuild — would take it away from its owner with no statement able to repair it.
     */
    @Test
    void badCacheTtlPassesOfButFailsTheCreateTimeDoor() {
        Map<String, String> m = HiveTestProperties.mapWith("file.meta.cache.ttl-second", "-2");
        HiveCatalogProperties p = Assertions.assertDoesNotThrow(() -> HiveCatalogProperties.of(m));
        Assertions.assertThrows(IllegalArgumentException.class, p::checkCreateTimeOnlyRules);
    }

    @Test
    void badPartitionCacheTtlFailsTheCreateTimeDoor() {
        Map<String, String> m = HiveTestProperties.mapWith("partition.cache.ttl-second", "-2");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HiveCatalogProperties.of(m).checkCreateTimeOnlyRules());
    }

    @Test
    void validCatalogPassesBothDoors() {
        Assertions.assertDoesNotThrow(() -> HiveTestProperties.minimal().checkCreateTimeOnlyRules());
    }

    /**
     * The provider's door must stay wired to the holder: {@code validateProperties} is one statement,
     * so nothing else would fail if someone emptied it.
     */
    @Test
    void providerDoorRunsBothHalves() {
        HiveConnectorProvider provider = new HiveConnectorProvider();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(new java.util.LinkedHashMap<>()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(HiveTestProperties.mapWith("partition.cache.ttl-second", "-2")));
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(HiveTestProperties.minimalMap()));
    }

    @Test
    void rawIsAnUnmodifiableCopy() {
        Map<String, String> m = HiveTestProperties.minimalMap();
        HiveCatalogProperties p = HiveCatalogProperties.of(m);
        m.put("added.after", "1");
        Assertions.assertFalse(p.getRaw().containsKey("added.after"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> p.getRaw().put("x", "y"));
    }
}
