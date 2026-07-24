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

import org.apache.doris.connector.api.mvcc.ConnectorTableFreshness;
import org.apache.doris.connector.hms.CachingHmsClient;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Tests the C-c wiring: {@link HiveConnector} hands its {@link HiveConnectorMetadata} a caching metastore client,
 * so every scan-side read (and the MTMV freshness probes) is served from the connector-owned cache after the hms
 * cutover instead of a fresh Thrift RPC.
 *
 * <p><b>Why this matters (Rule 9):</b> when a hive catalog becomes plugin-driven at the flip, the fe-core
 * engine-side {@code HiveExternalMetaCache} stops routing to it (it collapses to the schema-only
 * {@code ENGINE_DEFAULT}). Without this connector-level wrap, {@code getTable} / {@code listPartitionNames} /
 * {@code getPartitions} would each become an uncached RPC on every scan, and the periodic SQL-dictionary / MV
 * freshness poll ({@link HiveConnectorMetadata#getTableFreshness} /
 * {@link HiveConnectorMetadata#getPartitionFreshnessMillis}, both backed by {@code getPartitions}) would hit the
 * metastore every tick. These tests pin that the connector's own {@code wrapWithCache} produces a
 * {@link CachingHmsClient}, that reads through it are cache-backed end-to-end, and that the catalog's
 * {@code meta.cache.hive.*} properties reach that cache (so it can be turned off). The decorator's internal
 * caching correctness is covered separately by {@code CachingHmsClientTest} — this suite tests only the wiring.
 *
 * <p>Live since the hms flip: production {@code createClient} wraps its client here; this exercises the wrap
 * directly.
 */
public class HiveConnectorClientCacheTest {

    private static final String METASTORE_URI = "thrift://host:9083";
    private static final List<String> PART_KEYS = Arrays.asList("year", "month");
    private static final String PART_NAME = "year=2024/month=01";
    private static final String TRANSIENT_LAST_DDL_TIME = "transient_lastDdlTime";

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        m.put("hive.metastore.uris", METASTORE_URI);
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private HiveTableHandle partitionedHandle() {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(PART_KEYS)
                .build();
    }

    // ==================== the connector wraps its client in a caching decorator ====================

    @Test
    public void wrapWithCacheReturnsACachingDecorator() {
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        RecordingHmsClient raw = new RecordingHmsClient();

        HmsClient wrapped = connector.wrapWithCache(raw);

        // WHY: production createClient hands this wrapped client to HiveConnectorMetadata. If it returned the raw
        // client (no wrap), every metadata read would be an uncached RPC after the flip.
        Assertions.assertTrue(wrapped instanceof CachingHmsClient,
                "the connector must wrap its metastore client in the caching decorator");
        Assertions.assertNotSame(raw, wrapped, "the wrapped client must not be the raw delegate");
    }

    // ==================== table freshness is served from the cache (§2.6 dictionary/MV poll stays cheap) ====

    @Test
    public void repeatedTableFreshnessHitsTheCacheNotTheMetastore() {
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        RecordingHmsClient raw = new RecordingHmsClient();
        HiveConnectorMetadata md = metadataOver(connector.wrapWithCache(raw));

        ConnectorTableFreshness first = md.getTableFreshness(null, partitionedHandle()).orElse(null);
        ConnectorTableFreshness second = md.getTableFreshness(null, partitionedHandle()).orElse(null);

        Assertions.assertNotNull(first);
        Assertions.assertNotNull(second);
        Assertions.assertEquals(300_000L, first.getTimestampMillis(),
                "freshness must still surface the real max transient_lastDdlTime (x1000)");
        // WHY: the second poll must be served from the cache — one listPartitionNames + one getPartitions RPC
        // total, not two. This is exactly what keeps a hive-backed SQL dictionary's periodic version poll cheap.
        Assertions.assertEquals(1, raw.listPartitionNamesCalls,
                "a repeated table-freshness poll must not re-list partition names");
        Assertions.assertEquals(1, raw.getPartitionsCalls,
                "a repeated table-freshness poll must not re-fetch partitions (served from the cache)");
    }

    @Test
    public void repeatedPartitionFreshnessHitsTheCache() {
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        RecordingHmsClient raw = new RecordingHmsClient();
        HiveConnectorMetadata md = metadataOver(connector.wrapWithCache(raw));

        OptionalLong first = md.getPartitionFreshnessMillis(null, partitionedHandle(), PART_NAME);
        OptionalLong second = md.getPartitionFreshnessMillis(null, partitionedHandle(), PART_NAME);

        Assertions.assertTrue(first.isPresent());
        Assertions.assertEquals(300_000L, first.getAsLong());
        Assertions.assertEquals(first.getAsLong(), second.getAsLong());
        // WHY: the same partition requested twice is one cache entry (RPC-argument granularity) — one round-trip.
        Assertions.assertEquals(1, raw.getPartitionsCalls,
                "a repeated per-partition freshness fetch for the same partition must be served from the cache");
    }

    // ==================== the catalog's meta.cache.hive.* props reach the connector-owned cache ==============

    @Test
    public void disablingThePartitionCacheViaPropsMakesFreshnessReloadEachTime() {
        // Disable ONLY the partition-object cache; leave the partition-name cache on. This proves the connector
        // threads its own catalog properties into the decorator (so an operator can turn caching off) and that the
        // knobs are read PER entry.
        HiveConnector connector =
                new HiveConnector(props("meta.cache.hive.partition.enable", "false"), new FakeConnectorContext());
        RecordingHmsClient raw = new RecordingHmsClient();
        HiveConnectorMetadata md = metadataOver(connector.wrapWithCache(raw));

        md.getTableFreshness(null, partitionedHandle());
        md.getTableFreshness(null, partitionedHandle());

        // WHY: with the partition cache disabled, getPartitions reloads every poll...
        Assertions.assertEquals(2, raw.getPartitionsCalls,
                "disabling meta.cache.hive.partition must make getPartitions reload on every freshness poll");
        // ...while the still-enabled partition-name cache is served once — proving the knob is per entry.
        Assertions.assertEquals(1, raw.listPartitionNamesCalls,
                "the still-enabled partition-name cache must not reload when only the partition cache is off");
    }

    private HiveConnectorMetadata metadataOver(HmsClient client) {
        return new HiveConnectorMetadata(client, Collections.emptyMap(), new FakeConnectorContext());
    }

    /**
     * A minimal {@link HmsClient} that counts the two freshness-backing calls and returns a single partition with
     * a {@code transient_lastDdlTime}, so a cache hit (one call) is distinguishable from a reload (two calls).
     */
    private static final class RecordingHmsClient implements HmsClient {
        int getPartitionsCalls;
        int listPartitionNamesCalls;

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            listPartitionNamesCalls++;
            return new ArrayList<>(Collections.singletonList(PART_NAME));
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            getPartitionsCalls++;
            List<HmsPartitionInfo> out = new ArrayList<>();
            for (String name : partNames) {
                List<String> values = HiveWriteUtils.toPartitionValues(name);
                Map<String, String> params = Collections.singletonMap(TRANSIENT_LAST_DDL_TIME, "300");
                out.add(new HmsPartitionInfo(values, "loc", "if", "of", "serde", params));
            }
            return out;
        }

        // Unused abstract methods — trivial stubs (never hit by the freshness path).
        @Override
        public List<String> listDatabases() {
            return Collections.emptyList();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            return null;
        }

        @Override
        public List<String> listTables(String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return false;
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            return null;
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            return Collections.emptyMap();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            return null;
        }

        @Override
        public void close() {
        }
    }
}
