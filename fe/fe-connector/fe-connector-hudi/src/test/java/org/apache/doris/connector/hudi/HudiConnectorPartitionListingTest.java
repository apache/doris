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

import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;

/**
 * Tests the Hudi MVCC / listPartitions / freshness surface added so a PARTITIONED hudi-on-HMS table, served
 * post-flip through the GENERIC {@code PluginDrivenScanNode} path (like paimon), has a correct partition +
 * MVCC-snapshot surface. Each assertion pins WHY the behavior matters:
 * <ul>
 *   <li>the query-begin pin is a snapshot-id (instant) freshness, NOT a last-modified one (paimon model);</li>
 *   <li>per-partition names are HIVE-STYLE so the generic model's {@code HiveUtil.toPartitionValues} re-parse
 *       matches the partition-column arity (a raw positional name silently degrades to UNPARTITIONED);</li>
 *   <li>{@code lastModifiedMillis} is the pinned instant (a stable freshness marker), never {@code -1};</li>
 *   <li>the partition-name SOURCE follows {@code use_hive_sync_partition} (HMS vs hudi metadata);</li>
 *   <li>the dead-code MVCC/freshness seams are NOT overridden (SPI defaults hold).</li>
 * </ul>
 */
public class HudiConnectorPartitionListingTest {

    private static final List<String> YEAR_MONTH = Arrays.asList("year", "month");

    // ── item 1: instant string → long (pure) ──────────────────────────────────────────────────────────

    @Test
    public void requestedTimeParsesToLong() {
        Assertions.assertEquals(20240101120000000L,
                HudiScanPlanProvider.requestedTimeToInstant(Optional.of("20240101120000000")));
    }

    @Test
    public void emptyTimelinePinsZero() {
        // Empty timeline pins 0L (>= 0 so it survives getNewestUpdateVersionOrTime's v>=0 filter), NOT -1.
        Assertions.assertEquals(0L, HudiScanPlanProvider.requestedTimeToInstant(Optional.empty()));
    }

    // ── item 2: hive-style NAME rendering + arity round-trip ───────────────────────────────────────────

    @Test
    public void positionalPathRendersHiveStyleWithCorrectArity() {
        // Hudi's DEFAULT layout is positional "2024/01" with NO "col=" prefix. Rendering MUST inject the keys
        // so the generic re-parse yields exactly partKeys.size() values (else checkState throws -> the
        // partition is dropped -> silent UNPARTITIONED degrade).
        String name = render("2024/01", YEAR_MONTH);
        Assertions.assertEquals("year=2024/month=01", name);
        assertRoundTrips(name, YEAR_MONTH, Arrays.asList("2024", "01"));
    }

    @Test
    public void singleColumnPositionalPathRendersOneSegment() {
        // Single partition column with a bare value: size 1, not 0.
        String name = render("2024", Collections.singletonList("dt"));
        Assertions.assertEquals("dt=2024", name);
        assertRoundTrips(name, Collections.singletonList("dt"), Collections.singletonList("2024"));
    }

    @Test
    public void hiveStylePathIsRenderedIdempotently() {
        String name = render("year=2024/month=01", YEAR_MONTH);
        Assertions.assertEquals("year=2024/month=01", name);
        assertRoundTrips(name, YEAR_MONTH, Arrays.asList("2024", "01"));
    }

    @Test
    public void onDiskEscapedValueRoundTripsToRealValue() {
        // A value escaped on disk ("a%20b" = "a b") parses to the real value and the rendered name re-parses
        // back to it. (Space is not in Hive's escape set, so the rendered value carries a literal space.)
        List<String> dt = Collections.singletonList("dt");
        Assertions.assertEquals("a b",
                HudiScanPlanProvider.parsePartitionValues("dt=a%20b", dt).get("dt"));
        String name = render("dt=a%20b", dt);
        Assertions.assertEquals("dt=a b", name);
        assertRoundTrips(name, dt, Collections.singletonList("a b"));
    }

    @Test
    public void singleColumnSlashValueIsEscapedSoItRoundTrips() {
        // THE bug this guards: a single partition column whose value spans '/' (e.g. TimestampBasedKeyGenerator
        // OutputDateFormat=yyyy/MM/dd -> path "2024/01/02"). The value is the WHOLE path; without escaping the
        // '/', HiveUtil.toPartitionValues would truncate it to "2024". Escaping to "%2F" makes it round-trip.
        List<String> dt = Collections.singletonList("dt");
        Assertions.assertEquals("2024/01/02",
                HudiScanPlanProvider.parsePartitionValues("2024/01/02", dt).get("dt"));
        String name = render("2024/01/02", dt);
        Assertions.assertEquals("dt=2024%2F01%2F02", name);
        assertRoundTrips(name, dt, Collections.singletonList("2024/01/02"));
    }

    @Test
    public void distinctSlashValuedPartitionsDoNotCollide() {
        // Two distinct single-column slash paths must render distinct names AND re-parse to distinct values —
        // else the generic model collapses them onto one partition key, corrupting MTMV per-partition tracking.
        List<String> dt = Collections.singletonList("dt");
        String a = render("2024/01/02", dt);
        String b = render("2024/03/04", dt);
        Assertions.assertNotEquals(a, b);
        assertRoundTrips(a, dt, Collections.singletonList("2024/01/02"));
        assertRoundTrips(b, dt, Collections.singletonList("2024/03/04"));
    }

    // ── item 3/4: buildPartitionInfos + listPartitions/Names/Values ────────────────────────────────────

    @Test
    public void buildPartitionInfosStampsInstantAndValues() {
        List<ConnectorPartitionInfo> infos = HudiConnectorMetadata.buildPartitionInfos(
                Arrays.asList("2024/01", "2024/02"), YEAR_MONTH, 20240101120000000L);

        Assertions.assertEquals(2, infos.size());
        ConnectorPartitionInfo first = infos.get(0);
        Assertions.assertEquals("year=2024/month=01", first.getPartitionName());
        Assertions.assertEquals("2024", first.getPartitionValues().get("year"));
        Assertions.assertEquals("01", first.getPartitionValues().get("month"));
        // lastModifiedMillis == the instant (a stable non-negative marker), NOT the -1 UNKNOWN sentinel.
        Assertions.assertEquals(20240101120000000L, first.getLastModifiedMillis());
        Assertions.assertNotEquals(ConnectorPartitionInfo.UNKNOWN, first.getLastModifiedMillis());
        // row/size/file counts stay UNKNOWN (not collected on the hot path).
        Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, first.getRowCount());
        Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, first.getSizeBytes());
        Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, first.getFileCount());
    }

    @Test
    public void listPartitionNamesMatchesListPartitions() {
        HudiConnectorMetadata md = metadata(false,
                new RecordingHmsClient(null), stub(new AbstractMap.SimpleImmutableEntry<>(
                        99L, Arrays.asList("2024/01", "2024/02"))));
        ConnectorTableHandle handle = partitioned();

        List<String> names = md.listPartitionNames(null, handle);
        List<ConnectorPartitionInfo> parts = md.listPartitions(null, handle, Optional.empty());

        Assertions.assertEquals(Arrays.asList("year=2024/month=01", "year=2024/month=02"), names);
        Assertions.assertEquals(names.size(), parts.size());
        for (int i = 0; i < names.size(); i++) {
            Assertions.assertEquals(names.get(i), parts.get(i).getPartitionName());
        }
    }

    @Test
    public void listPartitionValuesProjectsRequestedColumnOrder() {
        HudiConnectorMetadata md = metadata(false,
                new RecordingHmsClient(null), stub(new AbstractMap.SimpleImmutableEntry<>(
                        99L, Collections.singletonList("2024/01"))));
        // Request in reversed order: inner list order must follow the input columns, not the storage order.
        List<List<String>> values = md.listPartitionValues(null, partitioned(), Arrays.asList("month", "year"));
        Assertions.assertEquals(Collections.singletonList(Arrays.asList("01", "2024")), values);
    }

    @Test
    public void unpartitionedTableListsNothing() {
        HudiConnectorMetadata md = metadata(false, new RecordingHmsClient(null),
                new DirectHudiMetaClientExecutor());
        ConnectorTableHandle handle = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(Collections.emptyList()).build();
        Assertions.assertTrue(md.listPartitions(null, handle, Optional.empty()).isEmpty());
        Assertions.assertTrue(md.listPartitionNames(null, handle).isEmpty());
    }

    // ── item 5: use_hive_sync_partition source selection ───────────────────────────────────────────────

    @Test
    public void hiveSyncTableListsPartitionsFromHms() {
        RecordingHmsClient hms = new RecordingHmsClient(Arrays.asList("year=2024/month=01", "year=2024/month=02"));
        // Stub returns the instant (a Long) for the timeline call; the partition NAMES must come from HMS.
        HudiConnectorMetadata md = metadata(true, hms, stub(7L));

        List<String> names = md.listPartitionNames(null, partitioned());

        Assertions.assertEquals(Arrays.asList("year=2024/month=01", "year=2024/month=02"), names);
        Assertions.assertTrue(hms.listPartitionNamesCalled, "hive-sync must list partitions from HMS");
    }

    @Test
    public void hiveSyncWithEmptyHmsFallsBackToMetaClientListing() {
        // A hive-sync table not yet synced to HMS: empty HMS must fall through to the hudi metadata listing
        // (the prune-to-zero guard), stamped with the metaClient instant — NOT return zero partitions.
        RecordingHmsClient hms = new RecordingHmsClient(Collections.emptyList());
        HudiConnectorMetadata md = metadata(true, hms,
                stub(new AbstractMap.SimpleImmutableEntry<>(5L, Collections.singletonList("2024/01"))));

        List<ConnectorPartitionInfo> parts = md.listPartitions(null, partitioned(), Optional.empty());

        Assertions.assertTrue(hms.listPartitionNamesCalled, "hive-sync must first consult HMS");
        Assertions.assertEquals(1, parts.size());
        Assertions.assertEquals("year=2024/month=01", parts.get(0).getPartitionName());
        Assertions.assertEquals(5L, parts.get(0).getLastModifiedMillis());
    }

    @Test
    public void nonHiveSyncTableNeverConsultsHms() {
        RecordingHmsClient hms = new RecordingHmsClient(null); // throws if listPartitionNames is called
        HudiConnectorMetadata md = metadata(false, hms,
                stub(new AbstractMap.SimpleImmutableEntry<>(88L, Collections.singletonList("2024/01"))));

        List<ConnectorPartitionInfo> parts = md.listPartitions(null, partitioned(), Optional.empty());

        Assertions.assertEquals(1, parts.size());
        Assertions.assertEquals("year=2024/month=01", parts.get(0).getPartitionName());
        Assertions.assertEquals(88L, parts.get(0).getLastModifiedMillis());
        Assertions.assertFalse(hms.listPartitionNamesCalled, "non-hive-sync must NOT consult HMS");
    }

    // ── item 6: beginQuerySnapshot ─────────────────────────────────────────────────────────────────────

    @Test
    public void beginQuerySnapshotPinsInstantWithoutLastModifiedFlag() {
        Optional<ConnectorMvccSnapshot> snapshot =
                HudiConnectorMetadata.buildBeginQuerySnapshot(20240101120000000L);
        Assertions.assertTrue(snapshot.isPresent());
        Assertions.assertEquals(20240101120000000L, snapshot.get().getSnapshotId());
        // The one bit that separates hudi (snapshot-id) from hive (last-modified): MUST stay false, else the
        // generic model would route hudi to the on-demand getTableFreshness probe (which hudi does not provide).
        Assertions.assertFalse(snapshot.get().isLastModifiedFreshness());
        // Time travel is a later step: schemaId stays default (-1 => latest schema).
        Assertions.assertEquals(-1L, snapshot.get().getSchemaId());
    }

    @Test
    public void beginQuerySnapshotOverrideThreadsInstantIntoPin() {
        // Drive the ACTUAL SPI override (not just the static helper): the stub executor returns the instant
        // latestInstant would read off the timeline, so the override must thread it into the pin unchanged and
        // leave lastModifiedFreshness false. Guards a mutation of the override body to empty / a wrong instant.
        HudiConnectorMetadata md = metadata(false, new RecordingHmsClient(null), stub(20240101120000000L));
        Optional<ConnectorMvccSnapshot> snapshot = md.beginQuerySnapshot(null, partitioned());
        Assertions.assertTrue(snapshot.isPresent());
        Assertions.assertEquals(20240101120000000L, snapshot.get().getSnapshotId());
        Assertions.assertFalse(snapshot.get().isLastModifiedFreshness());
    }

    // ── item 7: dead-code guard (SPI defaults hold) ────────────────────────────────────────────────────

    @Test
    public void mvccPartitionViewAndFreshnessSeamsAreNotOverridden() {
        HudiConnectorMetadata md = metadata(false, new RecordingHmsClient(null),
                new DirectHudiMetaClientExecutor());
        ConnectorTableHandle handle = partitioned();
        // A snapshot-id connector must NOT provide a range view (that would flip getPartitionSnapshot to the
        // MTMVSnapshotIdSnapshot branch) nor the last-modified freshness probes (dead code under flag=false).
        Assertions.assertFalse(md.getMvccPartitionView(null, handle).isPresent());
        Assertions.assertFalse(md.getTableFreshness(null, handle).isPresent());
        Assertions.assertEquals(OptionalLong.empty(),
                md.getPartitionFreshnessMillis(null, handle, "year=2024/month=01"));
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────────

    private static HudiTableHandle partitioned() {
        return new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(YEAR_MONTH).build();
    }

    private static HudiConnectorMetadata metadata(boolean useHiveSync, HmsClient hms,
            HudiMetaClientExecutor executor) {
        Map<String, String> props = useHiveSync
                ? Collections.singletonMap("use_hive_sync_partition", "true")
                : Collections.emptyMap();
        return new HudiConnectorMetadata(hms, props, executor);
    }

    /** Executor that ignores the action and returns a canned value (stubs out the live metaClient). */
    private static HudiMetaClientExecutor stub(Object cannedReturn) {
        return new HudiMetaClientExecutor() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> T execute(Callable<T> action) {
                return (T) cannedReturn;
            }
        };
    }

    /** Renders the hive-style name for a raw partition path the way buildPartitionInfos does (parse then render). */
    private static String render(String rawPath, List<String> partKeys) {
        return HudiScanPlanProvider.renderHiveStylePartitionName(
                partKeys, HudiScanPlanProvider.parsePartitionValues(rawPath, partKeys));
    }

    /** Asserts the rendered name re-parses (fe-core-style) to exactly the expected values, correct arity. */
    private static void assertRoundTrips(String name, List<String> partKeys, List<String> expectedValues) {
        List<String> reparsed = hiveToPartitionValues(name);
        Assertions.assertEquals(partKeys.size(), reparsed.size(),
                "re-parsed value count must equal the partition-column count (else checkState throws)");
        Assertions.assertEquals(expectedValues, reparsed);
    }

    /**
     * Local mirror of fe-core {@code HiveUtil.toPartitionValues} (cannot import fe-core from a connector test):
     * the generic model re-parses the rendered name EXACTLY this way under
     * {@code checkState(values.size() == types.size())}.
     */
    private static List<String> hiveToPartitionValues(String partitionName) {
        List<String> result = new ArrayList<>();
        int start = 0;
        while (true) {
            while (start < partitionName.length() && partitionName.charAt(start) != '=') {
                start++;
            }
            start++;
            int end = start;
            while (end < partitionName.length() && partitionName.charAt(end) != '/') {
                end++;
            }
            if (start > partitionName.length()) {
                break;
            }
            result.add(unescape(partitionName.substring(start, end)));
            start = end + 1;
        }
        return result;
    }

    private static String unescape(String path) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '%' && i + 2 < path.length()) {
                int code;
                try {
                    code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
                } catch (Exception e) {
                    code = -1;
                }
                if (code >= 0) {
                    sb.append((char) code);
                    i += 2;
                    continue;
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /** Minimal {@link HmsClient} double: records whether listPartitionNames was called; the rest fail loud. */
    private static final class RecordingHmsClient implements HmsClient {
        private final List<String> partitionNames;
        private boolean listPartitionNamesCalled;

        RecordingHmsClient(List<String> partitionNames) {
            this.partitionNames = partitionNames;
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            listPartitionNamesCalled = true;
            if (partitionNames == null) {
                throw new UnsupportedOperationException("listPartitionNames must not be called here");
            }
            return partitionNames;
        }

        @Override
        public List<String> listDatabases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listTables(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
