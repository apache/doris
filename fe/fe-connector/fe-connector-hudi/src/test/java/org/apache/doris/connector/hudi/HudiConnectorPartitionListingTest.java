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

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.hms.CachingHmsClient;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

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
    public void sparkSqlTimestampPartitionsDecodeWithoutChangingStringPartitions() {
        String timestamp = "2024-01-01 10:00:00";
        // SqlKeyGenerator escapes TIMESTAMP values even when URL encoding is disabled for STRING values.
        String physical = "event_time=" + PartitionPathEncodeUtils.escapePathName(timestamp) + "/label=%3A";
        RecordingHmsClient hms = new RecordingHmsClient(null);
        hms.tableInfo = timestampTable("s3://b/t");
        HudiConnectorMetadata md = metadata(false, hms,
                stub(HudiConnectorMetadata.unpinnedListing(99, Collections.singletonList(physical), false)));
        HudiTableHandle handle = (HudiTableHandle) md.getTableHandle(null, "db", "t").get();
        ConnectorPartitionInfo partition = md.listPartitions(null, handle, Optional.empty()).get(0);
        Assertions.assertEquals(timestamp, partition.getPartitionValues().get("event_time"));
        Assertions.assertEquals("%3A", partition.getPartitionValues().get("label"));
        Assertions.assertEquals(Arrays.asList(timestamp, "%3A"), partition.getOrderedPartitionValues());
        Assertions.assertEquals(partition.getPartitionValues(),
                HudiConnectorMetadata.parsePartitionName(partition.getPartitionName(), handle.getPartitionKeyNames()));
    }

    @Test
    public void sparkSqlTimestampHmsListingAndPruningRespectPersistedUrlFlags(@TempDir Path warehouse)
            throws Exception {
        String first = "2024-01-01 10:00:00";
        String second = "2024-01-02 11:00:00";
        for (Boolean encoded : Arrays.asList(null, false, true)) {
            String basePath = warehouse.resolve(String.valueOf(encoded)).toUri().toString();
            HoodieTableMetaClient.TableBuilder builder = HoodieTableMetaClient.newTableBuilder()
                    .setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("t")
                    .setPartitionFields("event_time,label");
            if (encoded != null) {
                builder.setUrlEncodePartitioning(encoded);
            }
            builder.initTable(new HadoopStorageConfiguration(new Configuration()), basePath);
            String label = Boolean.TRUE.equals(encoded) ? "%253A" : "%3A";
            List<String> paths = Arrays.asList(
                    "event_time=" + PartitionPathEncodeUtils.escapePathName(first) + "/label=" + label,
                    "event_time=" + PartitionPathEncodeUtils.escapePathName(second) + "/label=" + label);
            List<String> names = paths.stream().map(path -> path.replace("%", "%25")).collect(Collectors.toList());
            RecordingHmsClient hms = new RecordingHmsClient(names);
            hms.tableInfo = timestampTable(basePath);
            hms.locations = Map.of(names.get(0), basePath + "/" + paths.get(0),
                    names.get(1), basePath + "/" + paths.get(1));
            HudiConnectorMetadata md = new HudiConnectorMetadata(hms,
                    HudiTestProperties.with(HudiCatalogProperties.USE_HIVE_SYNC_PARTITION, "true"),
                    HudiMetaClientExecutor.inline());
            HudiTableHandle handle = (HudiTableHandle) md.getTableHandle(null, "db", "t").get();
            List<ConnectorPartitionInfo> partitions = md.listPartitions(null, handle, Optional.empty());
            Assertions.assertEquals(Arrays.asList(first, second), partitions.stream()
                    .map(partition -> partition.getPartitionValues().get("event_time")).collect(Collectors.toList()));
            Assertions.assertEquals(Arrays.asList("%3A", "%3A"), partitions.stream()
                    .map(partition -> partition.getPartitionValues().get("label")).collect(Collectors.toList()));
            ConnectorComparison predicate = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                    new ConnectorColumnRef("event_time", ConnectorType.of("DATETIMEV2", 6, -1)),
                    new ConnectorLiteral(ConnectorType.of("DATETIMEV2", 6, -1),
                            LocalDateTime.of(2024, 1, 1, 10, 0)));
            HudiTableHandle filtered = (HudiTableHandle) md.applyFilter(null, handle,
                    new ConnectorFilterConstraint(predicate)).get().getHandle();
            Assertions.assertEquals(Collections.singletonList(paths.get(0)), filtered.getPrunedPartitionPaths());
            Assertions.assertEquals(Collections.singleton("event_time"), filtered.getTimestampPartitionKeys());
        }
    }

    @Test
    public void sparkSqlTimestampMetadataPruningAndHistoricalListingKeepPhysicalPaths() {
        List<String> paths = Arrays.asList("event_time=2024-01-01 10%3A00%3A00/label=%3A",
                "event_time=2024-01-02 11%3A00%3A00/label=%3A");
        RecordingHmsClient hms = new RecordingHmsClient(null);
        hms.tableInfo = timestampTable("s3://b/t");
        HudiConnectorMetadata current = metadata(false, hms,
                stub(HudiConnectorMetadata.unpinnedListing(99, paths, false)));
        HudiTableHandle handle = (HudiTableHandle) current.getTableHandle(null, "db", "t").get();
        ConnectorComparison predicate = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("event_time", ConnectorType.of("DATETIMEV2", 6, -1)),
                new ConnectorLiteral(ConnectorType.of("DATETIMEV2", 6, -1), LocalDateTime.of(2024, 1, 1, 10, 0)));
        HudiTableHandle filtered = (HudiTableHandle) current.applyFilter(null, handle,
                new ConnectorFilterConstraint(predicate)).get().getHandle();
        Assertions.assertEquals(Collections.singletonList(paths.get(0)), filtered.getPrunedPartitionPaths());
        HudiConnectorMetadata historical = metadata(false, hms,
                stub(HudiConnectorMetadata.pinnedListing(99, paths, 7,
                        Collections.singletonList(paths.get(0)), false)));
        List<ConnectorPartitionInfo> partitions = historical.listPartitions(null,
                handle.toBuilder().queryInstant("20240101120000000").build(), Optional.empty());
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals(Arrays.asList("2024-01-01 10:00:00", "%3A"),
                partitions.get(0).getOrderedPartitionValues());
    }

    private static HmsTableInfo timestampTable(String basePath) {
        return HmsTableInfo.builder().dbName("db").tableName("t").location(basePath)
                .partitionKeys(Arrays.asList(
                        new ConnectorColumn("event_time", ConnectorType.of("DATETIMEV2", 6, -1), null, true, null),
                        new ConnectorColumn("label", ConnectorType.of("STRING"), null, true, null)))
                .build();
    }

    @Test
    public void metadataListingAndTimeTravelKeepLiteralPercentPartitionsDistinct() {
        for (boolean encoded : Arrays.asList(false, true)) {
            List<String> paths = encoded ? Arrays.asList("dt=%3A", "dt=%253A")
                    : Arrays.asList("dt=:", "dt=%3A");
            HudiTableHandle handle = partitioned().toBuilder()
                    .partitionKeyNames(Collections.singletonList("dt")).build();
            HudiConnectorMetadata current = metadata(false, new RecordingHmsClient(null),
                    stub(HudiConnectorMetadata.unpinnedListing(99, paths, encoded)));
            Assertions.assertEquals(Arrays.asList("dt=%3A", "dt=%253A"),
                    current.listPartitionNames(null, handle));
            HudiConnectorMetadata historical = metadata(false, new RecordingHmsClient(null),
                    stub(HudiConnectorMetadata.pinnedListing(99, paths, 7,
                            Collections.singletonList(paths.get(1)), encoded)));
            List<ConnectorPartitionInfo> parts = historical.listPartitions(null, handle, Optional.empty());
            Assertions.assertEquals(1, parts.size());
            Assertions.assertEquals("%3A", parts.get(0).getPartitionValues().get("dt"));
            Assertions.assertEquals("dt=%253A", parts.get(0).getPartitionName());
            Assertions.assertEquals(7, parts.get(0).getLastModifiedMillis());
        }
    }

    @Test
    public void physicalPathsHonorPersistedEncodingConfiguration(@TempDir Path warehouse) throws Exception {
        for (Boolean encoded : Arrays.asList(null, false, true)) {
            String basePath = warehouse.resolve(String.valueOf(encoded)).toUri().toString();
            HoodieTableMetaClient.TableBuilder builder = HoodieTableMetaClient.newTableBuilder()
                    .setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("t").setPartitionFields("dt");
            if (encoded != null) {
                builder.setUrlEncodePartitioning(encoded);
            }
            builder.initTable(new HadoopStorageConfiguration(new Configuration()), basePath);
            boolean decode = Boolean.TRUE.equals(encoded);
            List<String> physical = decode ? Arrays.asList("dt=%3A", "dt=%253A")
                    : Arrays.asList("dt=:", "dt=%3A");
            List<String> names = decode ? Arrays.asList("dt=%253A", "dt=%25253A")
                    : Arrays.asList("dt=%3A", "dt=%253A");
            RecordingHmsClient hms = new RecordingHmsClient(names);
            hms.locations = Map.of(names.get(0), basePath + "/" + physical.get(0),
                    names.get(1), basePath + "/" + physical.get(1));
            HudiConnectorMetadata md = new HudiConnectorMetadata(hms,
                    HudiTestProperties.with(HudiCatalogProperties.USE_HIVE_SYNC_PARTITION, "true"),
                    HudiMetaClientExecutor.inline());
            HudiTableHandle handle = new HudiTableHandle.Builder("db", "t", basePath, "COPY_ON_WRITE")
                    .partitionKeyNames(Collections.singletonList("dt")).build();
            Assertions.assertEquals(Arrays.asList(":", "%3A"), md.listPartitions(null, handle, Optional.empty())
                    .stream().map(p -> p.getPartitionValues().get("dt")).collect(Collectors.toList()));
            Assertions.assertEquals(Arrays.asList("dt=%3A", "dt=%253A"), md.listPartitionNames(null, handle));
            ConnectorComparison predicate = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                    new ConnectorColumnRef("dt", ConnectorType.of("STRING")),
                    new ConnectorLiteral(ConnectorType.of("STRING"), "%3A"));
            HudiTableHandle filtered = (HudiTableHandle) md.applyFilter(null, handle,
                    new ConnectorFilterConstraint(predicate)).get().getHandle();
            Assertions.assertEquals(Collections.singletonList(physical.get(1)), filtered.getPrunedPartitionPaths());
        }
    }

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

    // ── item 1b: instant → epoch millis (the unit the partition info contract requires) ────────────────

    @Test
    public void instantConvertsToEpochMillis() {
        // 20240101120000000 read as a hudi instant is 2024-01-01T12:00:00.000; in UTC that is this
        // epoch-millis value. The two representations of the same moment differ by four orders of
        // magnitude (2.0e16 vs 1.7e12), which is exactly why swapping them goes unnoticed.
        Assertions.assertEquals(1704110400000L,
                HudiScanPlanProvider.instantToEpochMillis(20240101120000000L, ZoneOffset.UTC));
        Assertions.assertTrue(
                HudiScanPlanProvider.instantToEpochMillis(20240101120000000L, ZoneOffset.UTC) < 1e13,
                "an epoch-millis value must be ~1e12, not the instant's ~1e16");
    }

    @Test
    public void legacySecondPrecisionInstantConverts() {
        // 14-digit (second-precision) instants predate the millis format. Hudi back-fills them with
        // ".999" (verified against hudi-common 1.0.2's fixInstantTimeCompatibility / DEFAULT_MILLIS_EXT),
        // which is also what keeps the mapping order-preserving.
        Assertions.assertEquals(1704110400999L,
                HudiScanPlanProvider.instantToEpochMillis(20240101120000L, ZoneOffset.UTC));
    }

    @Test
    public void emptyTimelineConvertsToZero() {
        // 0 = "no completed instant". Stays 0 (>= 0) so the version-token filter still accepts it.
        Assertions.assertEquals(0L, HudiScanPlanProvider.instantToEpochMillis(0L, ZoneOffset.UTC));
    }

    @Test
    public void unparseableInstantDegradesToZeroWithoutThrowing() {
        // This value only feeds cache/freshness heuristics, and it is computed on the partition-listing
        // hot path. Failing a whole table's queries over a statistics field is the wrong trade; 0 means
        // "no reliable change signal", which the engine already handles.
        Assertions.assertEquals(0L, HudiScanPlanProvider.instantToEpochMillis(5L, ZoneOffset.UTC));
    }

    @Test
    public void convertedValueReadsAsHowLongTheTableHasBeenQuiet() {
        // THE point of this whole conversion, stated as an assertion.
        //
        // The engine gates the SQL cache on `now - lastModifiedMillis >= quiet window`, having first
        // clamped `now` to at least lastModifiedMillis (a guard against FE/metadata clock skew). Feed it
        // a raw instant (~2.0e16) and that clamp drags "now" along with it, so the difference is 0
        // FOREVER - not "0 until the window passes". Result: partitioned hudi tables never serve from the
        // SQL cache, and because the gate takes the maximum across every table a query touches, ONE hudi
        // table also blocks caching for anything joined to it.
        //
        // MUTATION: pass the raw instant through instead of converting -> this difference becomes a huge
        // negative number -> red.
        ZoneId zone = ZoneId.systemDefault();
        LocalDateTime anHourAgo = LocalDateTime.now(zone).minusHours(1);
        long instant = Long.parseLong(anHourAgo.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")));

        long quietForMillis = System.currentTimeMillis()
                - HudiScanPlanProvider.instantToEpochMillis(instant, zone);

        Assertions.assertTrue(Math.abs(quietForMillis - Duration.ofHours(1).toMillis())
                        < Duration.ofMinutes(5).toMillis(),
                "wall clock minus this field must read as 'the table has been quiet ~1 hour', got "
                        + quietForMillis + " ms");
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
                HudiScanPlanProvider.parsePartitionValues("dt=a%20b", dt, true).get("dt"));
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
                HudiScanPlanProvider.parsePartitionValues("2024/01/02", dt, true).get("dt"));
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
    public void buildPartitionInfosStampsLastModifiedAndValues() {
        // The caller converts the instant to epoch millis; this method only passes it through.
        List<ConnectorPartitionInfo> infos = HudiConnectorMetadata.buildPartitionInfos(
                Arrays.asList("2024/01", "2024/02"), YEAR_MONTH, 1704110400000L, true);

        Assertions.assertEquals(2, infos.size());
        ConnectorPartitionInfo first = infos.get(0);
        Assertions.assertEquals("year=2024/month=01", first.getPartitionName());
        Assertions.assertEquals("2024", first.getPartitionValues().get("year"));
        Assertions.assertEquals("01", first.getPartitionValues().get("month"));
        // lastModifiedMillis is carried through verbatim (a stable non-negative marker), NOT the -1
        // UNKNOWN sentinel.
        Assertions.assertEquals(1704110400000L, first.getLastModifiedMillis());
        Assertions.assertNotEquals(ConnectorPartitionInfo.UNKNOWN, first.getLastModifiedMillis());
        // row/size/file counts stay UNKNOWN (not collected on the hot path).
        Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, first.getRowCount());
        Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, first.getSizeBytes());
        Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, first.getFileCount());
    }

    /**
     * The three spellings a null hudi partition arrives in, all of which must reach the engine as a NULL flag
     * rather than as a literal. It decides how the engine prunes - {@code col IS NULL} against
     * {@code col = '<sentinel>'} - while the scan side decides what those predicates match, and the two must
     * agree. The scan half of this rule has a test; the listing half is what this change touched, and did not.
     */
    @Test
    public void nullPartitionValuesAreFlaggedInEverySpellingHudiUses() {
        List<String> singleKey = Collections.singletonList("dt");
        List<ConnectorPartitionInfo> infos = HudiConnectorMetadata.buildPartitionInfos(
                Arrays.asList("dt=__HIVE_DEFAULT_PARTITION__", "dt=\\N", "dt=2024-01-01"),
                singleKey, 1704110400000L, true);

        Assertions.assertEquals(Collections.singletonList(true), infos.get(0).getPartitionValueNullFlags(),
                "the hive-canonical sentinel is a SQL NULL");
        Assertions.assertEquals(Collections.singletonList(true), infos.get(1).getPartitionValueNullFlags(),
                "hudi's older \\N spelling is the same SQL NULL - a table written that way used to be listed"
                        + " as the literal and scanned as NULL, so `col IS NULL` returned no rows");
        Assertions.assertEquals(Collections.singletonList(false), infos.get(2).getPartitionValueNullFlags(),
                "an ordinary value must not be flagged");
        // The value itself is carried through untouched either way: fe-core reads the flag, not the string.
        Assertions.assertEquals("2024-01-01", infos.get(2).getOrderedPartitionValues().get(0));
    }

    /**
     * The pinned listing, driven end to end. {@code restrictToPin} decides which partitions a
     * {@code FOR TIME AS OF} query may prune to, and until this test it could not be reached from a test at
     * all: the only seam was the unpinned one, so half the listing path was covered and the half this change
     * added was not.
     */
    @Test
    public void pinnedListingKeepsOnlyThePartitionsThatHeldDataAtThePin() {
        HudiConnectorMetadata md = metadata(false, new RecordingHmsClient(null),
                stub(HudiConnectorMetadata.pinnedListing(99L,
                        Arrays.asList("2024/01", "2024/02", "2024/03"),
                        1704110400000L, Arrays.asList("2024/01", "2024/03"), true)));

        List<ConnectorPartitionInfo> parts = md.listPartitions(null, partitioned(), Optional.empty());

        Assertions.assertEquals(Arrays.asList("year=2024/month=01", "year=2024/month=03"),
                parts.stream().map(ConnectorPartitionInfo::getPartitionName).collect(Collectors.toList()),
                "a partition that held no data at the pin must not be listed");
        for (ConnectorPartitionInfo part : parts) {
            // The pin, not "now": at a pin the partitions are frozen, and reporting the current instant would
            // tell the engine a past snapshot keeps changing.
            Assertions.assertEquals(1704110400000L, part.getLastModifiedMillis());
        }
    }

    /**
     * ...and it restricts by VALUE, over the listing's own strings. Both sides come from one
     * {@code listAllPartitionPaths}, which is what makes the comparison exact - the names and values handed
     * out stay the ones the unpinned listing produced.
     */
    @Test
    public void pinnedListingReportsTheUnpinnedListingsOwnNamesAndValues() {
        HudiConnectorMetadata md = metadata(false, new RecordingHmsClient(null),
                stub(HudiConnectorMetadata.pinnedListing(99L,
                        Collections.singletonList("2024/01"),
                        7L, Collections.singletonList("2024/01"), true)));

        ConnectorPartitionInfo only = md.listPartitions(null, partitioned(), Optional.empty()).get(0);

        Assertions.assertEquals("year=2024/month=01", only.getPartitionName());
        Assertions.assertEquals("2024", only.getPartitionValues().get("year"));
        Assertions.assertEquals("01", only.getPartitionValues().get("month"));
        Assertions.assertEquals(Arrays.asList(false, false), only.getPartitionValueNullFlags());
    }

    @Test
    public void listPartitionNamesMatchesListPartitions() {
        HudiConnectorMetadata md = metadata(false,
                new RecordingHmsClient(null),
                stub(HudiConnectorMetadata.unpinnedListing(99L, Arrays.asList("2024/01", "2024/02"), true)));
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
    public void hiveSyncListingUsesStorageLocationForEncodedValues() {
        List<String> names = Arrays.asList("dt=2023-12-01 08%253A00%253A00", "dt=%25253A", "dt=a%2Fb");
        RecordingHmsClient hms = new RecordingHmsClient(names);
        hms.locations = Map.of(names.get(0), "s3://b/t/dt=2023-12-01 08%3A00%3A00",
                names.get(1), "s3://b/t/dt=%253A", names.get(2), "s3://b/t/dt=a%2Fb");
        HudiConnectorMetadata md = metadata(true, hms, stub(new HudiConnectorMetadata.PartitionState(7L, true)));
        HudiTableHandle handle = partitioned().toBuilder()
                .partitionKeyNames(Collections.singletonList("dt")).build();
        List<ConnectorPartitionInfo> parts = md.listPartitions(null, handle, Optional.empty());
        Assertions.assertEquals(Arrays.asList("2023-12-01 08:00:00", "%3A", "a/b"),
                parts.stream().map(p -> p.getPartitionValues().get("dt")).collect(Collectors.toList()));
        Assertions.assertEquals(Arrays.asList("dt=2023-12-01 08%3A00%3A00", "dt=%253A", "dt=a%2Fb"),
                parts.stream().map(ConnectorPartitionInfo::getPartitionName).collect(Collectors.toList()));
    }

    @Test
    public void cachedNamesOmitDeletedEncodedPartitionsBeforePruning() {
        List<String> names = Arrays.asList("dt=gone%253Afirst", "dt=08%253A00",
                "dt=gone%253Amiddle", "dt=%25253A", "dt=plain");
        RecordingHmsClient delegate = new RecordingHmsClient(names);
        delegate.locations = Map.of(names.get(1), "s3://b/t/dt=08%3A00",
                names.get(3), "s3://b/t/dt=%253A");
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            CachingHmsClient cache = new CachingHmsClient(owner, delegate,
                    Map.of("meta.cache.hive.partition.enable", "false"));
            Assertions.assertEquals(names, cache.listPartitionNames("db", "t", -1));
            // Delete after caching the names: object loads must not make unrelated partitions unreadable.
            delegate.deletedNames = Arrays.asList(names.get(0), names.get(2));
            HudiConnectorMetadata md = metadata(true, cache, stub(new HudiConnectorMetadata.PartitionState(7L, true)));
            HudiTableHandle handle = partitioned().toBuilder()
                    .partitionKeyNames(Collections.singletonList("dt")).build();
            Assertions.assertEquals(Arrays.asList("08:00", "%3A", "plain"),
                    md.listPartitions(null, handle, Optional.empty()).stream()
                            .map(p -> p.getPartitionValues().get("dt")).collect(Collectors.toList()));
            ConnectorComparison predicate = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                    new ConnectorColumnRef("dt", ConnectorType.of("STRING")),
                    new ConnectorLiteral(ConnectorType.of("STRING"), "%3A"));
            HudiTableHandle filtered = (HudiTableHandle) md.applyFilter(null, handle,
                    new ConnectorFilterConstraint(predicate)).get().getHandle();
            Assertions.assertEquals(Collections.singletonList("dt=%253A"), filtered.getPrunedPartitionPaths());
            Assertions.assertEquals(names, cache.listPartitionNames("db", "t", -1));
        }
    }

    @Test
    public void encodedPartitionKeysDoNotChangePartitionIdentity() {
        RecordingHmsClient hms = new RecordingHmsClient(Collections.singletonList("d%2Ft=a%253Ab"));
        hms.locations = Collections.singletonMap("d%2Ft=a%253Ab", "s3://b/t/d/t=a%3Ab");
        HudiConnectorMetadata md = metadata(true, hms, stub(new HudiConnectorMetadata.PartitionState(7L, true)));
        HudiTableHandle handle = partitioned().toBuilder()
                .partitionKeyNames(Collections.singletonList("d/t")).build();
        List<ConnectorPartitionInfo> parts = md.listPartitions(null, handle, Optional.empty());
        Assertions.assertEquals(1, parts.size());
        Assertions.assertEquals("a:b", parts.get(0).getPartitionValues().get("d/t"));
    }

    @Test
    public void encodedPartitionWithoutValidStorageLocationFailsInsteadOfLosingRows() {
        for (String location : Arrays.asList(null, "", "s3://b/other/dt=a%3Ab",
                "s3://b/table/dt=a%3Ab", "s3://other/t/dt=a%3Ab", "s3a://other/t/dt=a%3Ab",
                "s3a://b/table/dt=a%3Ab", "s3a://b/t/../other/dt=a%3Ab", "s3a://b/t",
                "s3a://b/t/", "hdfs://b/t/dt=a%3Ab", "file:///t/dt=a%3Ab")) {
            RecordingHmsClient hms = new RecordingHmsClient(Collections.singletonList("dt=a%253Ab"));
            hms.locations = Collections.singletonMap("dt=a%253Ab", location);
            HudiConnectorMetadata md = metadata(true, hms, stub(new HudiConnectorMetadata.PartitionState(7L, true)));
            HudiTableHandle handle = partitioned().toBuilder()
                    .partitionKeyNames(Collections.singletonList("dt")).build();
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> md.listPartitions(null, handle, Optional.empty()), location);
        }
    }

    @Test
    public void hiveSyncListingAcceptsEquivalentS3SchemesWithoutDecodingPaths() {
        List<String> names = Arrays.asList("dt=a%253Ab", "dt=%25253A", "dt=a%2Fb", "dt=plain");
        for (String baseScheme : Arrays.asList("s3", "s3a")) {
            for (String partitionScheme : Arrays.asList("s3", "s3a")) {
                for (String path : Arrays.asList("/t", "/t/", "/a+b%20c/t", "")) {
                    String prefix = path.endsWith("/") ? path : path + "/";
                    RecordingHmsClient hms = new RecordingHmsClient(names);
                    hms.locations = Map.of(names.get(0), partitionScheme + "://b" + prefix + "dt=a%3Ab",
                            names.get(1), partitionScheme + "://b" + prefix + "dt=%253A",
                            names.get(2), partitionScheme + "://b" + prefix + "dt=a%2Fb");
                    HudiConnectorMetadata md = metadata(true, hms,
                            stub(new HudiConnectorMetadata.PartitionState(7L, true)));
                    HudiTableHandle handle = new HudiTableHandle.Builder("db", "t",
                            baseScheme + "://b" + path, "COPY_ON_WRITE")
                            .partitionKeyNames(Collections.singletonList("dt")).build();
                    List<ConnectorPartitionInfo> parts = md.listPartitions(null, handle, Optional.empty());
                    Assertions.assertEquals(Arrays.asList("a:b", "%3A", "a/b", "plain"), parts.stream()
                            .map(p -> p.getPartitionValues().get("dt")).collect(Collectors.toList()));
                    Assertions.assertEquals(Arrays.asList("dt=a%3Ab", "dt=%253A", "dt=a%2Fb", "dt=plain"),
                            md.listPartitionNames(null, handle));
                }
            }
        }
    }

    @Test
    public void hiveSyncTableListsPartitionsFromHms() {
        RecordingHmsClient hms = new RecordingHmsClient(Arrays.asList("year=2024/month=01", "year=2024/month=02"));
        // Stub supplies timeline/config state; the partition names must still come from HMS.
        HudiConnectorMetadata md = metadata(true, hms, stub(new HudiConnectorMetadata.PartitionState(7L, true)));

        List<String> names = md.listPartitionNames(null, partitioned());

        Assertions.assertEquals(Arrays.asList("year=2024/month=01", "year=2024/month=02"), names);
        Assertions.assertTrue(hms.listPartitionNamesCalled, "hive-sync must list partitions from HMS");
        Assertions.assertEquals(0, hms.getPartitionsCalls, "plain partition names need no extra RPC");
    }

    @Test
    public void hiveSyncWithEmptyHmsFallsBackToMetaClientListing() {
        // A hive-sync table not yet synced to HMS: empty HMS must fall through to the hudi metadata listing
        // (the prune-to-zero guard), stamped with the metaClient instant — NOT return zero partitions.
        RecordingHmsClient hms = new RecordingHmsClient(Collections.emptyList());
        HudiConnectorMetadata md = metadata(true, hms,
                stub(HudiConnectorMetadata.unpinnedListing(5L, Collections.singletonList("2024/01"), true)));

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
                stub(HudiConnectorMetadata.unpinnedListing(88L, Collections.singletonList("2024/01"), true)));

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

    // ── the snapshot-exact-listing capability ─────────────────────────────────────────────────────────

    /**
     * The capability has to be answered per table, because only one of the two listing sources can keep the
     * promise. Claiming it for the hive-sync branch makes fe-core prune a {@code FOR TIME AS OF} query against
     * a partition set that is a SUBSET of the pin - {@code collectPartitions} starts from what HMS holds now
     * and can only remove - so a partition dropped and unsynced after the pin is pruned away and the query
     * silently returns fewer rows, with a perfectly normal EXPLAIN.
     */
    @Test
    public void snapshotExactListingIsClaimedOnlyWhereItHolds() {
        HmsClient hms = new RecordingHmsClient(null);
        HudiMetaClientExecutor executor = new DirectHudiMetaClientExecutor();
        ConnectorTableHandle handle = partitioned();

        Assertions.assertTrue(metadata(false, hms, executor).listsPartitionsAtSnapshot(null, handle),
                "the metadata-table listing enumerates exactly the partitions holding data at the pin");
        Assertions.assertFalse(metadata(true, hms, executor).listsPartitionsAtSnapshot(null, handle),
                "the hive-sync listing starts from HMS-now, so it must not claim to be snapshot-exact");
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
        HudiCatalogProperties props = useHiveSync
                ? HudiTestProperties.with(HudiCatalogProperties.USE_HIVE_SYNC_PARTITION, "true")
                : HudiTestProperties.minimal();
        return new HudiConnectorMetadata(hms, props, executor) {
            @Override
            boolean urlEncodePartitioning(HudiTableHandle handle, List<String> paths) {
                return true;
            }
        };
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
                partKeys, HudiScanPlanProvider.parsePartitionValues(rawPath, partKeys, true));
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

    /** Records partition lookups and supplies locations; unrelated HMS operations fail loudly. */
    private static final class RecordingHmsClient implements HmsClient {
        private final List<String> partitionNames;
        private HmsTableInfo tableInfo;
        private Map<String, String> locations = Collections.emptyMap();
        private List<String> deletedNames = Collections.emptyList();
        private int getPartitionsCalls;
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
            return partitionNames.stream().filter(name -> !deletedNames.contains(name)).collect(Collectors.toList());
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
            return tableInfo != null;
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            if (tableInfo == null) {
                throw new UnsupportedOperationException("No table metadata configured");
            }
            return tableInfo;
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            if (partNames.stream().anyMatch(deletedNames::contains)) {
                throw new HmsClientException("Partition no longer exists");
            }
            return getExistingPartitions(dbName, tableName, partNames);
        }

        @Override
        public List<HmsPartitionInfo> getExistingPartitions(String dbName, String tableName, List<String> partNames) {
            getPartitionsCalls++;
            List<HmsPartitionInfo> result = new ArrayList<>();
            for (String name : partNames) {
                if (deletedNames.contains(name)) {
                    continue;
                }
                result.add(new HmsPartitionInfo(hiveToPartitionValues(name),
                        locations.getOrDefault(name, "s3://b/t/" + name), null, null, null, null));
            }
            return result;
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
