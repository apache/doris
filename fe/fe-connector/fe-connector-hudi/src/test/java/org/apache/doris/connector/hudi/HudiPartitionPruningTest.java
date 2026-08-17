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

import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.FilterApplicationResult;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.hudi.common.model.HoodieBaseFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Tests {@link HudiConnectorMetadata#applyFilter} partition pruning (P3-T05).
 *
 * <p>WHY: the SPI Hudi path previously listed ALL partitions unconditionally and
 * stored them as {@code prunedPartitionPaths}, doing no EQ/IN pruning at all and
 * silently forcing the partition source to HMS for any filtered query. These tests
 * pin the corrected behavior, mirroring {@code HiveConnectorMetadata}:
 * <ul>
 *   <li>EQ / IN predicates on partition columns reduce the scanned partition set;</li>
 *   <li>predicates on non-partition columns (or range predicates) never prune;</li>
 *   <li>when no partition predicate applies, the handle is left untouched
 *       ({@code Optional.empty()}) so scan planning falls back to Hudi's own listing;</li>
 *   <li>Hive Sync keeps metastore locations and extractor-produced logical values separate;</li>
 *   <li>a predicate that matches every / no partition is handled correctly.</li>
 * </ul>
 * A test that passed against the old stub (which always returned all partitions)
 * would be wrong — each assertion checks the precise pruned set.</p>
 */
public class HudiPartitionPruningTest {

    private static final List<String> PARTITIONS = Arrays.asList(
            "year=2023/month=12",
            "year=2024/month=01",
            "year=2024/month=02");

    private static final List<String> PART_KEYS = Arrays.asList("year", "month");

    @Test
    public void testEqOnPartitionColumnPrunes() {
        // year = '2024' -> only the two 2024 partitions
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), eq("year", "2024"));

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Arrays.asList("year=2024/month=01", "year=2024/month=02"),
                prunedPaths(result));
    }

    @Test
    public void testInOnPartitionColumnPrunes() {
        // month IN ('01', '12') -> spans years, keeps original order
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), in("month", "01", "12"));

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Arrays.asList("year=2023/month=12", "year=2024/month=01"),
                prunedPaths(result));
    }

    @Test
    public void testAndOfTwoPartitionColumnsPrunes() {
        // year = '2024' AND month = '01' -> a single partition
        ConnectorExpression expr = and(eq("year", "2024"), eq("month", "01"));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), expr);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Collections.singletonList("year=2024/month=01"),
                prunedPaths(result));
    }

    @Test
    public void testNonPartitionColumnInAndIsIgnored() {
        // year = '2024' AND price = '100' -> prune on year only; non-partition pred ignored
        ConnectorExpression expr = and(eq("year", "2024"), eq("price", "100"));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), expr);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Arrays.asList("year=2024/month=01", "year=2024/month=02"),
                prunedPaths(result));
    }

    @Test
    public void testNonPartitionPredicateOnlyLeavesHandleUntouched() {
        // price = '100' -> no partition predicate -> Optional.empty() (no source switch)
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), eq("price", "100"));

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testPredicateMatchingAllPartitionsHasNoEffect() {
        // year IN ('2023', '2024') -> matches every partition -> Optional.empty()
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), in("year", "2023", "2024"));

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testPredicateMatchingNoPartitionYieldsEmptyPrunedList() {
        // year = '1999' -> matches nothing -> present handle with empty pruned set (scan 0)
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), eq("year", "1999"));

        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(prunedPaths(result).isEmpty());
    }

    @Test
    public void testUnpartitionedTableIsNotTouched() {
        HudiTableHandle handle = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(Collections.emptyList())
                .build();
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(handle, eq("year", "2024"));

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testNonHiveStylePositionalPathsPruneToRelativePaths() {
        // H3 core: a non-hive-style Hudi table (hive_style_partitioning=false, the DEFAULT) has a POSITIONAL
        // physical layout ("2024/01"), NOT the HMS hive-style name ("year=2024/month=01"). applyFilter must prune
        // the RELATIVE storage paths (the Hudi metadata listing that the scan also feeds fsView), so the pruned
        // set is the shape fsView is keyed by. RED before the fix: applyFilter fed HMS hive-style names to fsView,
        // which finds nothing on a non-hive-style table -> 0 splits for any filtered query.
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(PARTITIONS),                 // HMS hive-style names -- must NOT be used here
                HudiTestProperties.minimal(),                  // use_hive_sync_partition=false -> non-hive-sync
                new StubMetaClientExecutor(new AbstractMap.SimpleImmutableEntry<>(
                        false, Arrays.asList("2024/01", "2024/02", "2023/12"))));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, partitionedHandle(), new ConnectorFilterConstraint(eq("year", "2024")));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(Arrays.asList("2024/01", "2024/02"), prunedPaths(result));
    }

    @Test
    public void testHiveSyncMapsHmsMatchesToHiveStylePhysicalPaths() {
        // When the physical Hudi layout is also hive-style, the HMS location preserves that same exact path.
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(PARTITIONS),
                HudiTestProperties.with(HudiCatalogProperties.USE_HIVE_SYNC_PARTITION, "true"),
                new StubMetaClientExecutor(new AbstractMap.SimpleImmutableEntry<>(true, PARTITIONS)));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, partitionedHandle(), new ConnectorFilterConstraint(eq("year", "2024")));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Arrays.asList("s3://b/t/year=2024/month=01", "s3://b/t/year=2024/month=02"),
                prunedPartitions(result).stream()
                        .map(HmsPartitionInfo::getLocation)
                        .collect(Collectors.toList()));
    }

    @Test
    public void testHiveSyncFailsWhenMatchedPartitionMetadataIsMissing() {
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(PARTITIONS, Collections.emptyMap()),
                HudiTestProperties.with(HudiCatalogProperties.USE_HIVE_SYNC_PARTITION, "true"),
                new StubMetaClientExecutor(null));

        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.applyFilter(null, partitionedHandle(),
                        new ConnectorFilterConstraint(and(eq("year", "2024"), eq("month", "01")))));
        Assertions.assertTrue(error.getMessage().contains("returned 0 of 1 matched partitions"));
    }

    @Test
    public void testHiveSyncExtractorKeepsPhysicalPathAndLogicalValue() {
        // SinglePartPartitionValueExtractor can register physical "2024/03/15" as logical
        // event_date=2024-03-15. Matching the logical HMS value back against a positional path is invalid: the
        // slash and hyphen forms intentionally differ. The HMS location must drive FileSystemView, while its
        // ordered value must drive columns_from_path.
        List<String> hmsPartitionNames = Arrays.asList(
                "event_date=2024-03-15", "event_date=2024-03-16");
        Map<String, HmsPartitionInfo> partitionsByName = new HashMap<>();
        partitionsByName.put("event_date=2024-03-15", hmsPartition(
                Collections.singletonList("2024-03-15"), "s3://b/t/2024/03/15"));
        partitionsByName.put("event_date=2024-03-16", hmsPartition(
                Collections.singletonList("2024-03-16"), "s3://b/t/2024/03/16"));
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(hmsPartitionNames, partitionsByName),
                HudiTestProperties.with(HudiCatalogProperties.USE_HIVE_SYNC_PARTITION, "true"),
                new FailingMetaClientExecutor());
        HudiTableHandle handle = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(Collections.singletonList("event_date"))
                .build();

        Optional<FilterApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(
                null, handle, new ConnectorFilterConstraint(eq("event_date", "2024-03-15")));
        Assertions.assertTrue(result.isPresent());
        HudiTableHandle prunedHandle = (HudiTableHandle) result.get().getHandle();
        Assertions.assertEquals(1, prunedHandle.getPrunedPartitions().size());
        HudiScanPlanProvider.PartitionScanInfo partition = HudiScanPlanProvider.hmsPartitionScanInfo(
                prunedHandle.getBasePath(), prunedHandle.getPartitionKeyNames(),
                prunedHandle.getPrunedPartitions().get(0));
        Assertions.assertEquals("2024/03/15", partition.getPartitionPath());
        Assertions.assertEquals(Collections.singletonMap("event_date", "2024-03-15"),
                partition.getPartitionValues());

        List<String> lookupPaths = new ArrayList<>();
        List<ConnectorScanRange> ranges = HudiScanPlanProvider.buildCowSnapshotRanges(
                partition.getPartitionPath(), "20240101120000", partition.getPartitionValues(), null,
                path -> path, (lookupPath, instant) -> {
                    lookupPaths.add(lookupPath);
                    return "2024/03/15".equals(lookupPath)
                            ? Collections.singletonList(new HoodieBaseFile(
                                    "s3://b/t/2024/03/15/fileid-1_0_20240101000000.parquet")).stream()
                            : Collections.<HoodieBaseFile>emptyList().stream();
                });

        Assertions.assertEquals(Collections.singletonList("2024/03/15"), lookupPaths);
        Assertions.assertEquals(1, ranges.size());
        HudiScanRange range = (HudiScanRange) ranges.get(0);
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(new TTableFormatFileDesc(), rangeDesc);
        Assertions.assertEquals(Collections.singletonList("event_date"), rangeDesc.getColumnsFromPathKeys());
        Assertions.assertEquals(Collections.singletonList("2024-03-15"), rangeDesc.getColumnsFromPath());
    }

    @Test
    public void testUnprunedHiveSyncScanUsesExtractorLocationsAndValues() {
        List<String> names = Arrays.asList("event_date=2024-03-15", "event_date=2024-03-16");
        Map<String, HmsPartitionInfo> partitionsByName = new HashMap<>();
        partitionsByName.put(names.get(0), hmsPartition(
                Collections.singletonList("2024-03-15"), "s3://b/t/2024/03/15"));
        partitionsByName.put(names.get(1), hmsPartition(
                Collections.singletonList("2024-03-16"), "s3://b/t/2024/03/16"));
        FakeHmsClient hmsClient = new FakeHmsClient(names, partitionsByName);
        Map<String, String> properties = HudiTestProperties.minimalMap();
        properties.put(HudiCatalogProperties.USE_HIVE_SYNC_PARTITION, "true");
        HudiScanPlanProvider provider = new HudiScanPlanProvider(properties, null, () -> hmsClient);
        HudiTableHandle handle = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(Collections.singletonList("event_date"))
                .build();

        List<HudiScanPlanProvider.PartitionScanInfo> partitions =
                provider.resolvePartitions(handle, null, false);

        Assertions.assertEquals(Arrays.asList("2024/03/15", "2024/03/16"), partitions.stream()
                .map(HudiScanPlanProvider.PartitionScanInfo::getPartitionPath)
                .collect(Collectors.toList()));
        Assertions.assertEquals(Arrays.asList("2024-03-15", "2024-03-16"), partitions.stream()
                .map(partition -> partition.getPartitionValues().get("event_date"))
                .collect(Collectors.toList()));
    }

    @Test
    public void testUnprunedHiveSyncRejectsDuplicatePhysicalLocation() {
        List<String> names = Arrays.asList("event_date=2024-03-15", "event_date=2024-03-16");
        Map<String, HmsPartitionInfo> partitionsByName = new HashMap<>();
        partitionsByName.put(names.get(0), hmsPartition(
                Collections.singletonList("2024-03-15"), "s3://b/t/2024/03"));
        partitionsByName.put(names.get(1), hmsPartition(
                Collections.singletonList("2024-03-16"), "s3://b/t/2024/03"));
        Map<String, String> properties = HudiTestProperties.minimalMap();
        properties.put(HudiCatalogProperties.USE_HIVE_SYNC_PARTITION, "true");
        HudiScanPlanProvider provider = new HudiScanPlanProvider(
                properties, null, () -> new FakeHmsClient(names, partitionsByName));
        HudiTableHandle handle = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(Collections.singletonList("event_date"))
                .build();

        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.resolvePartitions(handle, null, false));
        Assertions.assertTrue(error.getMessage().contains(
                "Multiple Hudi Hive Sync partitions point to 2024/03"));
    }

    @Test
    public void testPrunedHiveSyncRejectsDuplicatePhysicalLocation() {
        HudiTableHandle handle = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(Collections.singletonList("event_date"))
                .prunedPartitions(Arrays.asList(
                        hmsPartition(Collections.singletonList("2024-03-15"), "s3://b/t/2024/03"),
                        hmsPartition(Collections.singletonList("2024-03-16"), "s3://b/t/2024/03")))
                .build();
        HudiScanPlanProvider provider = new HudiScanPlanProvider(
                HudiTestProperties.minimalMap(), null,
                () -> {
                    throw new AssertionError("pruned partitions must not access HMS");
                });

        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.resolvePartitions(handle, null, false));
        Assertions.assertTrue(error.getMessage().contains(
                "Multiple Hudi Hive Sync partitions point to 2024/03"));
    }

    @Test
    public void testHandleWithHiveSyncPartitionsIsSerializable() throws Exception {
        HudiTableHandle handle = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(Collections.singletonList("event_date"))
                .prunedPartitions(Collections.singletonList(hmsPartition(
                        Collections.singletonList("2024-03-15"), "s3://b/t/2024/03/15")))
                .build();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(handle);
        }

        HudiTableHandle restored;
        try (ObjectInputStream input = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (HudiTableHandle) input.readObject();
        }
        Assertions.assertEquals(handle.getPrunedPartitions(), restored.getPrunedPartitions());
    }

    @Test
    public void testHiveSyncRejectsSiblingPartitionLocation() {
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> HudiScanPlanProvider.hmsPartitionScanInfo(
                        "s3://b/table", Collections.singletonList("dt"),
                        hmsPartition(Collections.singletonList("2024-03-15"),
                                "s3://b/table_backup/dt=2024-03-15")));
        Assertions.assertTrue(error.getMessage().contains("does not belong to table base path"));
    }

    @Test
    public void testHiveSyncRejectsDifferentStorageAuthority() {
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> HudiScanPlanProvider.hmsPartitionScanInfo(
                        "s3://bucket-a/table", Collections.singletonList("dt"),
                        hmsPartition(Collections.singletonList("2024-03-15"),
                                "s3://bucket-b/table/dt=2024-03-15")));
        Assertions.assertTrue(error.getMessage().contains("does not belong to table base path"));
    }

    @Test
    public void testHiveSyncRejectsIncompatibleStorageSchemeWithSameAuthority() {
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> HudiScanPlanProvider.hmsPartitionScanInfo(
                        "s3://bucket/table", Collections.singletonList("dt"),
                        hmsPartition(Collections.singletonList("2024-03-15"),
                                "hdfs://bucket/table/dt=2024-03-15")));
        Assertions.assertTrue(error.getMessage().contains("does not belong to table base path"));
    }

    @Test
    public void testHiveSyncRejectsIncompatibleStorageSchemeWithoutAuthority() {
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> HudiScanPlanProvider.hmsPartitionScanInfo(
                        "file:///warehouse/table", Collections.singletonList("dt"),
                        hmsPartition(Collections.singletonList("2024-03-15"),
                                "hdfs:///warehouse/table/dt=2024-03-15")));
        Assertions.assertTrue(error.getMessage().contains("does not belong to table base path"));
    }

    @Test
    public void testHiveSyncAcceptsS3SchemeAliases() {
        HudiScanPlanProvider.PartitionScanInfo partition = HudiScanPlanProvider.hmsPartitionScanInfo(
                "s3a://bucket/table", Collections.singletonList("dt"),
                hmsPartition(Collections.singletonList("2024-03-15"),
                        "s3://bucket/table/dt=2024-03-15"));
        Assertions.assertEquals("dt=2024-03-15", partition.getPartitionPath());
    }

    @Test
    public void prunePartitionPathsMatchesPositionalLayout() {
        // Direct offline unit for the non-hive-sync prune helper: positional relative paths matched by the values
        // parsePartitionValues extracts positionally.
        Assertions.assertEquals(
                Arrays.asList("2024/01", "2024/02"),
                HudiConnectorMetadata.prunePartitionPaths(
                        Arrays.asList("2024/01", "2024/02", "2023/12"),
                        PART_KEYS,
                        Collections.singletonMap("year", Collections.singletonList("2024")), false));
    }

    @Test
    public void testDatePartitionPredicatePrunesUnchanged() {
        // H2 non-regression: a DATE predicate literal is a LocalDate (not LocalDateTime), so it is NOT diverted to
        // hiveDateTimeString -- String.valueOf(LocalDate) = "2024-01-01" already matches the stored DATE value.
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(PARTITIONS), HudiTestProperties.minimal(),
                new StubMetaClientExecutor(new AbstractMap.SimpleImmutableEntry<>(
                        false, Arrays.asList("2024-01-01", "2024-01-02"))));
        HudiTableHandle handle = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(Collections.singletonList("dt"))
                .build();
        ConnectorComparison dateEq = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("dt", ConnectorType.of("DATEV2")),
                new ConnectorLiteral(ConnectorType.of("DATEV2"), LocalDate.of(2024, 1, 1)));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, handle, new ConnectorFilterConstraint(dateEq));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(Collections.singletonList("2024-01-01"), prunedPaths(result));
    }

    @Test
    public void testDecimalPartitionPredicatePrunesTrailingZeros() {
        // WHY: a DECIMAL predicate literal arrives as a BigDecimal carrying the column's declared scale
        // (decimal(8,4) -> "1.0000"), while the stored Hudi partition value is Hive-canonical trailing-zero
        // trimmed ("1"). String.valueOf(BigDecimal) keeps the scale, so the prune string-compare misses and
        // every row under d=1 is silently dropped. extractLiteralValue must render "1" via
        // stripTrailingZeros().toPlainString() to string-equal the stored value. Mirrors the sibling
        // HiveConnectorMetadata fix (#65473).
        // MUTATION: dropping the `instanceof BigDecimal` branch (falling through to String.valueOf) ->
        // literal renders "1.0000", ["1.0000"].contains("1") == false -> partition dropped -> prunedPaths
        // empty -> red.
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(PARTITIONS), HudiTestProperties.minimal(),
                new StubMetaClientExecutor(new AbstractMap.SimpleImmutableEntry<>(
                        false, Arrays.asList("1", "2"))));
        HudiTableHandle handle = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(Collections.singletonList("d"))
                .build();
        ConnectorComparison decimalEq = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("d", ConnectorType.of("DECIMALV3")),
                new ConnectorLiteral(ConnectorType.of("DECIMALV3"), new BigDecimal("1.0000")));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, handle, new ConnectorFilterConstraint(decimalEq));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(Collections.singletonList("1"), prunedPaths(result));
    }

    // ========== helpers ==========

    private Optional<FilterApplicationResult<ConnectorTableHandle>> applyFilter(
            HudiTableHandle handle, ConnectorExpression expr) {
        // Default (use_hive_sync_partition=false) -> the non-hive-sync branch, whose candidate source is the Hudi
        // metadata listing. Feed the canned partition list via the stub executor (no live metaClient). The
        // hive-style names here parse the same via parsePartitionValues, so the pruning assertions are unchanged.
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(PARTITIONS), HudiTestProperties.minimal(),
                new StubMetaClientExecutor(new AbstractMap.SimpleImmutableEntry<>(true, PARTITIONS)));
        return metadata.applyFilter(null, handle, new ConnectorFilterConstraint(expr));
    }

    private HudiTableHandle partitionedHandle() {
        return new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(PART_KEYS)
                .build();
    }

    @SuppressWarnings("unchecked")
    private List<String> prunedPaths(Optional<FilterApplicationResult<ConnectorTableHandle>> result) {
        return ((HudiTableHandle) result.get().getHandle()).getPrunedPartitionPaths();
    }

    private List<HmsPartitionInfo> prunedPartitions(
            Optional<FilterApplicationResult<ConnectorTableHandle>> result) {
        return ((HudiTableHandle) result.get().getHandle()).getPrunedPartitions();
    }

    private static HmsPartitionInfo hmsPartition(List<String> values, String location) {
        return new HmsPartitionInfo(values, location, null, null, null, Collections.emptyMap());
    }

    private static ConnectorColumnRef colRef(String name) {
        return new ConnectorColumnRef(name, ConnectorType.of("STRING"));
    }

    private static ConnectorLiteral lit(String value) {
        return new ConnectorLiteral(ConnectorType.of("STRING"), value);
    }

    private static ConnectorComparison eq(String col, String value) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ, colRef(col), lit(value));
    }

    private static ConnectorIn in(String col, String... values) {
        List<ConnectorExpression> inList = new ArrayList<>();
        for (String v : values) {
            inList.add(lit(v));
        }
        return new ConnectorIn(colRef(col), inList, false);
    }

    private static ConnectorAnd and(ConnectorExpression... children) {
        return new ConnectorAnd(Arrays.asList(children));
    }

    /**
     * Minimal {@link HmsClient} double returning a fixed partition-name list.
     * Only {@code listPartitionNames} is exercised by partition pruning; the rest fail loud.
     */
    private static final class FakeHmsClient implements HmsClient {
        private final List<String> partitionNames;
        private final Map<String, HmsPartitionInfo> partitionsByName;

        FakeHmsClient(List<String> partitionNames) {
            this(partitionNames, defaultPartitions(partitionNames));
        }

        FakeHmsClient(List<String> partitionNames, Map<String, HmsPartitionInfo> partitionsByName) {
            this.partitionNames = partitionNames;
            this.partitionsByName = partitionsByName;
        }

        private static Map<String, HmsPartitionInfo> defaultPartitions(List<String> partitionNames) {
            Map<String, HmsPartitionInfo> partitions = new HashMap<>();
            for (String partitionName : partitionNames) {
                List<String> values = new ArrayList<>();
                for (String fragment : partitionName.split("/")) {
                    values.add(HudiScanPlanProvider.unescapePathName(
                            fragment.substring(fragment.indexOf('=') + 1)));
                }
                partitions.put(partitionName, hmsPartition(values, "s3://b/t/" + partitionName));
            }
            return partitions;
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
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
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName,
                List<String> partNames) {
            List<HmsPartitionInfo> result = new ArrayList<>(partNames.size());
            for (String partName : partNames) {
                HmsPartitionInfo partition = partitionsByName.get(partName);
                if (partition != null) {
                    result.add(partition);
                }
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

    /**
     * {@link HudiMetaClientExecutor} test double that returns a canned value WITHOUT running the action, so a test
     * can supply the non-hive-sync {@code applyFilter} branch's {@code listAllPartitionPaths} result offline (no
     * live metaClient / filesystem). Mirrors the stub pattern in HudiConnectorPartitionListingTest.
     */
    private static final class StubMetaClientExecutor implements HudiMetaClientExecutor {
        private final Object canned;

        StubMetaClientExecutor(Object canned) {
            this.canned = canned;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T execute(Callable<T> action) {
            return (T) canned;
        }
    }

    private static final class FailingMetaClientExecutor implements HudiMetaClientExecutor {
        @Override
        public <T> T execute(Callable<T> action) {
            throw new AssertionError("Hive Sync must not list Hudi partition paths");
        }
    }
}
