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

import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.scan.ConnectorPartitionValues;

import org.apache.paimon.partition.Partition;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Partition-listing tests for {@link PaimonConnectorMetadata} (P5-T08), pinning byte-parity with
 * the legacy fe-core display-name logic ({@code PaimonUtil.generatePartitionInfo}).
 *
 * <p>Like {@link PaimonConnectorMetadataTest}, these run entirely offline against the
 * {@link RecordingPaimonCatalogOps} seam fake (null real catalog). The DATE epoch-day {@code 19723}
 * deliberately renders to {@code 2024-01-01} via {@link DateTimeUtils#formatDate(int)}; the expected
 * string is computed from the same SDK call so the assertion can never drift from the production
 * formatter.
 */
public class PaimonConnectorMetadataPartitionTest {

    private static final int DT_EPOCH_DAY = 19723; // DateTimeUtils.formatDate(19723) == 2024-01-01

    private static PaimonConnectorMetadata metadataWith(RecordingPaimonCatalogOps ops) {
        // Read-path tests ignore the context; a default RecordingConnectorContext is a no-op wrapper.
        return new PaimonConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext());
    }

    /** Two-key partitioned table: dt (DATE) + region (STRING). */
    private static RowType dtRegionRowType() {
        return RowType.builder()
                .field("id", DataTypes.INT())
                .field("dt", DataTypes.DATE())
                .field("region", DataTypes.STRING())
                .build();
    }

    private static PaimonTableHandle dtRegionHandle(FakePaimonTable table) {
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Arrays.asList("dt", "region"), Collections.emptyList());
        handle.setPaimonTable(table);
        return handle;
    }

    /** Real Paimon Partition fixture via the verified public 6-arg ctor. */
    private static Partition partition(Map<String, String> spec, long recordCount,
            long fileSizeInBytes, long lastFileCreationTime) {
        return new Partition(spec, recordCount, fileSizeInBytes, /*fileCount*/ 1, lastFileCreationTime,
                /*done*/ true);
    }

    @Test
    public void legacyNameTrueRendersDateKeyAndCarriesStats() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", dtRegionRowType(), Arrays.asList("dt", "region"), Collections.emptyList());
        table.setOptions(Collections.singletonMap("partition.legacy-name", "true"));
        ops.table = table;
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("dt", String.valueOf(DT_EPOCH_DAY));
        spec.put("region", "cn");
        ops.partitions = Collections.singletonList(partition(spec, 42L, 1024L, 1700000000000L));

        PaimonTableHandle handle = dtRegionHandle(table);

        List<String> names = metadataWith(ops).listPartitionNames(null, handle);
        List<ConnectorPartitionInfo> infos = metadataWith(ops).listPartitions(null, handle, Optional.empty());

        String expectedName = "dt=" + DateTimeUtils.formatDate(DT_EPOCH_DAY) + "/region=cn";
        // WHY: with legacy-name=true, Paimon stores DATE as an epoch-day int; the display name MUST
        // render it through the SAME DateTimeUtils.formatDate the legacy fe-core used (19723 ->
        // 2024-01-01), or the partition name diverges from every pre-migration cache/show output.
        // MUTATION: dropping the `legacyName && isDate` branch (appending the raw int "19723")
        // -> name becomes "dt=19723/region=cn" -> red.
        Assertions.assertEquals(Collections.singletonList(expectedName), names);

        Assertions.assertEquals(1, infos.size());
        ConnectorPartitionInfo info = infos.get(0);
        Assertions.assertEquals(expectedName, info.getPartitionName());
        // WHY: lastModifiedMillis must carry Partition.lastFileCreationTime() (NOT recordCount or
        // sizeBytes); the 6-arg ctor arg order is load-bearing for downstream freshness checks.
        // MUTATION: swapping the lastFileCreationTime arg for any other stat -> red.
        Assertions.assertEquals(1700000000000L, info.getLastModifiedMillis());
        // WHY: rowCount/sizeBytes carry the Paimon partition stats verbatim.
        // MUTATION: hardcoding UNKNOWN / swapping the two -> red.
        Assertions.assertEquals(42L, info.getRowCount());
        Assertions.assertEquals(1024L, info.getSizeBytes());
        // WHY: partitionValues carries the RENDERED value keyed by the raw remote name (dt -> the formatted
        // date, NOT the epoch-day int). The active partition_values() TVF feeder reads this map by remote name
        // and parses DATE via convertStringToDateV2, which throws on the raw epoch-day "19723" and fails the
        // whole query; the rendered "2024-01-01" is what it expects. MUTATION: passing the raw spec
        // (epoch-day) -> TVF-facing value diverges from the formatted date -> red.
        Assertions.assertEquals(DateTimeUtils.formatDate(DT_EPOCH_DAY), info.getPartitionValues().get("dt"));
        Assertions.assertEquals("cn", info.getPartitionValues().get("region"));
    }

    @Test
    public void partitionValueMapCarriesRenderedValuesForTvf() {
        // WHY: partition_values() reads ConnectorPartitionInfo.getPartitionValues() BY REMOTE NAME and feeds
        // it to a consumer that parses DATE via convertStringToDateV2 and maps HIVE_DEFAULT_PARTITION -> SQL
        // NULL. So the value MAP (not just orderedValues) must carry the Hive-canonical rendered form: a
        // formatted date (never the raw epoch-day, which throws and fails the whole TVF), and
        // HIVE_DEFAULT_PARTITION for a genuine null (never paimon's raw "__DEFAULT_PARTITION__", which the
        // consumer would render as a literal string instead of SQL NULL). This pins the TVF contract that
        // passing the raw spec verbatim previously violated.
        // MUTATION: passing `spec` as the ConnectorPartitionInfo value map -> dt="19723" and null
        // ="__DEFAULT_PARTITION__" -> red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", dtRegionRowType(), Arrays.asList("dt", "region"), Collections.emptyList());
        table.setOptions(Collections.singletonMap("partition.legacy-name", "true"));
        ops.table = table;
        Map<String, String> dateSpec = new LinkedHashMap<>();
        dateSpec.put("dt", String.valueOf(DT_EPOCH_DAY));
        dateSpec.put("region", "cn");
        Map<String, String> nullDateSpec = new LinkedHashMap<>();
        // A genuine-NULL DATE partition: paimon stores its default-name sentinel, handled before the DATE
        // branch so it never hits Integer.parseInt("__DEFAULT_PARTITION__").
        nullDateSpec.put("dt", "__DEFAULT_PARTITION__");
        nullDateSpec.put("region", "cn");
        ops.partitions = Arrays.asList(partition(dateSpec, 1L, 1L, 1L), partition(nullDateSpec, 1L, 1L, 1L));

        List<ConnectorPartitionInfo> infos = metadataWith(ops)
                .listPartitions(null, dtRegionHandle(table), Optional.empty());

        Assertions.assertEquals(DateTimeUtils.formatDate(DT_EPOCH_DAY),
                infos.get(0).getPartitionValues().get("dt"));
        Assertions.assertEquals(ConnectorPartitionValues.HIVE_DEFAULT_PARTITION,
                infos.get(1).getPartitionValues().get("dt"));
    }

    @Test
    public void listPartitionsCarriesFileCount() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", dtRegionRowType(), Arrays.asList("dt", "region"), Collections.emptyList());
        table.setOptions(Collections.singletonMap("partition.legacy-name", "true"));
        ops.table = table;
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("dt", String.valueOf(DT_EPOCH_DAY));
        spec.put("region", "cn");
        // Every stat is a DISTINCT value so an arg-swap mutation cannot pass by coincidence.
        // Paimon Partition ctor order: (spec, recordCount, fileSizeInBytes, fileCount,
        // lastFileCreationTime, done).
        ops.partitions = Collections.singletonList(new Partition(
                spec, /*recordCount*/ 42L, /*fileSizeInBytes*/ 1024L, /*fileCount*/ 7L,
                /*lastFileCreationTime*/ 1700000000000L, /*done*/ true));

        ConnectorPartitionInfo info = metadataWith(ops)
                .listPartitions(null, dtRegionHandle(table), Optional.empty()).get(0);

        // WHY: the SHOW PARTITIONS FileCount column (D-045) reads ConnectorPartitionInfo.getFileCount(),
        // which MUST carry Paimon Partition.fileCount() — the 7th ctor arg added for this feature.
        // MUTATION: dropping the partition.fileCount() feed (-> UNKNOWN=-1), or passing any other stat
        // (recordCount/fileSizeInBytes/lastFileCreationTime) into the fileCount slot -> red.
        Assertions.assertEquals(7L, info.getFileCount());
        Assertions.assertEquals(42L, info.getRowCount());
        Assertions.assertEquals(1024L, info.getSizeBytes());
        Assertions.assertEquals(1700000000000L, info.getLastModifiedMillis());
    }

    @Test
    public void legacyNameFalseDoesNotRenderDateKey() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", dtRegionRowType(), Arrays.asList("dt", "region"), Collections.emptyList());
        table.setOptions(Collections.singletonMap("partition.legacy-name", "false"));
        ops.table = table;
        Map<String, String> spec = new LinkedHashMap<>();
        // With legacy-name=false the remote already stores the human-readable date string.
        spec.put("dt", "2024-01-01");
        spec.put("region", "cn");
        ops.partitions = Collections.singletonList(partition(spec, 1L, 1L, 1L));

        List<String> names = metadataWith(ops).listPartitionNames(null, dtRegionHandle(table));

        // WHY: with legacy-name=false the DATE value is ALREADY a date string and must pass through
        // unchanged; re-rendering it (formatDate would parse "2024-01-01" as an int and throw, or
        // mangle the value) breaks parity. MUTATION: always taking the DATE-render branch -> red
        // (NumberFormatException on "2024-01-01").
        Assertions.assertEquals(Collections.singletonList("dt=2024-01-01/region=cn"), names);
    }

    @Test
    public void listPartitionValuesUsesRequestedColumnOrderWithRenderedValues() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", dtRegionRowType(), Arrays.asList("dt", "region"), Collections.emptyList());
        table.setOptions(Collections.singletonMap("partition.legacy-name", "true"));
        ops.table = table;
        // Paimon native spec order is dt, region; the request asks for the reversed order.
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("dt", String.valueOf(DT_EPOCH_DAY));
        spec.put("region", "cn");
        ops.partitions = Collections.singletonList(partition(spec, 1L, 1L, 1L));

        List<List<String>> values = metadataWith(ops)
                .listPartitionValues(null, dtRegionHandle(table), Arrays.asList("region", "dt"));

        // WHY: the partition_values() TVF contract requires the inner list order to match the
        // REQUESTED partitionColumns order (region, dt), NOT Paimon's native spec order (dt, region); and
        // each value is the Hive-canonical RENDERED form (dt -> the formatted date, never the raw epoch-day
        // int) so the TVF consumer can parse it (convertStringToDateV2 throws on "19723"). MUTATION:
        // iterating spec.entrySet()/keySet() instead of partitionColumns -> ["2024-01-01", "cn"] instead of
        // ["cn", "2024-01-01"] -> red; emitting the raw epoch-day "19723" instead of the rendered date -> red.
        Assertions.assertEquals(
                Collections.singletonList(Arrays.asList("cn", DateTimeUtils.formatDate(DT_EPOCH_DAY))),
                values);
    }

    /** Single STRING partition column {@code category}. */
    private static RowType categoryRowType() {
        return RowType.builder()
                .field("id", DataTypes.INT())
                .field("category", DataTypes.STRING())
                .build();
    }

    private static PaimonTableHandle categoryHandle(FakePaimonTable table) {
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.singletonList("category"), Collections.emptyList());
        handle.setPaimonTable(table);
        return handle;
    }

    @Test
    public void nullPartitionValueRendersHiveDefaultSentinel() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", categoryRowType(), Collections.singletonList("category"), Collections.emptyList());
        // No partition.default-name override -> Paimon's default sentinel.
        table.setOptions(Collections.singletonMap("partition.legacy-name", "true"));
        ops.table = table;
        Map<String, String> spec = new LinkedHashMap<>();
        // Paimon stores a genuine NULL partition value as its partition.default-name sentinel.
        spec.put("category", "__DEFAULT_PARTITION__");
        ops.partitions = Collections.singletonList(partition(spec, 1L, 729L, 1700000000000L));

        List<String> names = metadataWith(ops).listPartitionNames(null, categoryHandle(table));

        // WHY: Paimon renders a genuine NULL partition value as "__DEFAULT_PARTITION__". The display
        // name MUST normalize it to the Doris-canonical null sentinel so the FE prune bridge marks the
        // partition isNull and `category IS NULL` selects it (otherwise it is catalogued as the literal
        // string "__DEFAULT_PARTITION__" and IS NULL prunes it away -> empty result, the bug this fixes).
        // MUTATION: appending the raw spec value "__DEFAULT_PARTITION__" -> name diverges -> red.
        Assertions.assertEquals(
                Collections.singletonList("category=" + ConnectorPartitionValues.HIVE_DEFAULT_PARTITION),
                names);
    }

    @Test
    public void customDefaultPartitionNameIsHonoredAndOtherValuesUntouched() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", categoryRowType(), Collections.singletonList("category"), Collections.emptyList());
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("partition.legacy-name", "true");
        opts.put("partition.default-name", "__MY_NULL__");
        table.setOptions(opts);
        ops.table = table;
        Map<String, String> nullSpec = new LinkedHashMap<>();
        nullSpec.put("category", "__MY_NULL__");
        Map<String, String> literalSpec = new LinkedHashMap<>();
        // A literal value equal to Paimon's DEFAULT sentinel — but NOT this table's configured
        // default-name — is real data and must pass through unchanged.
        literalSpec.put("category", "__DEFAULT_PARTITION__");
        ops.partitions = Arrays.asList(
                partition(nullSpec, 1L, 1L, 1L),
                partition(literalSpec, 1L, 1L, 1L));

        List<String> names = metadataWith(ops).listPartitionNames(null, categoryHandle(table));

        // WHY: the null sentinel is read from THIS table's partition.default-name option (mirroring how
        // partition.legacy-name is read), not hardcoded. The configured "__MY_NULL__" maps to the null
        // sentinel; a literal "__DEFAULT_PARTITION__" (not this table's default) stays verbatim.
        // MUTATION: hardcoding "__DEFAULT_PARTITION__" as the sentinel -> the two rows swap which one is
        // treated as null -> red.
        Assertions.assertEquals(
                Arrays.asList(
                        "category=" + ConnectorPartitionValues.HIVE_DEFAULT_PARTITION,
                        "category=__DEFAULT_PARTITION__"),
                names);
    }

    @Test
    public void nullDatePartitionRendersSentinelInsteadOfCrashing() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", dtRegionRowType(), Arrays.asList("dt", "region"), Collections.emptyList());
        table.setOptions(Collections.singletonMap("partition.legacy-name", "true"));
        ops.table = table;
        Map<String, String> spec = new LinkedHashMap<>();
        // A genuine NULL value for a DATE partition column is ALSO the default-name sentinel, NOT an
        // epoch-day integer.
        spec.put("dt", "__DEFAULT_PARTITION__");
        spec.put("region", "cn");
        ops.partitions = Collections.singletonList(partition(spec, 1L, 1L, 1L));

        List<String> names = metadataWith(ops).listPartitionNames(null, dtRegionHandle(table));

        // WHY: the default-name check must run BEFORE the legacy DATE-render branch, or a null DATE
        // partition hits DateTimeUtils.formatDate(Integer.parseInt("__DEFAULT_PARTITION__")) and throws
        // NumberFormatException, failing the whole listPartitions call. MUTATION: ordering the DATE
        // branch first -> NumberFormatException -> red.
        Assertions.assertEquals(
                Collections.singletonList(
                        "dt=" + ConnectorPartitionValues.HIVE_DEFAULT_PARTITION + "/region=cn"),
                names);
    }

    @Test
    public void nullPartitionSuppliesNullFlagTrue() {
        // Variant B: paimon adopts genuine-NULL semantics. Its genuine-NULL partition value (rendered as the
        // Doris-canonical sentinel in the NAME) must ALSO carry isNull=true in the connector-supplied flag, so
        // the FE bridge builds a typed NullLiteral (not a StringLiteral). This realizes the connector's stated
        // intent: `category IS NULL` prunes to the null partition and an MTMV refresh materializes the null rows.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", categoryRowType(), Collections.singletonList("category"), Collections.emptyList());
        table.setOptions(Collections.singletonMap("partition.legacy-name", "true"));
        ops.table = table;
        Map<String, String> nullSpec = new LinkedHashMap<>();
        nullSpec.put("category", "__DEFAULT_PARTITION__");
        Map<String, String> literalSpec = new LinkedHashMap<>();
        literalSpec.put("category", "bj");
        ops.partitions = Arrays.asList(
                partition(nullSpec, 1L, 1L, 1L),
                partition(literalSpec, 1L, 1L, 1L));

        List<ConnectorPartitionInfo> infos =
                metadataWith(ops).listPartitions(null, categoryHandle(table), Optional.empty());

        // MUTATION: leaving paimon's flag false (pre-B parity) -> the null partition stays a StringLiteral -> red.
        Assertions.assertEquals(Collections.singletonList(true),
                infos.get(0).getPartitionValueNullFlags(), "genuine-null partition -> isNull flag true");
        Assertions.assertEquals(Collections.singletonList(false),
                infos.get(1).getPartitionValueNullFlags(), "ordinary value -> isNull flag false");
        // The name is still normalized to the sentinel (partition-name identity preserved).
        Assertions.assertEquals("category=" + ConnectorPartitionValues.HIVE_DEFAULT_PARTITION,
                infos.get(0).getPartitionName());
    }

    @Test
    public void nonPartitionedHandleReturnsEmptyWithoutSeamCall() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", RowType.builder().field("id", DataTypes.INT()).build(),
                Collections.emptyList(), Collections.emptyList());
        ops.table = table;
        // Empty partitionKeys == unpartitioned table.
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);

        PaimonConnectorMetadata metadata = metadataWith(ops);

        // WHY: legacy never lists partitions for unpartitioned tables (PaimonPartitionInfoLoader
        // returns EMPTY when partitionColumns is empty). All three SPI methods must short-circuit
        // to empty BEFORE touching the catalog seam. MUTATION: removing the empty-partitionKeys
        // guard -> a listPartitions seam call is logged -> red.
        Assertions.assertTrue(metadata.listPartitionNames(null, handle).isEmpty());
        Assertions.assertTrue(metadata.listPartitions(null, handle, Optional.empty()).isEmpty());
        Assertions.assertTrue(
                metadata.listPartitionValues(null, handle, Collections.singletonList("id")).isEmpty());
        Assertions.assertFalse(ops.log.contains("listPartitions:db1.t1"),
                "unpartitioned tables must not reach the listPartitions seam");
    }

    @Test
    public void tableNotExistDuringListYieldsEmpty() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", dtRegionRowType(), Arrays.asList("dt", "region"), Collections.emptyList());
        table.setOptions(Collections.singletonMap("partition.legacy-name", "true"));
        ops.table = table;
        ops.throwTableNotExist = true; // seam throws TableNotExistException on listPartitions

        List<String> names = metadataWith(ops).listPartitionNames(null, dtRegionHandle(table));

        // WHY: legacy getPaimonPartitions swallows TableNotExistException and returns empty rather
        // than failing the query; the connector must preserve that. MUTATION: removing the catch
        // (letting the checked exception propagate) -> the call throws instead of returning empty
        // -> red.
        Assertions.assertTrue(names.isEmpty());
        Assertions.assertTrue(ops.log.contains("listPartitions:db1.t1"),
                "the seam must have been reached (and thrown) before the empty result");
    }

    // ─────────── #65904: path-special characters in partition values ───────────
    // Legacy fe-core concatenated spec values into a Hive-style name and re-parsed it with
    // HiveUtil.toPartitionValues(); a value containing '/' or '=' parsed back wrong. Our SPI already
    // supplies values directly (fe-core never re-parses the name), so the core bug cannot occur — these
    // pin the remaining #65904 parity: partitionColumn-ordered values + SDK-escaped, collision-free names.

    /** Builds a rowType with an INT {@code id} plus the given STRING partition columns. */
    private static RowType stringPartitionedRowType(List<String> partitionCols) {
        RowType.Builder builder = RowType.builder().field("id", DataTypes.INT());
        for (String col : partitionCols) {
            builder.field(col, DataTypes.STRING());
        }
        return builder.build();
    }

    private static PaimonTableHandle stringHandle(FakePaimonTable table, List<String> partitionCols) {
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", partitionCols, Collections.emptyList());
        handle.setPaimonTable(table);
        return handle;
    }

    private static FakePaimonTable stringTable(List<String> partitionCols) {
        FakePaimonTable table = new FakePaimonTable(
                "t1", stringPartitionedRowType(partitionCols), partitionCols, Collections.emptyList());
        // legacy-name is irrelevant for STRING columns (no epoch-day render), but the collector reads it.
        table.setOptions(Collections.singletonMap("partition.legacy-name", "false"));
        return table;
    }

    @Test
    public void specialCharacterValuesAreEscapedInNameButRawInValues() {
        List<String> cols = Arrays.asList("source", "part_str", "pass");
        FakePaimonTable table = stringTable(cols);
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = table;
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("source", "dataset/team-a/segment-01");
        spec.put("part_str", "/ymd=20260701/hour=[0-9][0-9]/*.jsonl");
        spec.put("pass", "s1");
        ops.partitions = Collections.singletonList(partition(spec, 1L, 1L, 1L));

        ConnectorPartitionInfo info = metadataWith(ops)
                .listPartitions(null, stringHandle(table, cols), Optional.empty()).get(0);

        // WHY: a VALUE containing '/'/'='/'['/... must NOT be concatenated raw into the Hive-style name
        // (#65904); the name MUST be escaped by the Paimon SDK (generatePartitionPath) while the
        // connector-supplied VALUES stay RAW (fe-core consumes them directly, never re-parsing the name).
        // MUTATION: reverting to raw sb.append(value) -> name loses the %2F/%3D/%5B escapes -> red.
        String expectedName = "source=dataset%2Fteam-a%2Fsegment-01"
                + "/part_str=%2Fymd%3D20260701%2Fhour%3D%5B0-9%5D%5B0-9%5D%2F%2A.jsonl/pass=s1";
        Assertions.assertEquals(expectedName, info.getPartitionName());
        Assertions.assertEquals(
                Arrays.asList("dataset/team-a/segment-01", "/ymd=20260701/hour=[0-9][0-9]/*.jsonl", "s1"),
                info.getOrderedPartitionValues());
    }

    @Test
    public void usesPartitionColumnOrderNotSpecIterationOrder() {
        List<String> cols = Arrays.asList("source", "part_str", "pass");
        FakePaimonTable table = stringTable(cols);
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = table;
        // Spec iteration order (pass, part_str, source) DIFFERS from the partition-column order.
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("pass", "s1");
        spec.put("part_str", "/ymd=20260721");
        spec.put("source", "dataset/team-a/segment-01");
        ops.partitions = Collections.singletonList(partition(spec, 1L, 1L, 1L));

        ConnectorPartitionInfo info = metadataWith(ops)
                .listPartitions(null, stringHandle(table, cols), Optional.empty()).get(0);

        // WHY: name segments AND orderedValues MUST follow the partition-COLUMN order (source, part_str,
        // pass), never Paimon's spec map order, so value i lines up with the partition-column type i that
        // fe-core (PluginDrivenMvccExternalTable.toListPartitionItem) zips them against.
        // MUTATION: iterating spec.entrySet() -> order becomes pass/part_str/source -> red.
        Assertions.assertEquals(
                "source=dataset%2Fteam-a%2Fsegment-01/part_str=%2Fymd%3D20260721/pass=s1",
                info.getPartitionName());
        Assertions.assertEquals(
                Arrays.asList("dataset/team-a/segment-01", "/ymd=20260721", "s1"),
                info.getOrderedPartitionValues());
    }

    @Test
    public void specialCharValuesProduceCollisionFreeNames() {
        List<String> cols = Arrays.asList("a", "b");
        FakePaimonTable table = stringTable(cols);
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = table;
        Map<String, String> firstSpec = new LinkedHashMap<>();
        firstSpec.put("a", "x/b=y");
        firstSpec.put("b", "z");
        Map<String, String> secondSpec = new LinkedHashMap<>();
        secondSpec.put("a", "x");
        secondSpec.put("b", "y/b=z");
        ops.partitions = Arrays.asList(
                partition(firstSpec, 1L, 1L, 1L), partition(secondSpec, 2L, 2L, 2L));

        List<ConnectorPartitionInfo> infos = metadataWith(ops)
                .listPartitions(null, stringHandle(table, cols), Optional.empty());

        // WHY: un-escaped, {a:"x/b=y", b:"z"} and {a:"x", b:"y/b=z"} both concat to the SAME name
        // "a=x/b=y/b=z"; escaping keeps them distinct so neither partition is lost. MUTATION: raw concat
        // -> the two names collide -> the dedup guard throws (or, pre-guard, a partition silently vanishes).
        Assertions.assertEquals(2, infos.size());
        Assertions.assertEquals("a=x%2Fb%3Dy/b=z", infos.get(0).getPartitionName());
        Assertions.assertEquals("a=x/b=y%2Fb%3Dz", infos.get(1).getPartitionName());
        Assertions.assertEquals(Arrays.asList("x/b=y", "z"), infos.get(0).getOrderedPartitionValues());
        Assertions.assertEquals(Arrays.asList("x", "y/b=z"), infos.get(1).getOrderedPartitionValues());
    }

    @Test
    public void duplicatePartitionNamesFailLoud() {
        List<String> cols = Collections.singletonList("part");
        FakePaimonTable table = stringTable(cols);
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = table;
        ops.partitions = Arrays.asList(
                partition(Collections.singletonMap("part", "same"), 1L, 1L, 1L),
                partition(Collections.singletonMap("part", "same"), 2L, 2L, 2L));

        // WHY: two genuinely-duplicate remote partition specs must fail loud, not silently collapse to one
        // via a later name->item map-put. MUTATION: dropping the seenPartitionNames guard -> no throw -> red.
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> metadataWith(ops).listPartitions(null, stringHandle(table, cols), Optional.empty()));
        Assertions.assertTrue(ex.getMessage().contains("Duplicate Paimon partition name"));
    }
}
