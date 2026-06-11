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
        // WHY: partitionValues must be the RAW spec (epoch-day int, NOT date-rendered) because
        // downstream indexes partitions by raw remote keys. MUTATION: storing the rendered name
        // values (e.g. "2024-01-01") -> red.
        Assertions.assertEquals(String.valueOf(DT_EPOCH_DAY), info.getPartitionValues().get("dt"));
        Assertions.assertEquals("cn", info.getPartitionValues().get("region"));
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
    public void listPartitionValuesUsesRequestedColumnOrderWithRawValues() {
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
        // REQUESTED partitionColumns order (region, dt), NOT Paimon's native spec order (dt,
        // region), and to carry RAW values (the epoch-day int for dt, never the rendered date).
        // MUTATION: iterating spec.entrySet()/keySet() instead of partitionColumns -> [19723, cn]
        // instead of [cn, 19723] -> red; rendering dt -> "2024-01-01" instead of raw -> red.
        Assertions.assertEquals(
                Collections.singletonList(Arrays.asList("cn", String.valueOf(DT_EPOCH_DAY))),
                values);
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
}
