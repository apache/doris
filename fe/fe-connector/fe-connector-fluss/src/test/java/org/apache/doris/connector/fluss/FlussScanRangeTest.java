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

package org.apache.doris.connector.fluss;

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The range is the FE half of the wire contract with the be-java-extensions scanner: an untyped string
 * map that nothing between here and there type-checks. So these assert the map WHOLE — every key, its
 * exact rendering, and the absence of keys that do not belong to the range type — rather than spot
 * checking. A key that is quietly renamed, dropped or leaked across range types is exactly the failure
 * this file exists to catch.
 */
public class FlussScanRangeTest {

    private static final FlussScanRange.Partition DT_20260101 =
            FlussScanRange.Partition.of(77L, "dt=20260101",
                    Collections.singletonMap("dt", "20260101"));

    @Test
    public void logRangeCarriesOffsetsAndNothingElse() {
        FlussScanRange range = FlussScanRange.log(FlussScanRange.Partition.NONE, 3, 10L, 42L);

        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("fluss.range_type", "LOG");
        expected.put("fluss.bucket_id", "3");
        expected.put("fluss.log_start_offset", "10");
        expected.put("fluss.log_stop_offset", "42");
        Assertions.assertEquals(expected, range.getProperties());
        Assertions.assertEquals(FlussScanRange.RangeType.LOG, range.getRangeType());
    }

    @Test
    public void pkFullRangeCarriesTheKvSnapshotButNoLakeFields() {
        FlussScanRange range =
                FlussScanRange.pkFull(FlussScanRange.Partition.NONE, 0, 900L, 120L, 500L);

        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("fluss.range_type", "PK_FULL");
        expected.put("fluss.bucket_id", "0");
        expected.put("fluss.log_start_offset", "120");
        expected.put("fluss.log_stop_offset", "500");
        expected.put("fluss.kv_snapshot_id", "900");
        Assertions.assertEquals(expected, range.getProperties());
    }

    /**
     * A bucket that has never been snapshotted still produces a range: the scanner replays its whole
     * state from the log. The sentinel has to survive as {@code -1}, because "key missing" and
     * "no snapshot" would otherwise be indistinguishable on the scanner side.
     */
    @Test
    public void pkFullWithoutASnapshotSendsTheSentinel() {
        FlussScanRange range = FlussScanRange.pkFull(FlussScanRange.Partition.NONE, 0,
                FlussScanRange.NO_KV_SNAPSHOT, LogScanner.EARLIEST_OFFSET, 500L);

        Assertions.assertEquals("-1", range.getProperties().get("fluss.kv_snapshot_id"));
        Assertions.assertEquals("-2", range.getProperties().get("fluss.log_start_offset"));
    }

    /**
     * EARLIEST is fluss's own sentinel, not a Doris one: the scanner hands the value straight back to
     * fluss. If fluss ever renumbers it, FE and the scanner move together — but the rendering must stay
     * the raw number, never a name the scanner would have to translate.
     */
    @Test
    public void earliestOffsetGoesOutVerbatim() {
        FlussScanRange range = FlussScanRange.log(FlussScanRange.Partition.NONE, 1,
                LogScanner.EARLIEST_OFFSET, 7L);

        Assertions.assertEquals(String.valueOf(LogScanner.EARLIEST_OFFSET),
                range.getProperties().get("fluss.log_start_offset"));
        Assertions.assertEquals("-2", range.getProperties().get("fluss.log_start_offset"));
    }

    /**
     * A tail range is a log range in everything but name — same offsets, same keys — and that is the
     * point: what differs is how the scanner treats what it reads (replay by key rather than row by row),
     * which is exactly what the range type says. Carrying a kv snapshot id would be the bug: the rows
     * before the tail come from the lake, not from a snapshot.
     */
    @Test
    public void tailRangeCarriesTheLogWindowAndNoKvSnapshot() {
        FlussScanRange range = FlussScanRange.pkTail(DT_20260101, 2, 300L, 305L);

        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("fluss.range_type", "PK_TAIL");
        expected.put("fluss.partition_id", "77");
        expected.put("fluss.partition_name", "dt=20260101");
        expected.put("fluss.bucket_id", "2");
        expected.put("fluss.log_start_offset", "300");
        expected.put("fluss.log_stop_offset", "305");
        Assertions.assertEquals(expected, range.getProperties());
    }

    /**
     * An empty tail must not become a range. The bucket it would describe is one the lake already holds
     * entirely, and planning says so by not producing a range at all; one that got here would mean the
     * lake half and the fluss half were bounded by different offsets, which is how rows get lost.
     */
    @Test
    public void emptyTailRangeIsRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> FlussScanRange.pkTail(DT_20260101, 0, 5L, 5L));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> FlussScanRange.pkTail(DT_20260101, 0, 6L, 5L));
    }

    @Test
    public void unpartitionedRangeOmitsThePartitionKeys() {
        FlussScanRange range = FlussScanRange.log(FlussScanRange.Partition.NONE, 0, 0L, 1L);

        Assertions.assertFalse(range.getProperties().containsKey("fluss.partition_id"));
        Assertions.assertFalse(range.getProperties().containsKey("fluss.partition_name"));
        Assertions.assertTrue(range.getPartitionValues().isEmpty());
    }

    /**
     * An unpartitioned range still reports itself partition-bearing: the engine reads a null partition
     * value list as "parse the values out of the file path", and a fluss range has no path to parse.
     */
    @Test
    public void rangesAreAlwaysPartitionBearing() {
        Assertions.assertTrue(
                FlussScanRange.log(FlussScanRange.Partition.NONE, 0, 0L, 1L).isPartitionBearing());
        Assertions.assertTrue(
                FlussScanRange.log(DT_20260101, 0, 0L, 1L).isPartitionBearing());
    }

    @Test
    public void tableFormatTypeSelectsTheFlussReader() {
        FlussScanRange range = FlussScanRange.log(FlussScanRange.Partition.NONE, 0, 0L, 1L);

        Assertions.assertEquals("fluss", range.getTableFormatType());
        // Default of the SPI: everything fluss reads goes through the JNI scanner, there is no
        // native path to downgrade to.
        Assertions.assertEquals("jni", range.getFileFormat());
    }

    @Test
    public void populateRangeParamsWritesTheWholeMapIntoFlussParams() {
        FlussScanRange range = FlussScanRange.log(FlussScanRange.Partition.NONE, 5, 0L, 9L);
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();

        range.populateRangeParams(formatDesc, rangeDesc);

        Assertions.assertEquals(range.getProperties(), formatDesc.getFlussParams());
        // No other format's params may be touched — BE dispatches on table_format_type and would
        // read a stale struct if one were half-filled.
        Assertions.assertFalse(formatDesc.isSetPaimonParams());
        Assertions.assertFalse(formatDesc.isSetIcebergParams());
        Assertions.assertFalse(formatDesc.isSetJdbcParams());
        Assertions.assertFalse(formatDesc.isSetEsParams());
        // Unpartitioned: nothing for BE to materialize from the range.
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPathKeys());
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPath());
    }

    /**
     * Partition columns are not in what the scanner returns — the connector declares them as
     * path partition keys — so BE materializes them from here. Order matters: BE pairs the value list
     * with the key list positionally.
     */
    @Test
    public void populateRangeParamsHandsPartitionColumnsToBe() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("dt", "20260101");
        values.put("region", "cn");
        FlussScanRange range = FlussScanRange.log(
                FlussScanRange.Partition.of(9L, "dt=20260101/region=cn", values), 0, 0L, 1L);
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();

        range.populateRangeParams(formatDesc, rangeDesc);

        Assertions.assertEquals(Arrays.asList("dt", "region"), rangeDesc.getColumnsFromPathKeys());
        Assertions.assertEquals(Arrays.asList("20260101", "cn"), rangeDesc.getColumnsFromPath());
        // fluss refuses a null partition value at write time, so there is no null to signal.
        Assertions.assertEquals(Arrays.asList(false, false), rangeDesc.getColumnsFromPathIsNull());
    }

    /** Half-describing a partition is the bug this rules out; NONE is the only way to say "none". */
    @Test
    public void partitionWithoutColumnValuesIsRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> FlussScanRange.Partition.of(1L, "dt=20260101", Collections.emptyMap()));
    }

    /**
     * The SPI declares a scan range {@link java.io.Serializable} and the engine takes it at its word.
     * Round-tripping also pins that "is this partitioned?" survives — answering it by identity against
     * the NONE singleton would come back wrong on the far side of a deserialization.
     */
    @Test
    public void rangeSurvivesJavaSerialization() throws Exception {
        for (FlussScanRange original : Arrays.asList(
                FlussScanRange.log(FlussScanRange.Partition.NONE, 3, 10L, 42L),
                FlussScanRange.pkTail(DT_20260101, 2, 300L, 305L))) {
            FlussScanRange restored = roundTrip(original);

            Assertions.assertEquals(original.getProperties(), restored.getProperties());
            Assertions.assertEquals(original.getPartitionValues(), restored.getPartitionValues());
            Assertions.assertEquals(original.getRangeType(), restored.getRangeType());

            // The restored NONE is a different object than the singleton; the range must still be
            // able to tell it is unpartitioned.
            TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            restored.populateRangeParams(formatDesc, rangeDesc);
            Assertions.assertEquals(original.getProperties(), formatDesc.getFlussParams());
        }
    }

    /**
     * {@code NONE} is a singleton but has no {@code readResolve}, so deserializing gives back a
     * different object. Anything that answers "is this partitioned?" by comparing against the singleton
     * would say yes here, and the range would then claim a partition id of -1 with no columns.
     */
    @Test
    public void restoredNonePartitionStillReportsUnpartitioned() throws Exception {
        FlussScanRange restored = roundTrip(
                FlussScanRange.log(FlussScanRange.Partition.NONE, 0, 0L, 1L));

        Assertions.assertNotSame(FlussScanRange.Partition.NONE, restored.getPartition());
        Assertions.assertFalse(restored.getPartition().isPartitioned());
        Assertions.assertTrue(restored.getPartitionValues().isEmpty());
    }

    @Test
    public void partitionExposesItsIdentity() {
        Assertions.assertTrue(DT_20260101.isPartitioned());
        Assertions.assertEquals(77L, DT_20260101.getId());
        Assertions.assertEquals("dt=20260101", DT_20260101.getName());
        Assertions.assertEquals(Collections.singletonMap("dt", "20260101"), DT_20260101.getValues());
        Assertions.assertFalse(FlussScanRange.Partition.NONE.isPartitioned());
    }

    private static FlussScanRange roundTrip(FlussScanRange range) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(range);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (FlussScanRange) in.readObject();
        }
    }

    /** Guards the fixture itself: an unmodifiable view is still a live view of a caller's map. */
    @Test
    public void partitionValuesAreCopiedFromTheCaller() {
        Map<String, String> mutable = new LinkedHashMap<>();
        mutable.put("dt", "20260101");
        FlussScanRange.Partition partition = FlussScanRange.Partition.of(1L, "dt=20260101", mutable);

        mutable.put("dt", "20260102");

        Assertions.assertEquals("20260101", partition.getValues().get("dt"));
    }

    /** The property map is the wire payload; a caller must not be able to edit it after the fact. */
    @Test
    public void propertiesAreImmutable() {
        List<FlussScanRange> ranges = Arrays.asList(
                FlussScanRange.log(FlussScanRange.Partition.NONE, 0, 0L, 1L),
                FlussScanRange.pkFull(DT_20260101, 0, 1L, 0L, 1L),
                FlussScanRange.pkTail(DT_20260101, 0, 0L, 1L));
        for (FlussScanRange range : ranges) {
            Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> range.getProperties().put("fluss.bucket_id", "999"));
            Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> range.getPartitionValues().put("dt", "x"));
        }
    }
}
