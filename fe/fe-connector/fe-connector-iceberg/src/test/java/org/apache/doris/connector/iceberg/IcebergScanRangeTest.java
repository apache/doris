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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Skeleton tests for {@link IcebergScanRange} (P6.2-T01), mirroring the paimon connector's
 * {@code PaimonScanRange} carrier. Only the minimal {@code FILE_SCAN} file fields (path/start/length/
 * size/format) exist this task; the per-range delete-file / JNI-split / schema-id / partition / COUNT
 * carriers and {@code populateRangeParams} land in P6.2-T02..T09. These pin the carrier the builder
 * produces today so a later task that breaks the FILE_SCAN contract fails loudly.
 */
public class IcebergScanRangeTest {

    @Test
    public void builderProducesFileScanRangeWithFileFields() {
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://bucket/db/t/data/f.parquet")
                .start(128L)
                .length(4096L)
                .fileSize(8192L)
                .fileFormat("parquet")
                .build();

        // WHY: iceberg is a file-based connector, so the engine must build a TFileScanRange off this range.
        // MUTATION: returning JDBC_SCAN / CUSTOM -> wrong thrift scan-range variant -> red.
        Assertions.assertEquals(ConnectorScanRangeType.FILE_SCAN, range.getRangeType());
        Assertions.assertEquals(Optional.of("s3://bucket/db/t/data/f.parquet"), range.getPath());
        Assertions.assertEquals(128L, range.getStart());
        Assertions.assertEquals(4096L, range.getLength());
        Assertions.assertEquals(8192L, range.getFileSize());
        Assertions.assertEquals("parquet", range.getFileFormat());
        // WHY: BE selects its iceberg reader off TTableFormatFileDesc.table_format_type, whose value for
        // iceberg is "iceberg" (TableFormatType.ICEBERG). MUTATION: "paimon" / "plugin_driven" -> BE routes
        // the split to the wrong reader -> red.
        Assertions.assertEquals("iceberg", range.getTableFormatType());
    }

    @Test
    public void builderDefaultsMatchFileScanContract() {
        // A range built with only a path keeps the ConnectorScanRange contract defaults: start=0,
        // length=-1 (whole file), fileSize=-1 (unknown), fileFormat="" (not-yet-known, NOT "jni").
        // Pins that the skeleton does not invent values.
        IcebergScanRange range = new IcebergScanRange.Builder().path("/tmp/x").build();
        Assertions.assertEquals(0L, range.getStart());
        Assertions.assertEquals(-1L, range.getLength());
        Assertions.assertEquals(-1L, range.getFileSize());
        Assertions.assertEquals("", range.getFileFormat());
        // No connector-specific per-range properties exist yet (T03 introduces them); must be non-null.
        Assertions.assertNotNull(range.getProperties());
        Assertions.assertTrue(range.getProperties().isEmpty());
    }

    // ---- T03: populateRangeParams -> TIcebergFileDesc (mirrors legacy IcebergScanNode.setIcebergParams) ----

    private static TFileRangeDesc populate(IcebergScanRange range, TFileRangeDesc rangeDesc) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        formatDesc.setTableFormatType("iceberg");   // the generic node sets this from getTableFormatType()
        range.populateRangeParams(formatDesc, rangeDesc);
        rangeDesc.setTableFormatParams(formatDesc);
        return rangeDesc;
    }

    @Test
    public void populateRangeParamsV2PartitionedDataFile() {
        Map<String, String> parts = new LinkedHashMap<>();
        parts.put("p", "1");
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/p=1/f.parquet").start(0L).length(512L).fileSize(512L)
                .fileFormat("parquet").formatVersion(2)
                .partitionSpecId(0).partitionDataJson("[\"1\"]")
                .partitionValues(parts).build();

        TFileRangeDesc rangeDesc = populate(range, new TFileRangeDesc());
        TIcebergFileDesc fd = rangeDesc.getTableFormatParams().getIcebergParams();

        // Core v2 carriers. MUTATION: dropping any field (T01 default populateRangeParams dumps to jdbc_params
        // and never sets iceberg_params) -> red.
        Assertions.assertTrue(rangeDesc.getTableFormatParams().isSetIcebergParams());
        Assertions.assertEquals(2, fd.getFormatVersion());
        Assertions.assertEquals("s3://b/db/t/p=1/f.parquet", fd.getOriginalFilePath());
        Assertions.assertTrue(fd.isSetPartitionSpecId());
        Assertions.assertEquals(0, fd.getPartitionSpecId());
        Assertions.assertEquals("[\"1\"]", fd.getPartitionDataJson());
        // v2: no v1 content, no v3 row-lineage.
        Assertions.assertFalse(fd.isSetContent());
        Assertions.assertFalse(fd.isSetFirstRowId());
        Assertions.assertFalse(fd.isSetLastUpdatedSequenceNumber());
        // native parquet -> per-range FORMAT_PARQUET; non-count path -> table_level_row_count = -1.
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, rangeDesc.getFormatType());
        Assertions.assertEquals(-1L, rangeDesc.getTableFormatParams().getTableLevelRowCount());
        // identity partition columns -> columns-from-path (value present, not null).
        Assertions.assertEquals(Collections.singletonList("p"), rangeDesc.getColumnsFromPathKeys());
        Assertions.assertEquals(Collections.singletonList("1"), rangeDesc.getColumnsFromPath());
        Assertions.assertEquals(Collections.singletonList(false), rangeDesc.getColumnsFromPathIsNull());
    }

    @Test
    public void populateRangeParamsV1SetsDataContentAndOrcFormat() {
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.orc").fileFormat("orc").formatVersion(1).build();

        TIcebergFileDesc fd = populate(range, new TFileRangeDesc()).getTableFormatParams().getIcebergParams();

        // v1 only: content = FileContent.DATA.id() (== 0). MUTATION: setting content unconditionally (v2+) -> red.
        Assertions.assertTrue(fd.isSetContent());
        Assertions.assertEquals(0, fd.getContent());
        Assertions.assertEquals(1, fd.getFormatVersion());
        // v1 has no delete files: delete_files stays unset (legacy only sets it for v2+). MUTATION: emitting
        // an empty delete_files list for v1 -> red.
        Assertions.assertFalse(fd.isSetDeleteFiles());
        // orc data file -> per-range FORMAT_ORC.
        Assertions.assertEquals(TFileFormatType.FORMAT_ORC,
                populate(range, new TFileRangeDesc()).getFormatType());
    }

    // ---- T04: merge-on-read delete files -> TIcebergFileDesc.delete_files ----

    @Test
    public void populateRangeParamsV2NoDeletesEmitsEmptyDeleteFilesList() {
        // A v2 table with no deletes: legacy setIcebergParams calls setDeleteFiles(new ArrayList<>()) before
        // the (empty) loop, so the list is SET but empty, and content is NOT the v1 DATA marker.
        // MUTATION: leaving delete_files unset for v2 (the T03 behavior) -> red; setting content=DATA -> red.
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(2).build();

        TIcebergFileDesc fd = populate(range, new TFileRangeDesc()).getTableFormatParams().getIcebergParams();

        Assertions.assertTrue(fd.isSetDeleteFiles());
        Assertions.assertTrue(fd.getDeleteFiles().isEmpty());
        Assertions.assertFalse(fd.isSetContent());
    }

    @Test
    public void populateRangeParamsV2EmitsPositionDeleteFile() {
        // A position delete (content 1) with parquet format + [lower,upper] bounds. MUTATION: dropping the
        // bounds, wrong content id, or wrong format -> red.
        IcebergScanRange.DeleteFile posDelete = IcebergScanRange.DeleteFile.positionDelete(
                "s3://b/db/t/pos-delete.parquet", TFileFormatType.FORMAT_PARQUET, 10L, 99L);
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(2)
                .deleteFiles(Collections.singletonList(posDelete)).build();

        TIcebergFileDesc fd = populate(range, new TFileRangeDesc()).getTableFormatParams().getIcebergParams();

        Assertions.assertEquals(1, fd.getDeleteFilesSize());
        TIcebergDeleteFileDesc d = fd.getDeleteFiles().get(0);
        Assertions.assertEquals("s3://b/db/t/pos-delete.parquet", d.getPath());
        Assertions.assertEquals(1, d.getContent());
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, d.getFileFormat());
        Assertions.assertTrue(d.isSetPositionLowerBound());
        Assertions.assertEquals(10L, d.getPositionLowerBound());
        Assertions.assertTrue(d.isSetPositionUpperBound());
        Assertions.assertEquals(99L, d.getPositionUpperBound());
        // A position delete carries neither equality field-ids nor a deletion-vector blob ref.
        Assertions.assertFalse(d.isSetFieldIds());
        Assertions.assertFalse(d.isSetContentOffset());
        Assertions.assertFalse(d.isSetContentSizeInBytes());
    }

    @Test
    public void populateRangeParamsV2EmitsPositionDeleteWithoutBounds() {
        // No bounds present -> position_lower/upper_bound left UNSET (legacy emits them only when present).
        // MUTATION: defaulting an absent bound to 0 / -1 instead of unset -> red.
        IcebergScanRange.DeleteFile posDelete = IcebergScanRange.DeleteFile.positionDelete(
                "s3://b/db/t/pos-delete.orc", TFileFormatType.FORMAT_ORC, null, null);
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(2)
                .deleteFiles(Collections.singletonList(posDelete)).build();

        TIcebergDeleteFileDesc d = populate(range, new TFileRangeDesc())
                .getTableFormatParams().getIcebergParams().getDeleteFiles().get(0);

        Assertions.assertEquals(TFileFormatType.FORMAT_ORC, d.getFileFormat());
        Assertions.assertFalse(d.isSetPositionLowerBound());
        Assertions.assertFalse(d.isSetPositionUpperBound());
    }

    @Test
    public void populateRangeParamsV2EmitsDeletionVectorAndEqualityDelete() {
        // A deletion vector (content 3, PUFFIN): blob content_offset/size set, file_format UNSET, bounds
        // carried (it IS a position delete). An equality delete (content 2): field-ids set, no bounds/blob.
        IcebergScanRange.DeleteFile dv = IcebergScanRange.DeleteFile.deletionVector(
                "s3://b/db/t/dv.puffin", 5L, 42L, 16L, 64L);
        IcebergScanRange.DeleteFile eq = IcebergScanRange.DeleteFile.equalityDelete(
                "s3://b/db/t/eq-delete.parquet", TFileFormatType.FORMAT_PARQUET, Arrays.asList(3, 7));
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(2)
                .deleteFiles(Arrays.asList(dv, eq)).build();

        List<TIcebergDeleteFileDesc> deletes = populate(range, new TFileRangeDesc())
                .getTableFormatParams().getIcebergParams().getDeleteFiles();
        Assertions.assertEquals(2, deletes.size());

        TIcebergDeleteFileDesc dvDesc = deletes.get(0);
        Assertions.assertEquals(3, dvDesc.getContent());
        // MUTATION: emitting file_format for a PUFFIN DV (legacy setDeleteFileFormat skips PUFFIN) -> red.
        Assertions.assertFalse(dvDesc.isSetFileFormat());
        Assertions.assertEquals(16L, dvDesc.getContentOffset());
        Assertions.assertEquals(64L, dvDesc.getContentSizeInBytes());
        Assertions.assertEquals(5L, dvDesc.getPositionLowerBound());
        Assertions.assertEquals(42L, dvDesc.getPositionUpperBound());

        TIcebergDeleteFileDesc eqDesc = deletes.get(1);
        Assertions.assertEquals(2, eqDesc.getContent());
        Assertions.assertEquals(Arrays.asList(3, 7), eqDesc.getFieldIds());
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, eqDesc.getFileFormat());
        Assertions.assertFalse(eqDesc.isSetContentOffset());
        Assertions.assertFalse(eqDesc.isSetPositionLowerBound());
    }

    @Test
    public void populateRangeParamsV3SetsRowLineage() {
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(3)
                .firstRowId(100L).lastUpdatedSequenceNumber(5L).build();

        TIcebergFileDesc fd = populate(range, new TFileRangeDesc()).getTableFormatParams().getIcebergParams();

        // v3 row-lineage carriers. MUTATION: gating these on v2 / never setting them -> red.
        Assertions.assertTrue(fd.isSetFirstRowId());
        Assertions.assertEquals(100L, fd.getFirstRowId());
        Assertions.assertTrue(fd.isSetLastUpdatedSequenceNumber());
        Assertions.assertEquals(5L, fd.getLastUpdatedSequenceNumber());
        // v3 is NOT v1 -> no DATA content marker.
        Assertions.assertFalse(fd.isSetContent());
    }

    @Test
    public void populateRangeParamsV3NullRowLineageFallsBackToMinusOne() {
        // The v2->v3 upgrade case: a data file added before the upgrade has no first_row_id; legacy emits -1.
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(3)
                .firstRowId(null).lastUpdatedSequenceNumber(null).build();

        TIcebergFileDesc fd = populate(range, new TFileRangeDesc()).getTableFormatParams().getIcebergParams();

        Assertions.assertEquals(-1L, fd.getFirstRowId());
        Assertions.assertEquals(-1L, fd.getLastUpdatedSequenceNumber());
    }

    @Test
    public void populateRangeParamsUnpartitionedOmitsPartitionFields() {
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(2).build();

        TFileRangeDesc rangeDesc = populate(range, new TFileRangeDesc());
        TIcebergFileDesc fd = rangeDesc.getTableFormatParams().getIcebergParams();

        // Unpartitioned: spec-id / data-json absent; no columns-from-path. MUTATION: emitting empty lists -> red.
        Assertions.assertFalse(fd.isSetPartitionSpecId());
        Assertions.assertFalse(fd.isSetPartitionDataJson());
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPath());
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPathKeys());
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPathIsNull());
    }

    @Test
    public void populateRangeParamsUnsetsParentPathParsedColumnsFromPath() {
        // The generic FileQueryScanNode pre-fills columns-from-path by PARSING the path (iceberg does NOT
        // Hive-path-encode partitions -> garbage). populateRangeParams must UNSET those before (not) re-setting,
        // exactly like legacy setIcebergParams. MUTATION: skipping the unset -> the stale parsed values survive.
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setColumnsFromPath(Arrays.asList("stale"));
        rangeDesc.setColumnsFromPathKeys(Arrays.asList("stalekey"));
        rangeDesc.setColumnsFromPathIsNull(Arrays.asList(false));

        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(2).build();
        populate(range, rangeDesc);

        Assertions.assertFalse(rangeDesc.isSetColumnsFromPath());
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPathKeys());
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPathIsNull());
    }

    @Test
    public void populateRangeParamsNullPartitionValueUsesIsNullList() {
        // A genuine-null identity partition value: value rendered as "" with the parallel is_null = true (NO
        // __HIVE_DEFAULT_PARTITION__ sentinel — iceberg conveys null purely via the is_null list).
        Map<String, String> parts = new LinkedHashMap<>();
        parts.put("p", null);
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(2)
                .partitionSpecId(0).partitionValues(parts).build();

        TFileRangeDesc rangeDesc = populate(range, new TFileRangeDesc());

        Assertions.assertEquals(Collections.singletonList("p"), rangeDesc.getColumnsFromPathKeys());
        Assertions.assertEquals(Collections.singletonList(""), rangeDesc.getColumnsFromPath());
        Assertions.assertEquals(Collections.singletonList(true), rangeDesc.getColumnsFromPathIsNull());
    }

    @Test
    public void isPartitionBearingTracksPartitionSpecId() {
        // A file with a partition spec id is partition-bearing (metadata-sourced partitions) -> the engine must
        // NOT path-parse an empty partition map (which throws for iceberg's non-key=value layout). An
        // unpartitioned file (no spec id) is not partition-bearing -> keeps the legacy empty->null collapse.
        // MUTATION: returning the SPI default false for a partitioned file -> empty-map path-parse throw -> red.
        IcebergScanRange partitioned = new IcebergScanRange.Builder()
                .path("x").partitionSpecId(0).partitionValues(Collections.emptyMap()).build();
        Assertions.assertTrue(partitioned.isPartitionBearing());
        IcebergScanRange unpartitioned = new IcebergScanRange.Builder().path("x").build();
        Assertions.assertFalse(unpartitioned.isPartitionBearing());
    }

    @Test
    public void getPartitionValuesExposesTheIdentityMap() {
        Map<String, String> parts = new LinkedHashMap<>();
        parts.put("p", "1");
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("x").partitionValues(parts).build();
        // The parent PluginDrivenSplit reads getPartitionValues() to route columns-from-path through
        // normalizeColumnsFromPath (not path-parsing). MUTATION: returning emptyMap -> red.
        Assertions.assertEquals(parts, range.getPartitionValues());
    }

    // ---- T05: COUNT(*) pushdown row count carrier (pushDownRowCount -> table_level_row_count) ----

    @Test
    public void pushDownRowCountDefaultsToMinusOne() {
        // The normal scan path never sets a count: the SPI carrier defaults to -1 so the generic node renders
        // the (-1) "no precomputed count" sentinel and BE counts by reading. MUTATION: defaulting to 0 (a
        // valid count) -> the EXPLAIN line and BE count path would treat every range as pre-counted -> red.
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(2).build();
        Assertions.assertEquals(-1L, range.getPushDownRowCount());
        Assertions.assertEquals(-1L,
                populate(range, new TFileRangeDesc()).getTableFormatParams().getTableLevelRowCount());
    }

    @Test
    public void populateRangeParamsEmitsTableLevelRowCountWhenCountPushed() {
        // The single collapsed count range carries the snapshot-summary total -> BE serves COUNT from
        // table_level_row_count without reading. MUTATION: still emitting the constant -1 (T03 behavior) ->
        // BE re-reads and the EXPLAIN (n) is lost -> red.
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(2)
                .pushDownRowCount(4242L).build();
        Assertions.assertEquals(4242L, range.getPushDownRowCount());
        Assertions.assertEquals(4242L,
                populate(range, new TFileRangeDesc()).getTableFormatParams().getTableLevelRowCount());
    }

    // ---- T05: system-table serialized-split carrier (serialized_split + FORMAT_JNI, minimal shape) ----

    @Test
    public void serializedSplitDefaultsToNullAndIsNotEmittedOnNormalRanges() {
        // A normal data-file range carries no serialized_split: the carrier defaults to null and
        // populateRangeParams must NOT set the thrift field, so every T02/T03/T04 range is byte-unchanged.
        // MUTATION: emitting serialized_split unconditionally -> a stray sys field on every native range -> red.
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(2).build();
        Assertions.assertNull(range.getSerializedSplit());
        TIcebergFileDesc fd = populate(range, new TFileRangeDesc()).getTableFormatParams().getIcebergParams();
        Assertions.assertFalse(fd.isSetSerializedSplit());
    }

    @Test
    public void populateRangeParamsSystemTableEmitsSerializedSplitAndJniFormatOnly() {
        // A system-table (JNI) range mirrors legacy IcebergScanNode.setIcebergParams isSystemTable branch:
        // emit ONLY serialized_split + FORMAT_JNI + table_level_row_count=-1, and NONE of the file-level
        // carriers (format_version, original_file_path, content, delete_files, partition). The BE
        // IcebergSysTableJniScanner reads serialized_split and ignores every other field, so emitting them
        // would be a parity divergence. MUTATION: falling through to the normal data-file shape (setting
        // format_version / original_file_path) -> red; not setting FORMAT_JNI -> red; not setting
        // serialized_split -> red.
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("/dummyPath").serializedSplit("BASE64-FILESCANTASK").build();
        Assertions.assertEquals("BASE64-FILESCANTASK", range.getSerializedSplit());

        TFileRangeDesc rangeDesc = populate(range, new TFileRangeDesc());
        TIcebergFileDesc fd = rangeDesc.getTableFormatParams().getIcebergParams();

        Assertions.assertTrue(rangeDesc.getTableFormatParams().isSetIcebergParams());
        Assertions.assertEquals("BASE64-FILESCANTASK", fd.getSerializedSplit());
        // FORMAT_JNI per-range (legacy setIcebergParams:290), and the -1 table-level row count (legacy :291).
        Assertions.assertEquals(TFileFormatType.FORMAT_JNI, rangeDesc.getFormatType());
        Assertions.assertEquals(-1L, rangeDesc.getTableFormatParams().getTableLevelRowCount());
        // Minimal shape: NONE of the normal data-file carriers are set (legacy returns early before them).
        Assertions.assertFalse(fd.isSetFormatVersion());
        Assertions.assertFalse(fd.isSetOriginalFilePath());
        Assertions.assertFalse(fd.isSetContent());
        Assertions.assertFalse(fd.isSetDeleteFiles());
        Assertions.assertFalse(fd.isSetPartitionSpecId());
        // No columns-from-path on a sys split (the metadata table is not path-partitioned).
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPath());
    }

    // ── commit-bridge supply (S4 part 2): getOriginalPath() key + rewritableDeleteDescs() non-equality filter ──

    @Test
    public void getOriginalPathReturnsRawDataFilePathTheBeMatchesOn() {
        // The stash keys on this exact string (the BE matches a rewritable delete set against it). It is the RAW
        // path, distinct from the scheme-normalized open path. MUTATION: returning the normalized path here would
        // make every BE lookup miss -> resurrection.
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet")
                .originalPath("oss://b/db/t/f.parquet")
                .build();
        Assertions.assertEquals("oss://b/db/t/f.parquet", range.getOriginalPath());
    }

    @Test
    public void rewritableDeleteDescsKeepsDvAndPositionButDropsEquality() {
        // The rewritable supply is OR-merged into the new DV; only position deletes (content 1) and deletion
        // vectors (content 3) participate. Equality deletes (content 2) are re-applied by the reader, never
        // rewritten, so they MUST be excluded (mirrors legacy deleteFilesDescByReferencedDataFile). MUTATION:
        // including the equality delete -> the BE would treat equality rows as positions / over-delete.
        IcebergScanRange.DeleteFile dv = IcebergScanRange.DeleteFile.deletionVector(
                "s3://b/db/t/dv.puffin", 5L, 42L, 16L, 64L);
        IcebergScanRange.DeleteFile pos = IcebergScanRange.DeleteFile.positionDelete(
                "s3://b/db/t/pos.parquet", TFileFormatType.FORMAT_PARQUET, 1L, 9L);
        IcebergScanRange.DeleteFile eq = IcebergScanRange.DeleteFile.equalityDelete(
                "s3://b/db/t/eq.parquet", TFileFormatType.FORMAT_PARQUET, Arrays.asList(3, 7));
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(3)
                .deleteFiles(Arrays.asList(dv, pos, eq)).build();

        List<TIcebergDeleteFileDesc> descs = range.rewritableDeleteDescs();
        Assertions.assertEquals(2, descs.size());
        Assertions.assertEquals(3, descs.get(0).getContent());
        Assertions.assertEquals("s3://b/db/t/dv.puffin", descs.get(0).getPath());
        // DV carries the blob coordinates the BE needs to read it.
        Assertions.assertEquals(16L, descs.get(0).getContentOffset());
        Assertions.assertEquals(64L, descs.get(0).getContentSizeInBytes());
        Assertions.assertEquals(1, descs.get(1).getContent());
        // Position delete carries its file_format.
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, descs.get(1).getFileFormat());
    }

    @Test
    public void rewritableDeleteDescsEmptyWhenNoDeletesOrAllEquality() {
        IcebergScanRange none = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(3).build();
        Assertions.assertTrue(none.rewritableDeleteDescs().isEmpty());

        IcebergScanRange.DeleteFile eq = IcebergScanRange.DeleteFile.equalityDelete(
                "s3://b/db/t/eq.parquet", TFileFormatType.FORMAT_PARQUET, Arrays.asList(3));
        IcebergScanRange onlyEq = new IcebergScanRange.Builder()
                .path("s3://b/db/t/f.parquet").fileFormat("parquet").formatVersion(3)
                .deleteFiles(Collections.singletonList(eq)).build();
        Assertions.assertTrue(onlyEq.rewritableDeleteDescs().isEmpty());
    }
}
