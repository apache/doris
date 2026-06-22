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

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.FileContent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A single Iceberg scan range (split), mirroring the paimon connector's {@code PaimonScanRange}.
 *
 * <p>P6.2-T03 makes the range BE-ready: beyond the minimal {@code FILE_SCAN} file fields (path, byte
 * offset/length, size, real file format) it carries the typed iceberg per-file descriptor inputs
 * ({@code formatVersion}, identity {@code partitionValues}, {@code partitionSpecId},
 * {@code partitionDataJson}, v3 {@code firstRowId}/{@code lastUpdatedSequenceNumber}) and emits them through
 * {@link #populateRangeParams} into {@code TTableFormatFileDesc.iceberg_params}, mirroring the legacy
 * {@code IcebergScanNode.setIcebergParams}. Unlike paimon (which stashes stringly-typed {@code paimon.*}
 * props), iceberg's carriers are strongly typed fields (its params are numeric), so {@link #getProperties()}
 * stays empty. T04 adds the typed merge-on-read {@link DeleteFile} carriers (position / equality / deletion
 * vector); T05 adds the COUNT(*)-pushdown row count ({@code pushDownRowCount} → {@code table_level_row_count}).
 * The field-id history dictionary (T06, scan-level) lands later. Iceberg is not yet in
 * {@code SPI_READY_TYPES}, so no range reaches BE.</p>
 */
public class IcebergScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    // The BE-facing data-file path: scheme-normalized (oss/cos/obs/s3a -> s3) so BE's S3 factory can open it.
    private final String path;
    // The RAW iceberg data-file path; BE matches position-delete entries against it (original_file_path).
    private final String originalPath;
    private final long start;
    private final long length;
    private final long fileSize;
    private final String fileFormat;
    private final int formatVersion;
    private final Integer partitionSpecId;
    private final String partitionDataJson;
    private final Long firstRowId;
    private final Long lastUpdatedSequenceNumber;
    // Identity partition column (lowercased) -> serialized value, already ordered as the path_partition_keys
    // list, filtered to keys this file carries. Drives columns-from-path. Never null (empty when unpartitioned).
    private final Map<String, String> partitionValues;
    // Merge-on-read delete files applying to this data file (T04). Never null (empty when none / v1).
    private final List<DeleteFile> deleteFiles;
    // COUNT(*) pushdown precomputed row count (T05): -1 = no precomputed count (the normal scan path);
    // >= 0 = the single collapsed count range carrying the snapshot-summary total. Drives both the generic
    // node's EXPLAIN "pushdown agg=COUNT (n)" line (getPushDownRowCount) and the BE thrift
    // table_level_row_count (populateRangeParams).
    private final long pushDownRowCount;

    private IcebergScanRange(Builder builder) {
        this.path = builder.path;
        // Default the raw original path to the (possibly already-raw) path when a caller does not split them
        // — keeps single-arg .path(...) callers (and the prior behavior) intact.
        this.originalPath = builder.originalPath != null ? builder.originalPath : builder.path;
        this.start = builder.start;
        this.length = builder.length;
        this.fileSize = builder.fileSize;
        this.fileFormat = builder.fileFormat;
        this.formatVersion = builder.formatVersion;
        this.partitionSpecId = builder.partitionSpecId;
        this.partitionDataJson = builder.partitionDataJson;
        this.firstRowId = builder.firstRowId;
        this.lastUpdatedSequenceNumber = builder.lastUpdatedSequenceNumber;
        this.partitionValues = builder.partitionValues != null
                ? Collections.unmodifiableMap(builder.partitionValues)
                : Collections.emptyMap();
        this.deleteFiles = builder.deleteFiles != null
                ? Collections.unmodifiableList(builder.deleteFiles)
                : Collections.emptyList();
        this.pushDownRowCount = builder.pushDownRowCount;
    }

    @Override
    public ConnectorScanRangeType getRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public Optional<String> getPath() {
        return Optional.ofNullable(path);
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public long getFileSize() {
        return fileSize;
    }

    @Override
    public String getFileFormat() {
        return fileFormat;
    }

    /**
     * The table-format-type string BE uses to select its Iceberg reader, mirroring paimon's
     * {@code "paimon"}: the value of {@code TableFormatType.ICEBERG} (see fe-core
     * {@code org.apache.doris.datasource.TableFormatType}).
     */
    @Override
    public String getTableFormatType() {
        return "iceberg";
    }

    /**
     * The identity partition column values for this file. The generic {@code PluginDrivenSplit} reads this to
     * route columns-from-path through {@code normalizeColumnsFromPath} (instead of path-parsing); the
     * authoritative columns-from-path is then (re)written by {@link #populateRangeParams}.
     */
    @Override
    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    /**
     * A file with a partition spec id belongs to a partitioned table, so its (possibly empty) identity
     * partition values come from iceberg metadata — not the file path. Returning {@code true} stops the engine
     * from falling back to Hive-style path parsing when {@link #getPartitionValues()} is empty (which would
     * throw for iceberg's non-{@code key=value}-directory layout, e.g. a partition-spec-evolution file whose
     * historical spec had no identity fields). Mirrors legacy {@code IcebergScanNode} always supplying a
     * non-null empty partition list.
     */
    @Override
    public boolean isPartitionBearing() {
        return partitionSpecId != null;
    }

    /**
     * The precomputed COUNT(*)-pushdown row count this range carries, or {@code -1} when none (the normal
     * scan path). The generic {@code PluginDrivenScanNode} reads it (via {@code resolvePushDownRowCount}) to
     * render the EXPLAIN {@code pushdown agg=COUNT (n)} line; the same value drives the BE thrift
     * {@code table_level_row_count} in {@link #populateRangeParams}. Mirrors paimon's {@code paimon.row_count}
     * carrier (typed here since iceberg's params are numeric). Default {@code -1} keeps every normal range
     * (T02/T03/T04) byte-unchanged — only the single collapsed count range (T05) carries a real count.
     */
    @Override
    public long getPushDownRowCount() {
        return pushDownRowCount;
    }

    @Override
    public Map<String, String> getProperties() {
        // Iceberg carries its per-range payload as typed fields (see populateRangeParams), not as string
        // properties; nothing engine-generic needs reading here.
        return Collections.emptyMap();
    }

    /**
     * Fills the per-file iceberg descriptor, mirroring legacy {@code IcebergScanNode.setIcebergParams}. The
     * generic {@code PluginDrivenScanNode} has already set {@code formatDesc.table_format_type = "iceberg"}
     * and pre-filled the {@code rangeDesc} file-level fields (and a path-parsed columns-from-path, which is
     * invalid for iceberg and is overwritten below). This runs AFTER the parent, so it owns the final
     * iceberg_params, per-range format type, and columns-from-path.
     */
    @Override
    public void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        fileDesc.setFormatVersion(formatVersion);
        // original_file_path = the RAW (un-normalized) data-file path; BE matches position-delete entries
        // against it (legacy setOriginalFilePath:304 uses the raw originalPath, not the normalized location).
        // This stays raw even though the range path (getPath) is scheme-normalized for BE to open.
        fileDesc.setOriginalFilePath(originalPath);
        if (partitionSpecId != null) {
            fileDesc.setPartitionSpecId(partitionSpecId);
        }
        if (partitionDataJson != null) {
            fileDesc.setPartitionDataJson(partitionDataJson);
        }
        if (formatVersion >= 3) {
            // -1 means a file carried over from a v2->v3 upgrade (no row lineage yet).
            fileDesc.setFirstRowId(firstRowId != null ? firstRowId : -1);
            fileDesc.setLastUpdatedSequenceNumber(
                    lastUpdatedSequenceNumber != null ? lastUpdatedSequenceNumber : -1);
        }
        if (formatVersion < 2) {
            // v1 has no delete files; legacy marks the file content as DATA.
            fileDesc.setContent(FileContent.DATA.id());
        } else {
            // v2+ : emit the merge-on-read delete files. Legacy setIcebergParams always calls
            // setDeleteFiles(new ArrayList<>()) before the per-delete loop, so the list is set even when
            // empty (a no-delete v2 table); each TIcebergDeleteFileDesc carries content/format/bounds/
            // field-ids/DV-offset exactly as legacy built them.
            List<TIcebergDeleteFileDesc> deleteDescs = new ArrayList<>(deleteFiles.size());
            for (DeleteFile delete : deleteFiles) {
                deleteDescs.add(delete.toThrift());
            }
            fileDesc.setDeleteFiles(deleteDescs);
        }

        // native reader format (JNI = system tables, P6.5). Leaves the parent's default (FORMAT_JNI) otherwise.
        if ("orc".equals(fileFormat)) {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
        } else if ("parquet".equals(fileFormat)) {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
        }

        // table_level_row_count: the single collapsed COUNT(*)-pushdown range carries the snapshot-summary
        // total (T05); every other range carries the distinct -1 sentinel (BE then counts by reading). The
        // carrier defaults to -1, so all normal/T03/T04 ranges are byte-unchanged.
        formatDesc.setTableLevelRowCount(pushDownRowCount);
        formatDesc.setIcebergParams(fileDesc);

        // Overwrite the parent's iceberg-invalid path-parsed columns-from-path: unset, then re-set from the
        // identity map (value "" + parallel is_null on a genuine null; NO __HIVE_DEFAULT_PARTITION__ sentinel).
        rangeDesc.unsetColumnsFromPath();
        rangeDesc.unsetColumnsFromPathKeys();
        rangeDesc.unsetColumnsFromPathIsNull();
        if (!partitionValues.isEmpty()) {
            List<String> keys = new ArrayList<>(partitionValues.size());
            List<String> values = new ArrayList<>(partitionValues.size());
            List<Boolean> isNull = new ArrayList<>(partitionValues.size());
            for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
                String value = entry.getValue();
                keys.add(entry.getKey());
                values.add(value != null ? value : "");
                isNull.add(value == null);
            }
            rangeDesc.setColumnsFromPathKeys(keys);
            rangeDesc.setColumnsFromPath(values);
            rangeDesc.setColumnsFromPathIsNull(isNull);
        }
    }

    /**
     * Builder for {@link IcebergScanRange}, mirroring {@code PaimonScanRange.Builder} (constructed via
     * {@code new IcebergScanRange.Builder()}).
     */
    public static class Builder {
        private String path;
        private String originalPath;
        private long start;
        private long length = -1;
        private long fileSize = -1;
        // Default empty (NOT "jni", which is not a real iceberg file format). Production callers set the real
        // orc/parquet from the data file's format; mirrors PaimonScanRange.Builder.
        private String fileFormat = "";
        // Default 2 (the iceberg metadata default); production callers set the real table format version.
        private int formatVersion = 2;
        private Integer partitionSpecId;
        private String partitionDataJson;
        private Long firstRowId;
        private Long lastUpdatedSequenceNumber;
        private Map<String, String> partitionValues;
        private List<DeleteFile> deleteFiles;
        private long pushDownRowCount = -1;

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        /** The RAW iceberg data-file path (for original_file_path); defaults to {@link #path} when unset. */
        public Builder originalPath(String originalPath) {
            this.originalPath = originalPath;
            return this;
        }

        public Builder start(long start) {
            this.start = start;
            return this;
        }

        public Builder length(long length) {
            this.length = length;
            return this;
        }

        public Builder fileSize(long fileSize) {
            this.fileSize = fileSize;
            return this;
        }

        public Builder fileFormat(String fileFormat) {
            this.fileFormat = fileFormat;
            return this;
        }

        public Builder formatVersion(int formatVersion) {
            this.formatVersion = formatVersion;
            return this;
        }

        public Builder partitionSpecId(Integer partitionSpecId) {
            this.partitionSpecId = partitionSpecId;
            return this;
        }

        public Builder partitionDataJson(String partitionDataJson) {
            this.partitionDataJson = partitionDataJson;
            return this;
        }

        public Builder firstRowId(Long firstRowId) {
            this.firstRowId = firstRowId;
            return this;
        }

        public Builder lastUpdatedSequenceNumber(Long lastUpdatedSequenceNumber) {
            this.lastUpdatedSequenceNumber = lastUpdatedSequenceNumber;
            return this;
        }

        public Builder partitionValues(Map<String, String> partitionValues) {
            this.partitionValues = partitionValues;
            return this;
        }

        public Builder deleteFiles(List<DeleteFile> deleteFiles) {
            this.deleteFiles = deleteFiles;
            return this;
        }

        /** The precomputed COUNT(*)-pushdown row count for the collapsed count range; default -1 (no count). */
        public Builder pushDownRowCount(long pushDownRowCount) {
            this.pushDownRowCount = pushDownRowCount;
            return this;
        }

        public IcebergScanRange build() {
            return new IcebergScanRange(this);
        }
    }

    /**
     * One merge-on-read delete file applying to the data file of this range, mirroring the legacy
     * {@code IcebergDeleteFileFilter} hierarchy + the {@code TIcebergDeleteFileDesc} that
     * {@code IcebergScanNode.setIcebergParams} builds from it. Immutable + {@link Serializable} (the
     * enclosing range is serialized into the split). The {@code content} ids are the legacy literals
     * (1 = position delete, 2 = equality delete, 3 = deletion vector); only the fields relevant to a
     * given kind are non-null, and {@link #toThrift()} sets only the non-null ones (legacy sets bounds
     * only when present and {@code file_format} only for parquet/orc).
     */
    public static final class DeleteFile implements Serializable {

        private static final long serialVersionUID = 1L;

        // Iceberg file type (TIcebergDeleteFileDesc.content): 1 = position delete, 2 = equality delete,
        // 3 = deletion vector (legacy IcebergDeleteFileFilter.{PositionDelete,EqualityDelete,DeletionVector}.type()).
        private static final int CONTENT_POSITION_DELETE = 1;
        private static final int CONTENT_EQUALITY_DELETE = 2;
        private static final int CONTENT_DELETION_VECTOR = 3;

        private final String path;
        private final int content;
        // null for a deletion vector (PUFFIN); legacy setDeleteFileFormat only emits parquet/orc.
        private final TFileFormatType fileFormat;
        // null = unset (legacy: bound absent, or the -1 sentinel which is treated as absent).
        private final Long positionLowerBound;
        private final Long positionUpperBound;
        // equality delete only (null otherwise).
        private final List<Integer> fieldIds;
        // deletion vector only (null otherwise).
        private final Long contentOffset;
        private final Long contentSizeInBytes;

        private DeleteFile(String path, int content, TFileFormatType fileFormat, Long positionLowerBound,
                Long positionUpperBound, List<Integer> fieldIds, Long contentOffset, Long contentSizeInBytes) {
            this.path = path;
            this.content = content;
            this.fileFormat = fileFormat;
            this.positionLowerBound = positionLowerBound;
            this.positionUpperBound = positionUpperBound;
            this.fieldIds = fieldIds != null ? Collections.unmodifiableList(new ArrayList<>(fieldIds)) : null;
            this.contentOffset = contentOffset;
            this.contentSizeInBytes = contentSizeInBytes;
        }

        /** A position delete file (content 1): row positions to drop, with optional [lower,upper] bounds. */
        public static DeleteFile positionDelete(String path, TFileFormatType fileFormat,
                Long positionLowerBound, Long positionUpperBound) {
            return new DeleteFile(path, CONTENT_POSITION_DELETE, fileFormat,
                    positionLowerBound, positionUpperBound, null, null, null);
        }

        /**
         * A deletion vector (content 3): a PUFFIN blob referenced by {@code contentOffset}/
         * {@code contentSizeInBytes}. It is a position delete, so it also carries the optional position
         * bounds (legacy {@code DeletionVector extends PositionDelete}); {@code file_format} stays unset.
         */
        public static DeleteFile deletionVector(String path, Long positionLowerBound, Long positionUpperBound,
                long contentOffset, long contentSizeInBytes) {
            return new DeleteFile(path, CONTENT_DELETION_VECTOR, null,
                    positionLowerBound, positionUpperBound, null, contentOffset, contentSizeInBytes);
        }

        /** An equality delete file (content 2): rows equal on {@code fieldIds} are dropped (BE re-projects). */
        public static DeleteFile equalityDelete(String path, TFileFormatType fileFormat, List<Integer> fieldIds) {
            return new DeleteFile(path, CONTENT_EQUALITY_DELETE, fileFormat, null, null, fieldIds, null, null);
        }

        int getContent() {
            return content;
        }

        TIcebergDeleteFileDesc toThrift() {
            TIcebergDeleteFileDesc desc = new TIcebergDeleteFileDesc();
            desc.setPath(path);
            if (fileFormat != null) {
                desc.setFileFormat(fileFormat);
            }
            if (positionLowerBound != null) {
                desc.setPositionLowerBound(positionLowerBound);
            }
            if (positionUpperBound != null) {
                desc.setPositionUpperBound(positionUpperBound);
            }
            if (fieldIds != null) {
                desc.setFieldIds(new ArrayList<>(fieldIds));
            }
            if (contentOffset != null) {
                desc.setContentOffset(contentOffset);
            }
            if (contentSizeInBytes != null) {
                desc.setContentSizeInBytes(contentSizeInBytes);
            }
            desc.setContent(content);
            return desc;
        }
    }
}
