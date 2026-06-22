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
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.FileContent;

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
 * stays empty. Merge-on-read delete files (T04), COUNT pushdown (T05), and the field-id history dictionary
 * (T06, scan-level) land later. Iceberg is not yet in {@code SPI_READY_TYPES}, so no range reaches BE.</p>
 */
public class IcebergScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    private final String path;
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

    private IcebergScanRange(Builder builder) {
        this.path = builder.path;
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
        // original_file_path = the raw (un-normalized) data-file path; BE matches position-delete entries
        // against it. The range path IS that raw path (normalization happens later in the parent).
        fileDesc.setOriginalFilePath(path);
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
            // v1 has no delete files; legacy marks the file content as DATA. v2+ instead set delete_files (T04).
            fileDesc.setContent(FileContent.DATA.id());
        }

        // native reader format (JNI = system tables, P6.5). Leaves the parent's default (FORMAT_JNI) otherwise.
        if ("orc".equals(fileFormat)) {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
        } else if ("parquet".equals(fileFormat)) {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
        }

        // Non-count path: distinct -1 (COUNT pushdown sets a real count in T05).
        formatDesc.setTableLevelRowCount(-1);
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

        public Builder path(String path) {
            this.path = path;
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

        public IcebergScanRange build() {
            return new IcebergScanRange(this);
        }
    }
}
