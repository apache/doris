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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * A single Iceberg scan range (split), mirroring the paimon connector's {@code PaimonScanRange}.
 *
 * <p>P6.2-T01 (this task) carries only the minimal {@code FILE_SCAN} file fields — path, byte
 * offset/length, file size, and real file format. The per-range carriers for merge-on-read deletes
 * ({@link #getDeleteFiles()}), JNI-serialized splits, schema-evolution schema-id, partition
 * column-from-path values, COUNT(*) pushdown row count, and the typed {@code populateRangeParams}
 * Thrift descriptor land in P6.2-T02..T09. Until then the inherited {@code ConnectorScanRange}
 * defaults apply (and the connector is not yet in {@code SPI_READY_TYPES}, so no range reaches BE).</p>
 */
public class IcebergScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final String fileFormat;

    private IcebergScanRange(Builder builder) {
        this.path = builder.path;
        this.start = builder.start;
        this.length = builder.length;
        this.fileSize = builder.fileSize;
        this.fileFormat = builder.fileFormat;
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
     * The table-format type string BE uses to select its Iceberg reader, mirroring paimon's
     * {@code "paimon"}: the value of {@code TableFormatType.ICEBERG} (see fe-core
     * {@code org.apache.doris.datasource.TableFormatType}).
     */
    @Override
    public String getTableFormatType() {
        return "iceberg";
    }

    @Override
    public Map<String, String> getProperties() {
        // No connector-specific per-range properties exist yet; the predicate / delete / schema-evolution
        // carriers that paimon flattens into this map arrive in P6.2-T03+.
        return Collections.emptyMap();
    }

    /**
     * Builder for {@link IcebergScanRange}, mirroring {@code PaimonScanRange.Builder} (constructed via
     * {@code new IcebergScanRange.Builder()}). Only the {@code FILE_SCAN} file fields are settable this
     * task; later tasks add the delete / JNI / schema-id / partition / COUNT carriers.
     */
    public static class Builder {
        private String path;
        private long start;
        private long length = -1;
        private long fileSize = -1;
        // Default empty (NOT "jni", which is not a real iceberg file format). Production callers set the
        // real orc/parquet once split enumeration lands (P6.2-T03); mirrors PaimonScanRange.Builder.
        private String fileFormat = "";

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

        public IcebergScanRange build() {
            return new IcebergScanRange(this);
        }
    }
}
