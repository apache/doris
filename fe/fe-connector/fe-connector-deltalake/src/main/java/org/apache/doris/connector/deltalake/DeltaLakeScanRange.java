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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TDeltaLakeFileDesc;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A native Parquet byte range. Partition values and deletion vectors describe the whole data file. */
final class DeltaLakeScanRange implements ConnectorScanRange {
    private static final long serialVersionUID = 1L;
    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final long modificationTime;
    private final long targetSize;
    private final Map<String, String> partitions;
    private final TDeltaLakeFileDesc descriptor;
    private final long rowCount;

    DeltaLakeScanRange(String path, long start, long length, long fileSize, long modificationTime,
            long targetSize, Map<String, String> partitions, TDeltaLakeFileDesc descriptor) {
        this(path, start, length, fileSize, modificationTime, targetSize, partitions, descriptor, -1);
    }

    private DeltaLakeScanRange(String path, long start, long length, long fileSize, long modificationTime,
            long targetSize, Map<String, String> partitions, TDeltaLakeFileDesc descriptor, long rowCount) {
        this.path = path;
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.modificationTime = modificationTime;
        this.targetSize = targetSize;
        this.partitions = Collections.unmodifiableMap(new LinkedHashMap<>(partitions));
        this.descriptor = descriptor.deepCopy();
        this.rowCount = rowCount;
    }

    DeltaLakeScanRange withByteRange(long offset, long size) {
        if (rowCount == -1 && start == offset && length == size) {
            return this;
        }
        return new DeltaLakeScanRange(path, offset, size, fileSize, modificationTime, targetSize,
                partitions, descriptor, -1);
    }

    DeltaLakeScanRange withRowCount(long count) {
        return new DeltaLakeScanRange(path, start, length, fileSize, modificationTime, targetSize,
                partitions, descriptor, count);
    }

    @Override
    public long getPushDownRowCount() {
        return rowCount;
    }

    @Override
    public boolean isNativeReadRange() {
        return true;
    }

    @Override
    public Optional<String> getPath() {
        return Optional.of(path);
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
    public long getModificationTime() {
        return modificationTime;
    }

    @Override
    public long getSelfSplitWeight() {
        return length;
    }

    @Override
    public long getTargetSplitSize() {
        return targetSize;
    }

    @Override
    public String getFileFormat() {
        return "parquet";
    }

    @Override
    public String getTableFormatType() {
        return "deltalake";
    }

    @Override
    public Map<String, String> getPartitionValues() {
        return partitions;
    }

    @Override
    public boolean isPartitionBearing() {
        // Delta log values are authoritative, including for a table with no partition columns.
        return true;
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.emptyMap();
    }

    @Override
    public void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
        formatDesc.setDeltalakeParams(descriptor.deepCopy());
        formatDesc.setTableLevelRowCount(rowCount);
        rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
        List<String> keys = new ArrayList<>(partitions.size());
        List<String> values = new ArrayList<>(partitions.size());
        List<Boolean> nulls = new ArrayList<>(partitions.size());
        partitions.forEach((key, value) -> {
            keys.add(key);
            values.add(value == null ? "" : value);
            nulls.add(value == null);
        });
        // Override generic Hive path decoding, preserving literal null-sentinel strings as strings.
        rangeDesc.setColumnsFromPathKeys(keys);
        rangeDesc.setColumnsFromPath(values);
        rangeDesc.setColumnsFromPathIsNull(nulls);
    }
}
