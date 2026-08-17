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

package org.apache.doris.datasource.iceberg.cache;

import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.StructLike;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Cached manifest payload containing parsed files.
 */
public class ManifestCacheValue {
    private static final long AUXILIARY_LIST_ENTRY_BYTES = 32L;

    private final List<DataFile> dataFiles;
    private final List<DeleteFile> deleteFiles;
    private final long dataFileMetricEntryCount;
    private final long deleteFileMetricEntryCount;
    private final long retainedPayloadBytes;
    private final boolean accountingComplete;

    private ManifestCacheValue(List<DataFile> dataFiles, List<DeleteFile> deleteFiles,
            long dataFileMetricEntryCount, long deleteFileMetricEntryCount, long retainedPayloadBytes,
            boolean accountingComplete) {
        this.dataFiles = ImmutableList.copyOf(dataFiles);
        this.deleteFiles = ImmutableList.copyOf(deleteFiles);
        this.dataFileMetricEntryCount = dataFileMetricEntryCount;
        this.deleteFileMetricEntryCount = deleteFileMetricEntryCount;
        this.retainedPayloadBytes = retainedPayloadBytes;
        this.accountingComplete = accountingComplete;
    }

    public static ManifestCacheValue forDataFiles(List<DataFile> dataFiles) {
        Builder builder = dataFilesBuilder();
        if (dataFiles != null) {
            dataFiles.forEach(builder::addDataFile);
        }
        return builder.build();
    }

    public static ManifestCacheValue forDeleteFiles(List<DeleteFile> deleteFiles) {
        Builder builder = deleteFilesBuilder();
        if (deleteFiles != null) {
            deleteFiles.forEach(builder::addDeleteFile);
        }
        return builder.build();
    }

    public static Builder dataFilesBuilder() {
        return dataFilesBuilder(true);
    }

    public static Builder dataFilesBuilder(boolean accountRetainedSize) {
        return new Builder(true, accountRetainedSize);
    }

    public static Builder deleteFilesBuilder() {
        return deleteFilesBuilder(true);
    }

    public static Builder deleteFilesBuilder(boolean accountRetainedSize) {
        return new Builder(false, accountRetainedSize);
    }

    public List<DataFile> getDataFiles() {
        return dataFiles;
    }

    public List<DeleteFile> getDeleteFiles() {
        return deleteFiles;
    }

    public long getDataFileMetricEntryCount() {
        return dataFileMetricEntryCount;
    }

    public long getDeleteFileMetricEntryCount() {
        return deleteFileMetricEntryCount;
    }

    public long getRetainedPayloadBytes() {
        return retainedPayloadBytes;
    }

    public boolean isAccountingComplete() {
        return accountingComplete;
    }

    /** Accumulates retained-size counters in the manifest reader's existing file loop. */
    public static final class Builder {
        private final boolean dataContent;
        private final boolean accountRetainedSize;
        private final List<DataFile> dataFiles = new ArrayList<>();
        private final List<DeleteFile> deleteFiles = new ArrayList<>();
        private long metricEntryCount;
        private long retainedPayloadBytes;
        private boolean accountingComplete;

        private Builder(boolean dataContent, boolean accountRetainedSize) {
            this.dataContent = dataContent;
            this.accountRetainedSize = accountRetainedSize;
            this.accountingComplete = accountRetainedSize;
        }

        public void addDataFile(DataFile file) {
            if (!dataContent) {
                throw new IllegalStateException("delete manifest builder cannot accept a data file");
            }
            dataFiles.add(file);
            accountSafely(file);
        }

        public void addDeleteFile(DeleteFile file) {
            if (dataContent) {
                throw new IllegalStateException("data manifest builder cannot accept a delete file");
            }
            deleteFiles.add(file);
            accountSafely(file);
        }

        public ManifestCacheValue build() {
            return new ManifestCacheValue(dataFiles, deleteFiles,
                    dataContent ? metricEntryCount : 0L,
                    dataContent ? 0L : metricEntryCount,
                    retainedPayloadBytes, accountingComplete);
        }

        private void accountSafely(ContentFile<?> file) {
            if (!accountRetainedSize || !accountingComplete) {
                return;
            }
            try {
                account(file);
            } catch (RuntimeException e) {
                // A new or third-party ContentFile implementation must not turn optional cache
                // accounting into a manifest-read failure. Keep the files for the current query
                // and mark the value incomplete so weighted admission rejects it.
                metricEntryCount = 0L;
                retainedPayloadBytes = 0L;
                accountingComplete = false;
            }
        }

        private void account(ContentFile<?> file) {
            metricEntryCount = MetaCacheWeightUtils.saturatedAdd(
                    metricEntryCount, metricEntryCount(file));
            retainedPayloadBytes = MetaCacheWeightUtils.saturatedAdd(
                    retainedPayloadBytes, retainedPayloadBytes(file));
        }
    }

    private static long metricEntryCount(ContentFile<?> file) {
        long count = mapSize(file.columnSizes());
        count = MetaCacheWeightUtils.saturatedAdd(count, mapSize(file.valueCounts()));
        count = MetaCacheWeightUtils.saturatedAdd(count, mapSize(file.nullValueCounts()));
        count = MetaCacheWeightUtils.saturatedAdd(count, mapSize(file.nanValueCounts()));
        count = MetaCacheWeightUtils.saturatedAdd(count, mapSize(file.lowerBounds()));
        return MetaCacheWeightUtils.saturatedAdd(count, mapSize(file.upperBounds()));
    }

    private static long retainedPayloadBytes(ContentFile<?> file) {
        long bytes = MetaCacheWeightUtils.estimatedCharSequenceBytes(file.path());
        bytes = addBuffer(bytes, file.keyMetadata());
        bytes = addBuffers(bytes, file.lowerBounds());
        bytes = addBuffers(bytes, file.upperBounds());
        bytes = addListEntries(bytes, file.splitOffsets());
        bytes = addListEntries(bytes, file.equalityFieldIds());
        if (file instanceof DeleteFile) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                    MetaCacheWeightUtils.estimatedStringBytes(
                            ((DeleteFile) file).referencedDataFile()));
        }
        return addPartitionPayload(bytes, file.partition());
    }

    private static long addBuffers(long bytes, Map<Integer, ByteBuffer> buffers) {
        if (buffers == null) {
            return bytes;
        }
        for (ByteBuffer buffer : buffers.values()) {
            bytes = addBuffer(bytes, buffer);
        }
        return bytes;
    }

    private static long addBuffer(long bytes, ByteBuffer buffer) {
        return buffer == null ? bytes : MetaCacheWeightUtils.saturatedAdd(bytes, buffer.capacity());
    }

    private static long addListEntries(long bytes, List<?> values) {
        if (values == null) {
            return bytes;
        }
        return MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.saturatedMultiply(
                        values.size(), AUXILIARY_LIST_ENTRY_BYTES));
    }

    private static long addPartitionPayload(long bytes, StructLike partition) {
        if (partition == null) {
            return bytes;
        }
        try {
            for (int index = 0; index < partition.size(); index++) {
                Object value = partition.get(index, Object.class);
                if (value instanceof CharSequence) {
                    bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                            MetaCacheWeightUtils.estimatedCharSequenceBytes((CharSequence) value));
                } else if (value instanceof ByteBuffer) {
                    bytes = addBuffer(bytes, (ByteBuffer) value);
                } else if (value instanceof byte[]) {
                    bytes = MetaCacheWeightUtils.saturatedAdd(bytes, ((byte[]) value).length);
                }
            }
        } catch (RuntimeException ignored) {
            // A third-party StructLike may reject Object.class. The fixed per-file allowance
            // remains conservative, and cache accounting must never fail manifest loading.
        }
        return bytes;
    }

    private static int mapSize(Map<?, ?> map) {
        return map == null ? 0 : map.size();
    }

}
