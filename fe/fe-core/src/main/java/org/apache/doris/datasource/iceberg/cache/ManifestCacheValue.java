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
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Cached manifest payload containing parsed files.
 */
public class ManifestCacheValue {
    private static final long AUXILIARY_LIST_ENTRY_BYTES =
            MetaCacheWeightUtils.estimatedObjectBytes(32L);
    // A copied PartitionData instance shares its partition type, Avro schema, and serialized
    // schema with the other files produced by one manifest reader. These constants are calibrated
    // against the production reuseContainers()+file.copy() graph in IcebergExternalMetaCacheTest.
    private static final long SHARED_PARTITION_BASE_BYTES = 1320L;
    private static final long SHARED_PARTITION_FIELD_BYTES = 904L;
    private static final long PARTITION_FIELD_NAME_RETENTION_COPIES = 2L;
    // Bound variable-payload traversal (bound entries plus partition values) independently of
    // manifest size. Values beyond this limit are rejected from a weighted cache instead of being
    // admitted with an underestimate, so it sits well above ordinary wide manifests: 10,000 files
    // with 200 lower/upper bounds each is 4,000,000 elements.
    private static final long MAX_DEEP_ACCOUNTING_ELEMENTS = 8_000_000L;
    // The per-file constants in IcebergCacheSizeEstimator describe the Iceberg 1.10.1 copies that
    // ManifestReader + ContentFile.copy() produce. Only those implementations, with their pinned
    // instance-field layouts, are accounted; anything else fails closed at build time.
    private static final String GENERIC_DATA_FILE_CLASS_NAME = "org.apache.iceberg.GenericDataFile";
    private static final String GENERIC_DELETE_FILE_CLASS_NAME =
            "org.apache.iceberg.GenericDeleteFile";
    private static final boolean CONTENT_FILE_LAYOUT_SUPPORTED = checkContentFileLayout();

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

    private static boolean checkContentFileLayout() {
        ClassLoader loader = ContentFile.class.getClassLoader();
        return MetaCacheWeightUtils.hasExpectedInstanceFields(GENERIC_DATA_FILE_CLASS_NAME, loader)
                && MetaCacheWeightUtils.hasExpectedInstanceFields(
                        GENERIC_DELETE_FILE_CLASS_NAME, loader)
                && MetaCacheWeightUtils.hasExpectedInstanceFields(
                        "org.apache.iceberg.BaseFile", loader,
                        "partitionType:StructType", "fileOrdinal:Long", "manifestLocation:String",
                        "partitionSpecId:int", "content:FileContent", "filePath:String",
                        "format:FileFormat", "partitionData:PartitionData", "recordCount:Long",
                        "fileSizeInBytes:long", "dataSequenceNumber:Long",
                        "fileSequenceNumber:Long", "columnSizes:Map", "valueCounts:Map",
                        "nullValueCounts:Map", "nanValueCounts:Map", "lowerBounds:Map",
                        "upperBounds:Map", "splitOffsets:long[]", "equalityIds:int[]",
                        "keyMetadata:byte[]", "sortOrderId:Integer", "firstRowId:Long",
                        "referencedDataFile:String", "contentOffset:Long",
                        "contentSizeInBytes:Long", "avroSchema:Schema")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(
                        "org.apache.iceberg.avro.SupportsIndexProjection", loader,
                        "fromProjectionPos:int[]")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(PartitionData.class,
                        "partitionType:StructType", "size:int", "data:Object[]",
                        "stringSchema:String", "schema:Schema");
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

    /** Accounts retained payload while the manifest reader builds the cached lists. */
    public static final class Builder {
        private final boolean dataContent;
        private final boolean accountRetainedSize;
        private final List<DataFile> dataFiles = new ArrayList<>();
        private final List<DeleteFile> deleteFiles = new ArrayList<>();
        private long metricEntryCount;
        private long retainedPayloadBytes;
        private long deepAccountingElements;
        private boolean accountingComplete;
        private final IdentityHashMap<Object, IdentityHashMap<Object, Boolean>>
                accountedPartitionSchemas = new IdentityHashMap<>();

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
            recordAccounting(file);
        }

        public void addDeleteFile(DeleteFile file) {
            if (dataContent) {
                throw new IllegalStateException("data manifest builder cannot accept a delete file");
            }
            deleteFiles.add(file);
            recordAccounting(file);
        }

        public ManifestCacheValue build() {
            return new ManifestCacheValue(dataFiles, deleteFiles,
                    dataContent ? metricEntryCount : 0L,
                    dataContent ? 0L : metricEntryCount,
                    retainedPayloadBytes, accountingComplete);
        }

        private void recordAccounting(ContentFile<?> file) {
            if (!accountRetainedSize || !accountingComplete) {
                return;
            }
            try {
                requireSupportedContentFile(file);
                StructLike partition = file.partition();
                long nextDeepElements = MetaCacheWeightUtils.saturatedAdd(
                        deepAccountingElements, deepAccountingElements(file, partition));
                if (nextDeepElements > MAX_DEEP_ACCOUNTING_ELEMENTS) {
                    rejectAccounting();
                    return;
                }
                deepAccountingElements = nextDeepElements;
                addAccounting(account(file, partition));
                accountPartitionOwnership(partition);
            } catch (RuntimeException | LinkageError e) {
                // A new or third-party ContentFile implementation must not turn optional cache
                // accounting into a manifest-read failure. Keep the files for the current query
                // and mark the value incomplete so weighted admission rejects it.
                rejectAccounting();
            }
        }

        private void requireSupportedContentFile(ContentFile<?> file) {
            if (!CONTENT_FILE_LAYOUT_SUPPORTED) {
                throw new IllegalStateException("unsupported Iceberg content file layout");
            }
            String expectedClassName = dataContent
                    ? GENERIC_DATA_FILE_CLASS_NAME : GENERIC_DELETE_FILE_CLASS_NAME;
            if (file == null || !expectedClassName.equals(file.getClass().getName())) {
                throw new IllegalStateException("unsupported Iceberg content file implementation: "
                        + (file == null ? "null" : file.getClass().getName()));
            }
        }

        private void addAccounting(FileAccounting accounting) {
            metricEntryCount = MetaCacheWeightUtils.saturatedAdd(
                    metricEntryCount, accounting.metricEntryCount);
            retainedPayloadBytes = MetaCacheWeightUtils.saturatedAdd(
                    retainedPayloadBytes, accounting.retainedPayloadBytes);
        }

        private void rejectAccounting() {
            metricEntryCount = 0L;
            retainedPayloadBytes = 0L;
            deepAccountingElements = 0L;
            accountingComplete = false;
        }

        private void accountPartitionOwnership(StructLike partition) {
            if (partition == null || partition.size() == 0) {
                return;
            }
            if (!(partition instanceof PartitionData)) {
                throw new IllegalArgumentException(
                        "unsupported Iceberg partition container: "
                                + partition.getClass().getName());
            }
            PartitionData partitionData = (PartitionData) partition;
            retainedPayloadBytes = MetaCacheWeightUtils.saturatedAdd(
                    retainedPayloadBytes, partitionInstanceBytes(partitionData));
            Object partitionTypeIdentity = partitionData.getPartitionType();
            // A reader-produced PartitionData carries its Avro schema; only a Java-deserialized
            // instance would rebuild it here (CPU only, no IO).
            Object schemaIdentity = partitionData.getSchema();
            IdentityHashMap<Object, Boolean> schemas = accountedPartitionSchemas.computeIfAbsent(
                    partitionTypeIdentity, ignored -> new IdentityHashMap<>());
            if (schemas.put(schemaIdentity, Boolean.TRUE) == null) {
                retainedPayloadBytes = MetaCacheWeightUtils.saturatedAdd(
                        retainedPayloadBytes,
                        sharedPartitionBytes(partitionData.getPartitionType()));
            }
        }
    }

    private static FileAccounting account(ContentFile<?> file, StructLike partition) {
        return new FileAccounting(
                metricEntryCount(file), retainedPayloadBytes(file, partition));
    }

    private static long deepAccountingElements(ContentFile<?> file, StructLike partition) {
        long elements = MetaCacheWeightUtils.saturatedAdd(
                mapSize(file.lowerBounds()), mapSize(file.upperBounds()));
        if (partition != null) {
            if (partition.size() < 0) {
                throw new IllegalArgumentException("negative Iceberg partition size");
            }
            elements = MetaCacheWeightUtils.saturatedAdd(elements, partition.size());
        }
        return elements;
    }

    private static final class FileAccounting {
        private final long metricEntryCount;
        private final long retainedPayloadBytes;

        private FileAccounting(long metricEntryCount, long retainedPayloadBytes) {
            this.metricEntryCount = metricEntryCount;
            this.retainedPayloadBytes = retainedPayloadBytes;
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

    private static long retainedPayloadBytes(ContentFile<?> file, StructLike partition) {
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
        return addPartitionPayload(bytes, partition);
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
        for (int index = 0; index < partition.size(); index++) {
            Object value = partition.get(index, Object.class);
            if (value instanceof CharSequence) {
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                        MetaCacheWeightUtils.estimatedCharSequenceBytes((CharSequence) value));
            } else if (value instanceof ByteBuffer) {
                bytes = addByteArray(bytes, ((ByteBuffer) value).capacity());
            } else if (value instanceof byte[]) {
                bytes = addByteArray(bytes, ((byte[]) value).length);
            } else if (value instanceof java.math.BigDecimal) {
                int bits = ((java.math.BigDecimal) value).unscaledValue().bitLength();
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                        MetaCacheWeightUtils.estimatedObjectBytes(96L));
                bytes = MetaCacheWeightUtils.saturatedAdd(
                        bytes, MetaCacheWeightUtils.estimatedIntArrayPayloadBytes(
                                (bits + 31L) / 32L));
            } else if (value instanceof java.util.UUID) {
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                        MetaCacheWeightUtils.estimatedObjectBytes(32L));
            } else if (value instanceof Long || value instanceof Double) {
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                        MetaCacheWeightUtils.estimatedObjectBytes(24L));
            } else if (value instanceof Number || value instanceof Boolean
                    || value instanceof Character) {
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                        MetaCacheWeightUtils.estimatedObjectBytes(16L));
            } else if (value != null) {
                throw new IllegalArgumentException(
                        "unsupported Iceberg partition value: " + value.getClass().getName());
            }
        }
        return bytes;
    }

    private static long addByteArray(long bytes, long payloadBytes) {
        return MetaCacheWeightUtils.saturatedAdd(
                bytes, MetaCacheWeightUtils.estimatedByteArrayBytes(payloadBytes));
    }

    private static long partitionInstanceBytes(PartitionData partition) {
        return MetaCacheWeightUtils.saturatedAdd(
                MetaCacheWeightUtils.estimatedObjectBytes(32L),
                MetaCacheWeightUtils.estimatedObjectArrayBytes(partition.size()));
    }

    private static long sharedPartitionBytes(Types.StructType partitionType) {
        long rawBytes = MetaCacheWeightUtils.saturatedAdd(
                SHARED_PARTITION_BASE_BYTES,
                MetaCacheWeightUtils.saturatedMultiply(
                        partitionType.fields().size(), SHARED_PARTITION_FIELD_BYTES));
        long bytes = MetaCacheWeightUtils.estimatedObjectBytes(rawBytes);
        for (Types.NestedField field : partitionType.fields()) {
            long payloadBytes = MetaCacheWeightUtils.estimatedStringPayloadBytes(field.name());
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                    MetaCacheWeightUtils.saturatedMultiply(
                            payloadBytes, PARTITION_FIELD_NAME_RETENTION_COPIES));
        }
        return bytes;
    }

    private static int mapSize(Map<?, ?> map) {
        return map == null ? 0 : map.size();
    }

}
