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

import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.iceberg.IcebergPartitionCache.CachedPartitions;
import org.apache.doris.connector.iceberg.IcebergPartitionCache.Key;
import org.apache.doris.connector.iceberg.IcebergPartitionUtils.IcebergRawPartition;
import org.apache.doris.connector.iceberg.IcebergTableCache.CachedTable;

import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UnboundPartitionSpec;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Type-specific retained-heap estimators for the large Iceberg connector cache values. */
final class IcebergCacheSizeEstimator {
    private static final String[] CONTENT_FILE_FIELD_NAMES = {
            "content", "file_path", "file_format", "partition", "record_count", "file_size_in_bytes",
            "column_sizes", "value_counts", "null_value_counts", "nan_value_counts", "lower_bounds",
            "upper_bounds", "key_metadata", "split_offsets", "equality_ids", "sort_order_id", "first_row_id",
            "referenced_data_file", "content_offset", "content_size_in_bytes"
    };
    private static final long TABLE_IDENTIFIER_SHALLOW_BYTES = JvmSizeUtils.instanceSize(TableIdentifier.class);
    private static final long CACHED_TABLE_SHALLOW_BYTES = JvmSizeUtils.instanceSize(CachedTable.class);
    private static final long TABLE_METADATA_SHALLOW_BYTES = JvmSizeUtils.instanceSize(TableMetadata.class);
    private static final long PARTITION_KEY_SHALLOW_BYTES = JvmSizeUtils.instanceSize(Key.class);
    private static final long CACHED_PARTITIONS_SHALLOW_BYTES = JvmSizeUtils.instanceSize(CachedPartitions.class);
    private static final long RAW_PARTITION_SHALLOW_BYTES = JvmSizeUtils.instanceSize(IcebergRawPartition.class);
    private static final long MANIFEST_KEY_SHALLOW_BYTES = JvmSizeUtils.instanceSize(IcebergManifestEntryKey.class);
    private static final long MANIFEST_VALUE_SHALLOW_BYTES = JvmSizeUtils.instanceSize(ManifestCacheValue.class);
    private static final long INTEGER_SHALLOW_BYTES = JvmSizeUtils.instanceSize(Integer.class);
    private static final long LONG_SHALLOW_BYTES = JvmSizeUtils.instanceSize(Long.class);
    private static final long HASH_MAP_NODE_SHALLOW_BYTES = classSize("java.util.HashMap$Node");
    private static final long LINKED_HASH_MAP_ENTRY_SHALLOW_BYTES = classSize("java.util.LinkedHashMap$Entry");
    private static final long CONTENT_FILE_SCHEMA_BYTES = estimateContentFileSchema();

    private IcebergCacheSizeEstimator() {
    }

    /** Caffeine callback: the expensive table graph was sized once when {@link CachedTable} was constructed. */
    static long estimateTableEntry(TableIdentifier key, CachedTable value) {
        return add(estimateTableIdentifier(key), value.estimatedBytes);
    }

    static long estimateTable(Table table) {
        long bytes = add(CACHED_TABLE_SHALLOW_BYTES, JvmSizeUtils.instanceSize(table.getClass()));
        bytes = add(bytes, JvmSizeUtils.stringSize(table.name()));
        if (!(table instanceof HasTableOperations)) {
            bytes = add(bytes, JvmSizeUtils.stringSize(table.location()));
            return add(bytes, estimateStringMap(table.properties()));
        }

        TableOperations operations = ((HasTableOperations) table).operations();
        bytes = add(bytes, JvmSizeUtils.instanceSize(operations.getClass()));
        return add(bytes, estimateTableMetadata(operations.current()));
    }

    static long estimatePartitionKey(Key key) {
        return add(PARTITION_KEY_SHALLOW_BYTES, estimateTableIdentifier(key.id));
    }

    static long estimatePartitions(List<IcebergRawPartition> partitions) {
        long bytes = CACHED_PARTITIONS_SHALLOW_BYTES;
        // CachedPartitions owns an unmodifiable wrapper around an exact-size ArrayList copy.
        bytes = add(bytes, JvmSizeUtils.instanceSize(partitions.getClass()));
        bytes = add(bytes, JvmSizeUtils.arrayListSize(partitions.size()));
        for (IcebergRawPartition partition : partitions) {
            bytes = add(bytes, RAW_PARTITION_SHALLOW_BYTES);
            bytes = add(bytes, JvmSizeUtils.stringSize(partition.name));
            bytes = add(bytes, estimateStringList(partition.columnNames));
            bytes = add(bytes, estimateStringList(partition.values));
            bytes = add(bytes, estimateStringList(partition.transforms));
        }
        return bytes;
    }

    /** Caffeine callback: the partition list was sized once when {@link CachedPartitions} was constructed. */
    static long estimatePartitionEntry(Key key, CachedPartitions value) {
        return add(key.estimatedBytes, value.estimatedBytes);
    }

    static long estimateManifestKey(IcebergManifestEntryKey key) {
        return add(MANIFEST_KEY_SHALLOW_BYTES, JvmSizeUtils.stringSize(key.getManifestPath()));
    }

    static long estimateManifestValue(ManifestCacheValue value) {
        long bytes = MANIFEST_VALUE_SHALLOW_BYTES;
        bytes = add(bytes, estimateContentFileList(value.getDataFiles()));
        return add(bytes, estimateContentFileList(value.getDeleteFiles()));
    }

    /** Caffeine callback: key and manifest payload sizes are precomputed during construction. */
    static long estimateManifestEntry(IcebergManifestEntryKey key, ManifestCacheValue value) {
        return add(key.getEstimatedBytes(), value.getEstimatedBytes());
    }

    private static long estimateTableIdentifier(TableIdentifier identifier) {
        long bytes = TABLE_IDENTIFIER_SHALLOW_BYTES;
        Namespace namespace = identifier.namespace();
        bytes = add(bytes, JvmSizeUtils.instanceSize(namespace.getClass()));
        String[] levels = namespace.levels();
        bytes = add(bytes, JvmSizeUtils.objectArraySize(levels.length));
        for (String level : levels) {
            bytes = add(bytes, JvmSizeUtils.stringSize(level));
        }
        return add(bytes, JvmSizeUtils.stringSize(identifier.name()));
    }

    private static long estimateTableMetadata(TableMetadata metadata) {
        long bytes = TABLE_METADATA_SHALLOW_BYTES;
        bytes = add(bytes, JvmSizeUtils.stringSize(metadata.metadataFileLocation()));
        bytes = add(bytes, JvmSizeUtils.stringSize(metadata.uuid()));
        bytes = add(bytes, JvmSizeUtils.stringSize(metadata.location()));
        bytes = add(bytes, estimateStringMap(metadata.properties()));

        List<Schema> schemas = metadata.schemas();
        bytes = add(bytes, estimateListStructure(schemas));
        bytes = add(bytes, estimateIntegerIndexMap(metadata.schemasById()));
        for (Schema schema : schemas) {
            bytes = add(bytes, estimateSchema(schema));
        }

        List<PartitionSpec> specs = metadata.specs();
        bytes = add(bytes, estimateListStructure(specs));
        bytes = add(bytes, estimateIntegerIndexMap(metadata.specsById()));
        for (PartitionSpec spec : specs) {
            bytes = add(bytes, estimatePartitionSpec(spec));
        }

        List<SortOrder> sortOrders = metadata.sortOrders();
        bytes = add(bytes, estimateListStructure(sortOrders));
        bytes = add(bytes, estimateIntegerIndexMap(metadata.sortOrdersById()));
        for (SortOrder sortOrder : sortOrders) {
            bytes = add(bytes, estimateSortOrder(sortOrder));
        }

        List<Snapshot> snapshots = metadata.snapshots();
        bytes = add(bytes, estimateListStructure(snapshots));
        bytes = add(bytes, estimateLongIndexMap(snapshots));
        for (Snapshot snapshot : snapshots) {
            bytes = add(bytes, estimateSnapshot(snapshot));
        }

        bytes = add(bytes, estimateHistory(metadata.snapshotLog()));
        bytes = add(bytes, estimateMetadataLog(metadata.previousFiles()));
        bytes = add(bytes, estimateSnapshotRefs(metadata.refs()));
        bytes = add(bytes, estimateStatisticsFiles(metadata.statisticsFiles()));
        bytes = add(bytes, estimatePartitionStatisticsFiles(metadata.partitionStatisticsFiles()));
        bytes = add(bytes, estimateMetadataUpdates(metadata.changes()));
        bytes = add(bytes, estimateShallowList(metadata.encryptionKeys()));
        // TableMetadata retains a serializable snapshot supplier after the immutable snapshot list is loaded.
        return add(bytes, JvmSizeUtils.objectArraySize(1));
    }

    private static long estimateSchema(Schema schema) {
        List<Types.NestedField> columns = schema.columns();
        long bytes = JvmSizeUtils.instanceSize(schema.getClass());
        bytes = add(bytes, JvmSizeUtils.instanceSize(schema.asStruct().getClass()));
        bytes = add(bytes, estimateListStructure(columns));
        for (Types.NestedField field : columns) {
            bytes = add(bytes, estimateNestedField(field));
        }
        bytes = add(bytes, JvmSizeUtils.objectArraySize(schema.identifierFieldIds().size()));
        bytes = add(bytes, estimateMapStructure(schema.getAliases()));
        return add(bytes, estimateSchemaIndexes(columns.size()));
    }

    private static long estimateNestedField(Types.NestedField field) {
        long bytes = JvmSizeUtils.instanceSize(field.getClass());
        bytes = add(bytes, JvmSizeUtils.stringSize(field.name()));
        bytes = add(bytes, JvmSizeUtils.stringSize(field.doc()));
        return add(bytes, estimateIcebergType(field.type()));
    }

    private static long estimateIcebergType(Type type) {
        long bytes = JvmSizeUtils.instanceSize(type.getClass());
        if (type.isStructType()) {
            List<Types.NestedField> fields = type.asStructType().fields();
            bytes = add(bytes, estimateListStructure(fields));
            for (Types.NestedField field : fields) {
                bytes = add(bytes, estimateNestedField(field));
            }
        } else if (type.isListType()) {
            bytes = add(bytes, estimateNestedField(type.asListType().fields().get(0)));
        } else if (type.isMapType()) {
            for (Types.NestedField field : type.asMapType().fields()) {
                bytes = add(bytes, estimateNestedField(field));
            }
        }
        return bytes;
    }

    private static long estimatePartitionSpec(PartitionSpec spec) {
        List<PartitionField> fields = spec.fields();
        long bytes = add(JvmSizeUtils.instanceSize(spec.getClass()), JvmSizeUtils.objectArraySize(fields.size()));
        for (PartitionField field : fields) {
            bytes = add(bytes, JvmSizeUtils.instanceSize(field.getClass()));
            bytes = add(bytes, JvmSizeUtils.stringSize(field.name()));
            bytes = add(bytes, JvmSizeUtils.instanceSize(field.transform().getClass()));
        }
        return bytes;
    }

    private static long estimateSortOrder(SortOrder sortOrder) {
        List<SortField> fields = sortOrder.fields();
        long bytes = add(JvmSizeUtils.instanceSize(sortOrder.getClass()), JvmSizeUtils.objectArraySize(fields.size()));
        for (SortField field : fields) {
            bytes = add(bytes, JvmSizeUtils.instanceSize(field.getClass()));
            bytes = add(bytes, JvmSizeUtils.instanceSize(field.transform().getClass()));
        }
        return bytes;
    }

    private static long estimateSnapshot(Snapshot snapshot) {
        long bytes = JvmSizeUtils.instanceSize(snapshot.getClass());
        bytes = add(bytes, estimateBoxed(snapshot.parentId(), LONG_SHALLOW_BYTES));
        bytes = add(bytes, estimateBoxed(snapshot.schemaId(), INTEGER_SHALLOW_BYTES));
        bytes = add(bytes, estimateBoxed(snapshot.firstRowId(), LONG_SHALLOW_BYTES));
        bytes = add(bytes, estimateBoxed(snapshot.addedRows(), LONG_SHALLOW_BYTES));
        bytes = add(bytes, JvmSizeUtils.stringSize(snapshot.operation()));
        bytes = add(bytes, JvmSizeUtils.stringSize(snapshot.manifestListLocation()));
        bytes = add(bytes, JvmSizeUtils.stringSize(snapshot.keyId()));
        return add(bytes, estimateStringMap(snapshot.summary()));
    }

    private static long estimateHistory(List<HistoryEntry> history) {
        long bytes = estimateListStructure(history);
        for (HistoryEntry entry : history) {
            bytes = add(bytes, JvmSizeUtils.instanceSize(entry.getClass()));
        }
        return bytes;
    }

    private static long estimateMetadataLog(List<TableMetadata.MetadataLogEntry> entries) {
        long bytes = estimateListStructure(entries);
        for (TableMetadata.MetadataLogEntry entry : entries) {
            bytes = add(bytes, JvmSizeUtils.instanceSize(entry.getClass()));
            bytes = add(bytes, JvmSizeUtils.stringSize(entry.file()));
        }
        return bytes;
    }

    private static long estimateSnapshotRefs(Map<String, SnapshotRef> refs) {
        long bytes = estimateMapStructure(refs);
        for (Map.Entry<String, SnapshotRef> entry : refs.entrySet()) {
            bytes = add(bytes, JvmSizeUtils.stringSize(entry.getKey()));
            SnapshotRef ref = entry.getValue();
            bytes = add(bytes, JvmSizeUtils.instanceSize(ref.getClass()));
            bytes = add(bytes, estimateBoxed(ref.minSnapshotsToKeep(), INTEGER_SHALLOW_BYTES));
            bytes = add(bytes, estimateBoxed(ref.maxSnapshotAgeMs(), LONG_SHALLOW_BYTES));
            bytes = add(bytes, estimateBoxed(ref.maxRefAgeMs(), LONG_SHALLOW_BYTES));
        }
        return bytes;
    }

    private static long estimateStatisticsFiles(List<StatisticsFile> files) {
        long bytes = estimateListStructure(files);
        for (StatisticsFile file : files) {
            bytes = add(bytes, JvmSizeUtils.instanceSize(file.getClass()));
            bytes = add(bytes, JvmSizeUtils.stringSize(file.path()));
            List<BlobMetadata> blobs = file.blobMetadata();
            bytes = add(bytes, estimateListStructure(blobs));
            for (BlobMetadata blob : blobs) {
                bytes = add(bytes, JvmSizeUtils.instanceSize(blob.getClass()));
                bytes = add(bytes, JvmSizeUtils.stringSize(blob.type()));
                bytes = add(bytes, estimateBoxedList(blob.fields(), INTEGER_SHALLOW_BYTES));
                bytes = add(bytes, estimateStringMap(blob.properties()));
            }
        }
        return bytes;
    }

    private static long estimatePartitionStatisticsFiles(List<PartitionStatisticsFile> files) {
        long bytes = estimateListStructure(files);
        for (PartitionStatisticsFile file : files) {
            bytes = add(bytes, JvmSizeUtils.instanceSize(file.getClass()));
            bytes = add(bytes, JvmSizeUtils.stringSize(file.path()));
        }
        return bytes;
    }

    private static long estimateShallowList(List<?> values) {
        long bytes = estimateListStructure(values);
        for (Object value : values) {
            bytes = add(bytes, JvmSizeUtils.instanceSize(value.getClass()));
        }
        return bytes;
    }

    private static long estimateMetadataUpdates(List<MetadataUpdate> updates) {
        long bytes = estimateListStructure(updates);
        for (MetadataUpdate update : updates) {
            bytes = add(bytes, JvmSizeUtils.instanceSize(update.getClass()));
            if (update instanceof MetadataUpdate.SetProperties) {
                bytes = add(bytes, estimateMapStructure(((MetadataUpdate.SetProperties) update).updated()));
            } else if (update instanceof MetadataUpdate.AddPartitionSpec) {
                UnboundPartitionSpec spec = ((MetadataUpdate.AddPartitionSpec) update).spec();
                bytes = add(bytes, JvmSizeUtils.instanceSize(spec.getClass()));
                bytes = add(bytes, estimateListStructure(spec.fields()));
            } else if (update instanceof MetadataUpdate.AddSortOrder) {
                bytes = add(bytes, JvmSizeUtils.instanceSize(
                        ((MetadataUpdate.AddSortOrder) update).sortOrder().getClass()));
            }
        }
        return bytes;
    }

    private static long estimateContentFileList(List<? extends ContentFile<?>> files) {
        if (files.isEmpty()) {
            return 0L;
        }
        long bytes = add(estimateListStructure(files), CONTENT_FILE_SCHEMA_BYTES);
        Set<Object> ownedObjects = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        for (ContentFile<?> file : files) {
            bytes = add(bytes, estimateContentFile(file, ownedObjects));
        }
        return bytes;
    }

    private static long estimateContentFileSchema() {
        long bytes = JvmSizeUtils.instanceSize(Types.StructType.class);
        bytes = add(bytes, JvmSizeUtils.arrayListSize(CONTENT_FILE_FIELD_NAMES.length));
        for (String name : CONTENT_FILE_FIELD_NAMES) {
            bytes = add(bytes, JvmSizeUtils.instanceSize(Types.NestedField.class));
            bytes = add(bytes, JvmSizeUtils.stringSize(name));
        }
        return bytes;
    }

    private static long estimateContentFile(ContentFile<?> file, Set<Object> ownedObjects) {
        long bytes = JvmSizeUtils.instanceSize(file.getClass());
        if (file instanceof StructLike) {
            bytes = add(bytes, JvmSizeUtils.intArraySize(((StructLike) file).size()));
            bytes = add(bytes, LONG_SHALLOW_BYTES);
        }
        bytes = add(bytes, estimateOwnedCharSequence(file.path(), ownedObjects));
        bytes = add(bytes, estimateOwnedString(file.manifestLocation(), ownedObjects));
        bytes = add(bytes, estimatePartition(file.partition(), ownedObjects));
        bytes = add(bytes, estimateLongMap(file.columnSizes()));
        bytes = add(bytes, estimateLongMap(file.valueCounts()));
        bytes = add(bytes, estimateLongMap(file.nullValueCounts()));
        bytes = add(bytes, estimateLongMap(file.nanValueCounts()));
        bytes = add(bytes, estimateByteBufferMap(file.lowerBounds()));
        bytes = add(bytes, estimateByteBufferMap(file.upperBounds()));
        ByteBuffer keyMetadata = file.keyMetadata();
        if (keyMetadata != null) {
            bytes = add(bytes, JvmSizeUtils.byteArraySize(keyMetadata.remaining()));
        }
        List<Long> splitOffsets = file.splitOffsets();
        if (splitOffsets != null) {
            bytes = add(bytes, JvmSizeUtils.longArraySize(splitOffsets.size()));
        }
        List<Integer> equalityFieldIds = file.equalityFieldIds();
        if (equalityFieldIds != null) {
            bytes = add(bytes, JvmSizeUtils.intArraySize(equalityFieldIds.size()));
        }
        bytes = add(bytes, estimateBoxed(file.pos(), LONG_SHALLOW_BYTES));
        bytes = add(bytes, estimateBoxed(file.sortOrderId(), INTEGER_SHALLOW_BYTES));
        bytes = add(bytes, estimateBoxed(file.dataSequenceNumber(), LONG_SHALLOW_BYTES));
        bytes = add(bytes, estimateBoxed(file.fileSequenceNumber(), LONG_SHALLOW_BYTES));
        bytes = add(bytes, estimateBoxed(file.firstRowId(), LONG_SHALLOW_BYTES));
        if (file instanceof DeleteFile) {
            DeleteFile deleteFile = (DeleteFile) file;
            bytes = add(bytes, estimateOwnedString(deleteFile.referencedDataFile(), ownedObjects));
            bytes = add(bytes, estimateBoxed(deleteFile.contentOffset(), LONG_SHALLOW_BYTES));
            bytes = add(bytes, estimateBoxed(deleteFile.contentSizeInBytes(), LONG_SHALLOW_BYTES));
        }
        return bytes;
    }

    private static long estimatePartition(StructLike partition, Set<Object> ownedObjects) {
        if (partition == null || !ownedObjects.add(partition)) {
            return 0L;
        }
        long bytes = JvmSizeUtils.instanceSize(partition.getClass());
        bytes = add(bytes, JvmSizeUtils.objectArraySize(partition.size()));
        for (int i = 0; i < partition.size(); i++) {
            bytes = add(bytes, estimateOwnedScalar(partition.get(i, Object.class), ownedObjects));
        }
        return bytes;
    }

    private static long estimateOwnedScalar(Object value, Set<Object> ownedObjects) {
        if (value == null || !ownedObjects.add(value)) {
            return 0L;
        }
        if (value instanceof CharSequence) {
            return estimateCharSequence((CharSequence) value);
        }
        if (value instanceof ByteBuffer) {
            return estimateByteBuffer((ByteBuffer) value);
        }
        return JvmSizeUtils.instanceSize(value.getClass());
    }

    private static long estimateOwnedCharSequence(CharSequence value, Set<Object> ownedObjects) {
        return value == null || !ownedObjects.add(value) ? 0L : estimateCharSequence(value);
    }

    private static long estimateOwnedString(String value, Set<? super String> ownedStrings) {
        return value == null || !ownedStrings.add(value) ? 0L : JvmSizeUtils.stringSize(value);
    }

    private static long estimateCharSequence(CharSequence value) {
        if (value instanceof String) {
            return JvmSizeUtils.stringSize((String) value);
        }
        return add(JvmSizeUtils.instanceSize(value.getClass()), JvmSizeUtils.stringSize(value.toString()));
    }

    private static long estimateLongMap(Map<Integer, Long> values) {
        if (values == null || values.isEmpty()) {
            return 0L;
        }
        return add(estimateMapStructure(values), multiply(values.size(), INTEGER_SHALLOW_BYTES + LONG_SHALLOW_BYTES));
    }

    private static long estimateByteBufferMap(Map<Integer, ByteBuffer> values) {
        if (values == null || values.isEmpty()) {
            return 0L;
        }
        long bytes = add(estimateMapStructure(values), multiply(values.size(), INTEGER_SHALLOW_BYTES));
        for (ByteBuffer value : values.values()) {
            bytes = add(bytes, estimateByteBuffer(value));
        }
        return bytes;
    }

    private static long estimateByteBuffer(ByteBuffer value) {
        if (value == null) {
            return 0L;
        }
        long bytes = JvmSizeUtils.instanceSize(value.getClass());
        return value.hasArray() ? add(bytes, JvmSizeUtils.byteArraySize(value.capacity())) : bytes;
    }

    private static long estimateSchemaIndexes(int fieldCount) {
        if (fieldCount == 0) {
            return 0L;
        }
        long oneIndex = JvmSizeUtils.instanceSize(HashMap.class);
        oneIndex = add(oneIndex, JvmSizeUtils.objectArraySize(hashCapacity(fieldCount)));
        oneIndex = add(oneIndex, multiply(fieldCount, HASH_MAP_NODE_SHALLOW_BYTES));
        long bytes = multiply(3L, oneIndex);
        return add(bytes, multiply(2L * fieldCount, INTEGER_SHALLOW_BYTES));
    }

    private static long estimateIntegerIndexMap(Map<Integer, ?> values) {
        return add(estimateMapStructure(values), multiply(values.size(), INTEGER_SHALLOW_BYTES));
    }

    private static long estimateLongIndexMap(List<?> values) {
        if (values.isEmpty()) {
            return JvmSizeUtils.instanceSize(HashMap.class);
        }
        long bytes = JvmSizeUtils.instanceSize(HashMap.class);
        bytes = add(bytes, JvmSizeUtils.objectArraySize(hashCapacity(values.size())));
        bytes = add(bytes, multiply(values.size(), HASH_MAP_NODE_SHALLOW_BYTES));
        return add(bytes, multiply(values.size(), LONG_SHALLOW_BYTES));
    }

    private static long estimateStringMap(Map<String, String> values) {
        if (values == null) {
            return 0L;
        }
        long bytes = estimateMapStructure(values);
        for (Map.Entry<String, String> entry : values.entrySet()) {
            bytes = add(bytes, JvmSizeUtils.stringSize(entry.getKey()));
            bytes = add(bytes, JvmSizeUtils.stringSize(entry.getValue()));
        }
        return bytes;
    }

    private static long estimateStringList(List<String> values) {
        if (values == null || values.isEmpty()) {
            return 0L;
        }
        long bytes = estimateListStructure(values);
        for (String value : values) {
            bytes = add(bytes, JvmSizeUtils.stringSize(value));
        }
        return bytes;
    }

    private static long estimateBoxedList(List<?> values, long elementBytes) {
        if (values == null || values.isEmpty()) {
            return 0L;
        }
        return add(estimateListStructure(values), multiply(values.size(), elementBytes));
    }

    private static long estimateBoxed(Object value, long bytes) {
        return value == null ? 0L : bytes;
    }

    private static long estimateListStructure(List<?> values) {
        int capacity = values instanceof ArrayList ? arrayListCapacity(values.size()) : values.size();
        long bytes = JvmSizeUtils.instanceSize(values.getClass());
        if (values.getClass().getName().equals("java.util.ImmutableCollections$List12")) {
            return bytes;
        }
        return add(bytes, JvmSizeUtils.objectArraySize(capacity));
    }

    private static long estimateMapStructure(Map<?, ?> values) {
        if (values == null) {
            return 0L;
        }
        long bytes = JvmSizeUtils.instanceSize(values.getClass());
        if (values.isEmpty()) {
            return bytes;
        }
        if (values instanceof HashMap) {
            int capacity = hashCapacity(values.size());
            long nodeBytes = values instanceof LinkedHashMap
                    ? LINKED_HASH_MAP_ENTRY_SHALLOW_BYTES
                    : HASH_MAP_NODE_SHALLOW_BYTES;
            bytes = add(bytes, JvmSizeUtils.objectArraySize(capacity));
            return add(bytes, multiply(values.size(), nodeBytes));
        }
        bytes = add(bytes, JvmSizeUtils.objectArraySize(saturatedDouble(values.size())));
        return add(bytes, multiply(values.size(), HASH_MAP_NODE_SHALLOW_BYTES));
    }

    private static int arrayListCapacity(int size) {
        if (size == 0) {
            return 0;
        }
        int capacity = 10;
        while (capacity < size) {
            int grown = capacity + (capacity >> 1);
            if (grown < 0) {
                return Integer.MAX_VALUE;
            }
            capacity = grown;
        }
        return capacity;
    }

    private static int hashCapacity(int size) {
        if (size == 0) {
            return 0;
        }
        long needed = (size * 4L + 2L) / 3L;
        int capacity = 16;
        while (capacity < needed && capacity < 1 << 30) {
            capacity <<= 1;
        }
        return capacity;
    }

    private static int saturatedDouble(int value) {
        return value > Integer.MAX_VALUE / 2 ? Integer.MAX_VALUE : value * 2;
    }

    private static long multiply(long left, long right) {
        return JvmSizeUtils.saturatedMultiply(left, right);
    }

    private static long classSize(String className) {
        try {
            return JvmSizeUtils.instanceSize(Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Required JVM collection class is missing: " + className, e);
        }
    }

    private static long add(long left, long right) {
        return JvmSizeUtils.saturatedAdd(left, right);
    }
}
