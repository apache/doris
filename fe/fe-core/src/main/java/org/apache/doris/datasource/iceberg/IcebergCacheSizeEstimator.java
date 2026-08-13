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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.iceberg.cache.ManifestCacheValue;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.Map;

/** Constant-time retained-weight formulas for Iceberg cache entries. */
final class IcebergCacheSizeEstimator {
    private static final long KEY_BASE_BYTES = 128L;
    private static final long TABLE_BASE_BYTES = 16L * 1024L;
    private static final long SCHEMA_VERSION_BYTES = 512L;
    private static final long SCHEMA_FIELD_BYTES = 512L;
    private static final long NESTED_SCHEMA_FIELD_BYTES = 512L;
    private static final long PARTITION_SPEC_BYTES = 256L;
    private static final long PARTITION_SPEC_FIELD_BYTES = 384L;
    private static final long SORT_ORDER_BYTES = 256L;
    private static final long SORT_FIELD_BYTES = 256L;
    private static final long TABLE_PROPERTY_BYTES = 256L;
    private static final long CURRENT_SNAPSHOT_BYTES = 512L;
    private static final long PARTITION_BYTES = 512L;
    private static final long PARTITION_ALIAS_BYTES = 256L;
    private static final long NAME_MAPPING_ENTRY_BYTES = 256L;
    private static final long MANIFEST_ENTRY_BASE_BYTES = 256L;
    private static final long DATA_FILE_BYTES = 16L * 1024L;
    private static final long DELETE_FILE_BYTES = 18L * 1024L;
    private static final long FILE_METRIC_ENTRY_BYTES = 160L;

    private IcebergCacheSizeEstimator() {
    }

    static MetaCacheSizeEstimate estimateTableEntry(NameMapping key, IcebergTableCacheValue value) {
        Table table = value.getRetainedIcebergTable();
        MetaCacheSizeEstimate support = checkSupportedTable(table);
        if (!support.isComplete()) {
            return support;
        }
        long bytes = MetaCacheWeightUtils.saturatedAdd(
                KEY_BASE_BYTES, MetaCacheWeightUtils.estimatedNameMappingBytes(key));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, estimateTable(table));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, value.getRetainedTablePayloadBytes());
        bytes = MetaCacheWeightUtils.saturatedAdd(
                bytes, value.getRetainedCurrentSnapshotPayloadBytes());
        return MetaCacheSizeEstimate.complete(bytes);
    }

    static MetaCacheSizeEstimate estimateSnapshotEntry(
            IcebergSnapshotEntryKey key, IcebergSnapshotCacheValue value) {
        long bytes = KEY_BASE_BYTES;
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedNameMappingBytes(key.getNameMapping()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedStringBytes(key.getTableUuid()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedStringBytes(key.getMetadataFileLocation()));

        IcebergPartitionInfo partitionInfo = value.getPartitionInfo();
        bytes = addCount(bytes, partitionInfo.getNameToPartitionItem().size(), PARTITION_BYTES);
        bytes = addCount(bytes, partitionInfo.getNameToIcebergPartition().size(), PARTITION_BYTES);
        bytes = addCount(bytes, partitionInfo.getNameToIcebergPartitionNames().size(), PARTITION_ALIAS_BYTES);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, partitionInfo.getRetainedPayloadBytes());
        bytes = addCount(bytes, value.getNameMapping().map(java.util.Map::size).orElse(0),
                NAME_MAPPING_ENTRY_BYTES);
        bytes = MetaCacheWeightUtils.saturatedAdd(
                bytes, value.getRetainedNameMappingPayloadBytes());

        if (value.getRetainedIcebergTable().isPresent()) {
            Table table = value.getRetainedIcebergTable().get();
            MetaCacheSizeEstimate support = checkSupportedTable(table);
            if (!support.isComplete()) {
                return support;
            }
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, estimateTable(table));
            bytes = MetaCacheWeightUtils.saturatedAdd(
                    bytes, value.getRetainedTablePayloadBytes());
            bytes = MetaCacheWeightUtils.saturatedAdd(
                    bytes, value.getRetainedCurrentSnapshotPayloadBytes());
        }
        return MetaCacheSizeEstimate.complete(bytes);
    }

    static MetaCacheSizeEstimate estimateManifestEntry(
            IcebergManifestEntryKey key, ManifestCacheValue value) {
        long bytes = MetaCacheWeightUtils.saturatedAdd(
                MANIFEST_ENTRY_BASE_BYTES,
                MetaCacheWeightUtils.estimatedStringBytes(key.getManifestPath()));
        bytes = addCount(bytes, value.getDataFiles().size(), DATA_FILE_BYTES);
        bytes = addCount(bytes, value.getDeleteFiles().size(), DELETE_FILE_BYTES);
        bytes = addCount(bytes, value.getDataFileMetricEntryCount(), FILE_METRIC_ENTRY_BYTES);
        bytes = addCount(bytes, value.getDeleteFileMetricEntryCount(), FILE_METRIC_ENTRY_BYTES);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, value.getRetainedPayloadBytes());
        return MetaCacheSizeEstimate.complete(bytes);
    }

    private static MetaCacheSizeEstimate checkSupportedTable(Table table) {
        if (table == null) {
            return MetaCacheSizeEstimate.incomplete("missing_iceberg_table");
        }
        if (!(table instanceof HasTableOperations)) {
            return MetaCacheSizeEstimate.incomplete(
                    "unsupported_iceberg_table:" + table.getClass().getName());
        }
        TableMetadata metadata = ((HasTableOperations) table).operations().current();
        if (metadata == null) {
            return MetaCacheSizeEstimate.incomplete("missing_iceberg_table_metadata");
        }
        if (metadata.metadataFileLocation() == null
                || metadata.metadataFileLocation().isEmpty()) {
            return MetaCacheSizeEstimate.incomplete("missing_iceberg_metadata_location");
        }
        return MetaCacheSizeEstimate.complete(1L);
    }

    /** Reads only metadata collection sizes and a constant number of strings; no FileIO is used. */
    private static long estimateTable(Table table) {
        TableMetadata metadata = ((HasTableOperations) table).operations().current();
        long bytes = MetaCacheWeightUtils.saturatedAdd(
                TABLE_BASE_BYTES, MetaCacheWeightUtils.estimatedStringBytes(table.name()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedStringBytes(metadata.location()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedStringBytes(metadata.metadataFileLocation()));

        bytes = addCount(bytes, metadata.properties().size(), TABLE_PROPERTY_BYTES);
        if (metadata.currentSnapshot() != null) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, CURRENT_SNAPSHOT_BYTES);
        }
        return bytes;
    }

    /** Captures exact historical cardinalities and skew-sensitive payload once before admission. */
    static long retainedTablePayloadBytes(Table table) {
        if (!(table instanceof HasTableOperations)) {
            return 0L;
        }
        TableMetadata metadata = ((HasTableOperations) table).operations().current();
        if (metadata == null) {
            return 0L;
        }

        long bytes = 0L;
        for (Schema schema : metadata.schemas()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, SCHEMA_VERSION_BYTES);
            for (Types.NestedField field : schema.columns()) {
                bytes = addFieldPayload(bytes, field, false);
            }
        }
        for (PartitionSpec spec : metadata.specs()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, PARTITION_SPEC_BYTES);
            for (org.apache.iceberg.PartitionField field : spec.fields()) {
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes, PARTITION_SPEC_FIELD_BYTES);
                bytes = addString(bytes, field.name());
            }
        }
        for (SortOrder sortOrder : metadata.sortOrders()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, SORT_ORDER_BYTES);
            bytes = addCount(bytes, sortOrder.fields().size(), SORT_FIELD_BYTES);
        }
        for (Map.Entry<String, String> property : metadata.properties().entrySet()) {
            bytes = addString(bytes, property.getKey());
            bytes = addString(bytes, property.getValue());
        }
        bytes = addString(bytes, metadata.uuid());
        return bytes;
    }

    private static long addFieldPayload(long bytes, Types.NestedField field, boolean nested) {
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                nested ? NESTED_SCHEMA_FIELD_BYTES : SCHEMA_FIELD_BYTES);
        bytes = addString(bytes, field.name());
        bytes = addString(bytes, field.doc());
        bytes = addDefaultPayload(bytes, field.initialDefault());
        bytes = addDefaultPayload(bytes, field.writeDefault());
        return addTypePayload(bytes, field.type());
    }

    private static long addTypePayload(long bytes, Type type) {
        if (type.isStructType()) {
            for (Types.NestedField field : type.asStructType().fields()) {
                bytes = addFieldPayload(bytes, field, true);
            }
        } else if (type.isListType()) {
            bytes = addTypePayload(bytes, type.asListType().elementType());
        } else if (type.isMapType()) {
            bytes = addTypePayload(bytes, type.asMapType().keyType());
            bytes = addTypePayload(bytes, type.asMapType().valueType());
        }
        return bytes;
    }

    private static long addDefaultPayload(long bytes, Object value) {
        if (value instanceof CharSequence) {
            return MetaCacheWeightUtils.saturatedAdd(bytes,
                    MetaCacheWeightUtils.estimatedCharSequenceBytes((CharSequence) value));
        } else if (value instanceof ByteBuffer) {
            return MetaCacheWeightUtils.saturatedAdd(bytes, ((ByteBuffer) value).capacity());
        } else if (value instanceof byte[]) {
            return MetaCacheWeightUtils.saturatedAdd(bytes, ((byte[]) value).length);
        }
        return bytes;
    }

    private static long addString(long bytes, String value) {
        return MetaCacheWeightUtils.saturatedAdd(
                bytes, MetaCacheWeightUtils.estimatedStringBytes(value));
    }

    private static long addStringMap(long bytes, Map<String, String> values, long entryBytes) {
        if (values == null) {
            return bytes;
        }
        bytes = addCount(bytes, values.size(), entryBytes);
        for (Map.Entry<String, String> entry : values.entrySet()) {
            bytes = addString(bytes, entry.getKey());
            bytes = addString(bytes, entry.getValue());
        }
        return bytes;
    }

    private static long addCount(long bytes, long count, long bytesPerItem) {
        return MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.saturatedMultiply(count, bytesPerItem));
    }

}
