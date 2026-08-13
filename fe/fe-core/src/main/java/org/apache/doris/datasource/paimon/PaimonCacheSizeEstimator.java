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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;

import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;

/** Constant-time retained-weight formula for Paimon snapshot projections. */
final class PaimonCacheSizeEstimator {
    private static final long KEY_BASE_BYTES = 128L;
    private static final long SNAPSHOT_BASE_BYTES = 4L * 1024L;
    private static final long TABLE_BASE_BYTES = 16L * 1024L;
    private static final long TABLE_FIELD_BYTES = 3584L;
    private static final long TABLE_OPTION_BYTES = 256L;
    private static final long TABLE_KEY_BYTES = 128L;
    private static final long NESTED_FIELD_BYTES = 512L;
    private static final long PARTITION_BYTES = 1280L;
    private static final long PARTITION_ITEM_BYTES = 1024L;
    private static final long WRAPPER_BYTES = 512L;

    private PaimonCacheSizeEstimator() {
    }

    static MetaCacheSizeEstimate estimateSnapshotEntry(
            PaimonSnapshotEntryKey key, PaimonSnapshotCacheValue value) {
        Table table = value.getSnapshot().getTable();
        if (!isSupportedTable(table)) {
            return MetaCacheSizeEstimate.incomplete("unsupported_paimon_table:"
                    + (table == null ? "null" : table.getClass().getName()));
        }

        long bytes = MetaCacheWeightUtils.saturatedAdd(
                KEY_BASE_BYTES, MetaCacheWeightUtils.estimatedNameMappingBytes(key.getNameMapping()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, SNAPSHOT_BASE_BYTES);
        bytes = addCount(bytes, value.getPartitionInfo().getNameToPartition().size(), PARTITION_BYTES);
        bytes = addCount(bytes, value.getPartitionInfo().getNameToPartitionItem().size(), PARTITION_ITEM_BYTES);
        bytes = MetaCacheWeightUtils.saturatedAdd(
                bytes, value.getPartitionInfo().getRetainedPayloadBytes());
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, value.getRetainedTablePayloadBytes());
        return MetaCacheSizeEstimate.complete(
                MetaCacheWeightUtils.saturatedAdd(bytes, estimateTable(table)));
    }

    private static boolean isSupportedTable(Table table) {
        if (table instanceof PrivilegedFileStoreTable) {
            return isSupportedTable(((PrivilegedFileStoreTable) table).wrapped());
        }
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable fallback = (FallbackReadFileStoreTable) table;
            return isSupportedTable(fallback.wrapped()) && isSupportedTable(fallback.other());
        }
        if (!(table instanceof FileStoreTable)) {
            return false;
        }
        String className = table.getClass().getName();
        return "org.apache.paimon.table.AppendOnlyFileStoreTable".equals(className)
                || "org.apache.paimon.table.PrimaryKeyFileStoreTable".equals(className);
    }

    /** Uses TableSchema cardinalities only and deliberately never calls FileStoreTable.store(). */
    private static long estimateTable(Table table) {
        if (table instanceof PrivilegedFileStoreTable) {
            return MetaCacheWeightUtils.saturatedAdd(WRAPPER_BYTES,
                    estimateTable(((PrivilegedFileStoreTable) table).wrapped()));
        }
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable fallback = (FallbackReadFileStoreTable) table;
            long bytes = MetaCacheWeightUtils.saturatedAdd(WRAPPER_BYTES, estimateTable(fallback.wrapped()));
            return MetaCacheWeightUtils.saturatedAdd(bytes, estimateTable(fallback.other()));
        }

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        TableSchema schema = fileStoreTable.schema();
        long bytes = TABLE_BASE_BYTES;
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedStringBytes(table.name()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedStringBytes(fileStoreTable.location().toString()));
        bytes = addCount(bytes, schema.fields().size(), TABLE_FIELD_BYTES);
        bytes = addCount(bytes, schema.options().size(), TABLE_OPTION_BYTES);
        bytes = addCount(bytes, schema.partitionKeys().size(), TABLE_KEY_BYTES);
        bytes = addCount(bytes, schema.primaryKeys().size(), TABLE_KEY_BYTES);
        return addCount(bytes, schema.bucketKeys().size(), TABLE_KEY_BYTES);
    }

    /**
     * Captures skew-sensitive schema text once when the snapshot cache value is constructed.
     * All collections are already materialized in TableSchema; this never opens the table store.
     */
    static long retainedTablePayloadBytes(Table table) {
        if (table instanceof PrivilegedFileStoreTable) {
            return retainedTablePayloadBytes(((PrivilegedFileStoreTable) table).wrapped());
        }
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable fallback = (FallbackReadFileStoreTable) table;
            return MetaCacheWeightUtils.saturatedAdd(
                    retainedTablePayloadBytes(fallback.wrapped()),
                    retainedTablePayloadBytes(fallback.other()));
        }
        if (!(table instanceof FileStoreTable)) {
            return 0L;
        }

        TableSchema schema = ((FileStoreTable) table).schema();
        if (schema == null) {
            return 0L;
        }
        long bytes = addString(0L, schema.comment());
        for (DataField field : schema.fields()) {
            bytes = addFieldPayload(bytes, field, false);
        }
        for (Map.Entry<String, String> option : schema.options().entrySet()) {
            bytes = addString(bytes, option.getKey());
            bytes = addString(bytes, option.getValue());
        }
        bytes = addStrings(bytes, schema.partitionKeys());
        bytes = addStrings(bytes, schema.primaryKeys());
        return addStrings(bytes, schema.bucketKeys());
    }

    private static long addStrings(long bytes, List<String> values) {
        for (String value : values) {
            bytes = addString(bytes, value);
        }
        return bytes;
    }

    private static long addFieldPayload(long bytes, DataField field, boolean nested) {
        if (nested) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, NESTED_FIELD_BYTES);
        }
        bytes = addString(bytes, field.name());
        bytes = addString(bytes, field.description());
        bytes = addString(bytes, field.defaultValue());
        return addTypePayload(bytes, field.type());
    }

    private static long addTypePayload(long bytes, DataType type) {
        if (type instanceof RowType) {
            for (DataField field : ((RowType) type).getFields()) {
                bytes = addFieldPayload(bytes, field, true);
            }
        } else if (type instanceof ArrayType) {
            bytes = addTypePayload(bytes, ((ArrayType) type).getElementType());
        } else if (type instanceof MapType) {
            bytes = addTypePayload(bytes, ((MapType) type).getKeyType());
            bytes = addTypePayload(bytes, ((MapType) type).getValueType());
        } else if (type instanceof MultisetType) {
            bytes = addTypePayload(bytes, ((MultisetType) type).getElementType());
        }
        return bytes;
    }

    private static long addString(long bytes, String value) {
        return MetaCacheWeightUtils.saturatedAdd(
                bytes, MetaCacheWeightUtils.estimatedStringBytes(value));
    }

    private static long addCount(long bytes, long count, long bytesPerItem) {
        return MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.saturatedMultiply(count, bytesPerItem));
    }
}
