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

import com.google.common.collect.ImmutableMap;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.types.VectorType;

import java.util.List;
import java.util.Map;

/** Publication-time retained-weight formula for Paimon snapshot projections. */
final class PaimonCacheSizeEstimator {
    // Calibrated against JOL retained-graph deltas in PaimonExternalMetaCacheTest.
    private static final long MAX_TABLE_ACCOUNTING_ELEMENTS = 50_000L;
    private static final int MAX_TYPE_ACCOUNTING_DEPTH = 128;
    private static final long KEY_BASE_BYTES = objectBytes(128L);
    private static final long SNAPSHOT_BASE_BYTES = objectBytes(4L * 1024L);
    private static final long TABLE_BASE_BYTES = objectBytes(16L * 1024L);
    // A top-level DataField, its list slot and shared per-field overhead; the DataType instance
    // is accounted separately by addTypePayload.
    private static final long TABLE_FIELD_BYTES = objectBytes(40L);
    private static final long TABLE_OPTION_BYTES = objectBytes(44L);
    private static final long TABLE_KEY_BYTES = objectBytes(128L);
    // Exact Paimon 1.4.2 layouts, pinned by PAIMON_TYPE_LAYOUT_SUPPORTED.
    private static final long DATA_FIELD_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(4L, 4L);
    private static final long ARRAY_TYPE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(2L, 1L);
    private static final long VECTOR_TYPE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(2L, 5L);
    private static final long MAP_TYPE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(3L, 1L);
    private static final long MULTISET_TYPE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(2L, 1L);
    // RowType plus Collections.unmodifiableList(new ArrayList<>(fields)).
    private static final long ROW_TYPE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(6L, 1L);
    private static final long UNMODIFIABLE_LIST_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(2L, 0L);
    private static final long ARRAY_LIST_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(1L, 8L);
    private static final long HASH_MAP_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(4L, 16L);
    private static final long HASH_MAP_NODE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(3L, 4L);
    private static final long INTEGER_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(0L, 4L);
    private static final int ROW_TYPE_LAZY_MAP_COUNT = 4;
    // Accepted leaf DataType implementations and the int fields each adds to DataType's nullable
    // flag and type root. Any other class, including a future or third-party type, rejects
    // weighted admission instead of being counted as an arbitrary primitive.
    private static final String[] NO_LEAF_FIELDS = {};
    private static final String[] LENGTH_LEAF_FIELDS = {"length:int"};
    private static final String[] PRECISION_LEAF_FIELDS = {"precision:int"};
    private static final Map<Class<? extends DataType>, String[]> LEAF_TYPE_FIELDS =
            ImmutableMap.<Class<? extends DataType>, String[]>builder()
                    .put(CharType.class, LENGTH_LEAF_FIELDS)
                    .put(VarCharType.class, LENGTH_LEAF_FIELDS)
                    .put(BooleanType.class, NO_LEAF_FIELDS)
                    .put(BinaryType.class, LENGTH_LEAF_FIELDS)
                    .put(VarBinaryType.class, LENGTH_LEAF_FIELDS)
                    .put(DecimalType.class, new String[] {"precision:int", "scale:int"})
                    .put(TinyIntType.class, NO_LEAF_FIELDS)
                    .put(SmallIntType.class, NO_LEAF_FIELDS)
                    .put(IntType.class, NO_LEAF_FIELDS)
                    .put(BigIntType.class, NO_LEAF_FIELDS)
                    .put(FloatType.class, NO_LEAF_FIELDS)
                    .put(DoubleType.class, NO_LEAF_FIELDS)
                    .put(DateType.class, NO_LEAF_FIELDS)
                    .put(TimeType.class, PRECISION_LEAF_FIELDS)
                    .put(TimestampType.class, PRECISION_LEAF_FIELDS)
                    .put(LocalZonedTimestampType.class, PRECISION_LEAF_FIELDS)
                    .put(VariantType.class, NO_LEAF_FIELDS)
                    .put(BlobType.class, NO_LEAF_FIELDS)
                    .build();
    private static final boolean PAIMON_TYPE_LAYOUT_SUPPORTED = checkPaimonTypeLayout();
    private static final boolean PAIMON_TABLE_LAYOUT_SUPPORTED = checkPaimonTableLayout();
    private static final long PARTITION_BYTES = objectBytes(160L);
    private static final long PARTITION_ITEM_BYTES = objectBytes(640L);
    private static final long WRAPPER_BYTES = objectBytes(512L);

    private PaimonCacheSizeEstimator() {
    }

    private static long objectBytes(long bytes) {
        return MetaCacheWeightUtils.estimatedObjectBytes(bytes);
    }

    /** DataType: typeRoot reference plus the isNullable flag, then the subclass int fields. */
    private static long leafTypeBytes(String[] intFields) {
        return MetaCacheWeightUtils.estimatedObjectLayoutBytes(
                1L, 1L + (long) Integer.BYTES * intFields.length);
    }

    /** Pin the Paimon 1.4.2 DataType/DataField/RowType layouts the formulas above are built on. */
    private static boolean checkPaimonTypeLayout() {
        boolean supported = MetaCacheWeightUtils.hasExpectedInstanceFields(
                DataType.class, "isNullable:boolean", "typeRoot:DataTypeRoot")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(
                        DataField.class, "id:int", "name:String", "type:DataType",
                        "description:String", "defaultValue:String")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(
                        RowType.class, "fields:List", "laziedNameToField:Map",
                        "laziedNameToIndex:Map", "laziedFieldIdToField:Map",
                        "laziedFieldIdToIndex:Map")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(
                        ArrayType.class, "elementType:DataType")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(
                        VectorType.class, "elementType:DataType", "length:int")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(
                        MapType.class, "keyType:DataType", "valueType:DataType")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(
                        MultisetType.class, "elementType:DataType");
        for (Map.Entry<Class<? extends DataType>, String[]> leaf : LEAF_TYPE_FIELDS.entrySet()) {
            supported &= MetaCacheWeightUtils.hasExpectedInstanceFields(
                    leaf.getKey(), leaf.getValue());
        }
        return supported;
    }

    /** Pin TableSchema and the two accepted FileStoreTable implementations. */
    private static boolean checkPaimonTableLayout() {
        ClassLoader loader = FileStoreTable.class.getClassLoader();
        String[] abstractTableFields = {
                "fileIO:FileIO", "path:Path", "tableSchema:TableSchema",
                "catalogEnvironment:CatalogEnvironment", "manifestCache:SegmentsCache",
                "snapshotCache:Cache", "statsCache:Cache", "dvmetaCache:DVMetaCache"};
        return MetaCacheWeightUtils.hasExpectedInstanceFields(
                TableSchema.class, "version:int", "id:long", "fields:List",
                "highestFieldId:int", "partitionKeys:List", "primaryKeys:List",
                "bucketKeys:List", "numBucket:int", "options:Map", "comment:String",
                "timeMillis:long")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(
                        "org.apache.paimon.table.AbstractFileStoreTable", loader,
                        abstractTableFields)
                && MetaCacheWeightUtils.hasExpectedInstanceFields(
                        "org.apache.paimon.table.AppendOnlyFileStoreTable", loader,
                        "lazyStore:AppendOnlyFileStore")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(
                        "org.apache.paimon.table.PrimaryKeyFileStoreTable", loader,
                        "lazyStore:KeyValueFileStore");
    }

    static MetaCacheSizeEstimate estimateSnapshotEntry(
            PaimonSnapshotEntryKey key, PaimonSnapshotCacheValue value) {
        if (!MetaCacheWeightUtils.isSupportedJvmObjectLayout()) {
            return MetaCacheSizeEstimate.incomplete("unsupported_jvm_object_alignment");
        }
        if (!PAIMON_TYPE_LAYOUT_SUPPORTED || !PAIMON_TABLE_LAYOUT_SUPPORTED) {
            return MetaCacheSizeEstimate.incomplete("unsupported_paimon_layout");
        }
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
        return retainedTablePayloadBytes(
                table, new AccountingBudget(MAX_TABLE_ACCOUNTING_ELEMENTS));
    }

    private static long retainedTablePayloadBytes(Table table, AccountingBudget budget) {
        budget.charge(1L);
        if (table instanceof PrivilegedFileStoreTable) {
            return retainedTablePayloadBytes(
                    ((PrivilegedFileStoreTable) table).wrapped(), budget);
        }
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable fallback = (FallbackReadFileStoreTable) table;
            return MetaCacheWeightUtils.saturatedAdd(
                    retainedTablePayloadBytes(fallback.wrapped(), budget),
                    retainedTablePayloadBytes(fallback.other(), budget));
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
            bytes = addFieldPayload(bytes, field, false, budget, 0);
        }
        budget.charge(schema.options().size());
        for (Map.Entry<String, String> option : schema.options().entrySet()) {
            bytes = addString(bytes, option.getKey());
            bytes = addString(bytes, option.getValue());
        }
        bytes = addStrings(bytes, schema.partitionKeys(), budget);
        bytes = addStrings(bytes, schema.primaryKeys(), budget);
        return addStrings(bytes, schema.bucketKeys(), budget);
    }

    private static long addStrings(
            long bytes, List<String> values, AccountingBudget budget) {
        budget.charge(values.size());
        for (String value : values) {
            bytes = addString(bytes, value);
        }
        return bytes;
    }

    private static long addFieldPayload(
            long bytes, DataField field, boolean nested, AccountingBudget budget,
            int typeDepth) {
        budget.charge(1L);
        if (nested) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, DATA_FIELD_BYTES);
        }
        bytes = addString(bytes, field.name());
        bytes = addString(bytes, field.description());
        bytes = addString(bytes, field.defaultValue());
        return addTypePayload(bytes, field.type(), budget, typeDepth);
    }

    /**
     * Account one DataType instance and its owned children. Every accepted implementation is
     * matched explicitly; an unknown class throws so estimateSafely rejects weighted admission
     * instead of counting a future composite type as a small primitive.
     */
    private static long addTypePayload(
            long bytes, DataType type, AccountingBudget budget, int typeDepth) {
        if (typeDepth > MAX_TYPE_ACCOUNTING_DEPTH) {
            throw new IllegalStateException(
                    "Paimon cache accounting type depth exceeded");
        }
        budget.charge(1L);
        if (type == null) {
            throw new IllegalStateException("Paimon field type is missing");
        }
        Class<?> typeClass = type.getClass();
        if (typeClass == RowType.class) {
            RowType rowType = (RowType) type;
            List<DataField> fields = rowType.getFields();
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, rowTypeBytes(fields));
            for (DataField field : fields) {
                bytes = addFieldPayload(bytes, field, true, budget, typeDepth + 1);
            }
            return bytes;
        }
        if (typeClass == ArrayType.class) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, ARRAY_TYPE_BYTES);
            return addTypePayload(
                    bytes, ((ArrayType) type).getElementType(), budget, typeDepth + 1);
        }
        if (typeClass == VectorType.class) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, VECTOR_TYPE_BYTES);
            return addTypePayload(
                    bytes, ((VectorType) type).getElementType(), budget, typeDepth + 1);
        }
        if (typeClass == MapType.class) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, MAP_TYPE_BYTES);
            bytes = addTypePayload(
                    bytes, ((MapType) type).getKeyType(), budget, typeDepth + 1);
            return addTypePayload(
                    bytes, ((MapType) type).getValueType(), budget, typeDepth + 1);
        }
        if (typeClass == MultisetType.class) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, MULTISET_TYPE_BYTES);
            return addTypePayload(
                    bytes, ((MultisetType) type).getElementType(), budget, typeDepth + 1);
        }
        String[] leafFields = LEAF_TYPE_FIELDS.get(typeClass);
        if (leafFields == null) {
            throw new IllegalStateException(
                    "Unsupported Paimon data type: " + typeClass.getName());
        }
        return MetaCacheWeightUtils.saturatedAdd(bytes, leafTypeBytes(leafFields));
    }

    /**
     * RowType, its unmodifiable ArrayList copy of the fields, and the four lazy lookup maps that
     * getField/getFieldIndex materialize after admission. The maps are reserved up front in O(N)
     * so a query cannot grow the retained graph past the admitted weight; nothing is materialized.
     */
    private static long rowTypeBytes(List<DataField> fields) {
        long fieldCount = fields.size();
        long bytes = MetaCacheWeightUtils.saturatedAdd(ROW_TYPE_BYTES, UNMODIFIABLE_LIST_BYTES);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, ARRAY_LIST_BYTES);
        if (fieldCount == 0L) {
            return bytes;
        }
        bytes = MetaCacheWeightUtils.saturatedAdd(
                bytes, MetaCacheWeightUtils.estimatedObjectArrayBytes(fieldCount));
        long uncachedFieldIds = 0L;
        for (DataField field : fields) {
            if (field.id() < -128 || field.id() > 127) {
                uncachedFieldIds++;
            }
        }
        long uncachedIndexes = fieldCount > 128L ? fieldCount - 128L : 0L;
        long mapBytes = MetaCacheWeightUtils.saturatedAdd(HASH_MAP_BYTES,
                MetaCacheWeightUtils.estimatedObjectArrayBytes(hashMapCapacity(fieldCount)));
        mapBytes = addCount(mapBytes, fieldCount, HASH_MAP_NODE_BYTES);
        bytes = addCount(bytes, ROW_TYPE_LAZY_MAP_COUNT, mapBytes);
        // Boxed keys/values outside the Integer cache: nameToIndex values, fieldIdToField keys,
        // and fieldIdToIndex boxes both again.
        bytes = addCount(bytes, uncachedIndexes, INTEGER_BYTES);
        bytes = addCount(bytes, uncachedFieldIds, INTEGER_BYTES);
        bytes = addCount(bytes, uncachedIndexes, INTEGER_BYTES);
        return addCount(bytes, uncachedFieldIds, INTEGER_BYTES);
    }

    private static long hashMapCapacity(long size) {
        long capacity = 16L;
        while (size > capacity - capacity / 4L) {
            capacity = MetaCacheWeightUtils.saturatedMultiply(capacity, 2L);
            if (capacity == Long.MAX_VALUE) {
                return capacity;
            }
        }
        return capacity;
    }

    private static long addString(long bytes, String value) {
        return MetaCacheWeightUtils.saturatedAdd(
                bytes, MetaCacheWeightUtils.estimatedStringBytes(value));
    }

    private static long addCount(long bytes, long count, long bytesPerItem) {
        return MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.saturatedMultiply(count, bytesPerItem));
    }

    private static final class AccountingBudget {
        private long remaining;

        private AccountingBudget(long remaining) {
            this.remaining = remaining;
        }

        private void charge(long elements) {
            if (elements < 0L || elements > remaining) {
                throw new IllegalStateException(
                        "Paimon cache accounting work budget exceeded");
            }
            remaining -= elements;
        }
    }
}
