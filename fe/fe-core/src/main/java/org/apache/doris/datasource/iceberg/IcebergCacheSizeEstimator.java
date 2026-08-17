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

import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Publication-time retained-weight formulas for Iceberg cache entries. */
final class IcebergCacheSizeEstimator {
    // Calibrated against JOL retained-graph deltas in IcebergExternalMetaCacheTest.
    // Every metadata element visited (field, type, snapshot, summary entry, ...) costs a few
    // reads; the bound only guards against pathological metadata and is far above real tables
    // (a 10,000-snapshot history with 15 summary keys each is 160,000 elements). Exceeding it
    // rejects weighted admission, so it must not be reachable by ordinary long-lived tables.
    private static final long MAX_TABLE_ACCOUNTING_ELEMENTS = 2_000_000L;
    // Total name characters the estimator may lower-case while reserving case-insensitive indexes.
    private static final long MAX_TABLE_ACCOUNTING_CHARACTERS = 4_000_000L;
    private static final int MAX_TYPE_ACCOUNTING_DEPTH = 128;
    private static final long KEY_BASE_BYTES = objectBytes(128L);
    private static final long TABLE_BASE_BYTES = objectBytes(16L * 1024L);
    // TableMetadata-side share of one schema version: schemas list slot and schemasById entry,
    // including the growth of both from their singleton to their regular immutable shapes.
    private static final long SCHEMA_VERSION_BYTES = objectBytes(128L);
    private static final long PARTITION_SPEC_BYTES = objectBytes(256L);
    // Exact active-layout sizes of the Iceberg/Guava objects that lazy partition, sort and
    // schema state allocates. Iceberg 1.10.1 field layouts are pinned by ICEBERG_LAZY_LAYOUT_SUPPORTED.
    private static final long PARTITION_FIELD_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(2L, 8L);
    private static final long SORT_FIELD_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(3L, 4L);
    // Identity/Bucket/Truncate transforms are allocated per parsed field; time transforms are enums.
    private static final long TRANSFORM_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(1L, 0L);
    private static final long NESTED_FIELD_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(5L, 5L);
    private static final long STRUCT_TYPE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(6L, 0L);
    private static final long SCHEMA_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(11L, 8L);
    private static final long IMMUTABLE_LIST_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(1L, 0L);
    private static final long IMMUTABLE_MAP_KEY_SET_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(2L, 0L);
    private static final long SINGLETON_IMMUTABLE_SET_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(1L, 0L);
    private static final long REGULAR_IMMUTABLE_SET_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(3L, 8L);
    private static final long ARRAY_LIST_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(1L, 8L);
    private static final long HASH_MAP_NODE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(3L, 4L);
    private static final long HASH_MAP_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(4L, 16L);
    private static final long INTEGER_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(0L, 4L);
    private static final long LONG_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(0L, 8L);
    private static final String TRUNCATE_TRANSFORM_PREFIX = "truncate[";
    // Truncate on a decimal source retains a BigInteger width (object plus one-int magnitude).
    private static final long TRUNCATE_WIDTH_BYTES = MetaCacheWeightUtils.saturatedAdd(
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(1L, 20L),
            MetaCacheWeightUtils.estimatedIntArrayBytes(1L));
    private static final long LIST_MULTIMAP_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(9L, 0L);
    private static final long CAPTURING_SUPPLIER_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(1L, 0L);
    private static final long POSITION_ACCESSOR_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(2L, 4L);
    // One WrappedPositionAccessor (1 ref + int) per optional struct ancestor. Required ancestors
    // collapse into a single Position2/3Accessor that replaces the inner accessor, which retains
    // less than this per-level reservation.
    private static final long WRAPPED_ACCESSOR_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(1L, 4L);
    private static final long LIST_TYPE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(2L, 0L);
    private static final long MAP_TYPE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(3L, 0L);
    private static final long DECIMAL_TYPE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(0L, 8L);
    private static final long FIXED_TYPE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(0L, 4L);
    private static final long GEOMETRY_TYPE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(1L, 0L);
    private static final long GEOGRAPHY_TYPE_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(2L, 0L);
    private static final long SORT_ORDER_BYTES = objectBytes(256L);
    private static final long TABLE_PROPERTY_BYTES = objectBytes(40L);
    private static final long CURRENT_SNAPSHOT_BYTES = objectBytes(512L);
    private static final long HISTORICAL_SNAPSHOT_BYTES = objectBytes(176L);
    private static final long SNAPSHOT_LOG_ENTRY_BYTES = objectBytes(38L);
    private static final long METADATA_LOG_ENTRY_BYTES = objectBytes(128L);
    private static final long SNAPSHOT_REF_BYTES = objectBytes(128L);
    private static final long STATISTICS_FILE_BYTES = objectBytes(512L);
    private static final long BLOB_METADATA_BYTES = objectBytes(128L);
    private static final long BLOB_FIELD_BYTES = objectBytes(32L);
    private static final long PARTITION_STATISTICS_FILE_BYTES = objectBytes(256L);
    private static final long ENCRYPTED_KEY_BYTES = objectBytes(256L);
    // One retained IcebergPartition (value/transform ArrayLists) or one RangePartitionItem with a
    // single partition column plus its map entry; extra columns are charged by IcebergPartitionInfo.
    private static final long PARTITION_BYTES = objectBytes(680L);
    private static final long PARTITION_ALIAS_BYTES = objectBytes(256L);
    private static final long NAME_MAPPING_ENTRY_BYTES = objectBytes(256L);
    private static final long MANIFEST_ENTRY_BASE_BYTES = objectBytes(256L);
    private static final long DATA_FILE_BYTES = objectBytes(896L);
    private static final long DELETE_FILE_BYTES = objectBytes(1024L);
    private static final long FILE_METRIC_ENTRY_BYTES = objectBytes(104L);
    private static final String BASE_SNAPSHOT_CLASS_NAME = "org.apache.iceberg.BaseSnapshot";
    private static final Field[] BASE_SNAPSHOT_RETAINED_CACHE_FIELDS =
            loadBaseSnapshotRetainedCacheFields();
    // TableMetadata.snapshots()/snapshot(id) load lazily through a catalog supplier
    // (REST snapshot-loading-mode=refs). Publication must not perform that IO.
    private static final Field TABLE_METADATA_SNAPSHOTS_LOADED_FIELD =
            loadTableMetadataField("snapshotsLoaded", boolean.class);
    private static final Field TABLE_METADATA_SNAPSHOTS_SUPPLIER_FIELD =
            loadTableMetadataField("snapshotsSupplier", null);
    // The formulas above are built on the Iceberg 1.10.1 instance-field layouts of the classes a
    // cached table retains. Every non-static field is pinned, not only the transient lazy ones: a
    // library upgrade that adds a retained reference makes weighted admission fail closed.
    private static final boolean ICEBERG_LAZY_LAYOUT_SUPPORTED = checkIcebergLayout();

    private IcebergCacheSizeEstimator() {
    }

    private static long objectBytes(long bytes) {
        return MetaCacheWeightUtils.estimatedObjectBytes(bytes);
    }

    static MetaCacheSizeEstimate estimateTableEntry(NameMapping key, IcebergTableCacheValue value) {
        MetaCacheSizeEstimate layoutSupport = checkJvmObjectLayout();
        if (!layoutSupport.isComplete()) {
            return layoutSupport;
        }
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
        MetaCacheSizeEstimate layoutSupport = checkJvmObjectLayout();
        if (!layoutSupport.isComplete()) {
            return layoutSupport;
        }
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
        bytes = addCount(bytes, value.getNameMapping().map(Map::size).orElse(0),
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
        MetaCacheSizeEstimate layoutSupport = checkJvmObjectLayout();
        if (!layoutSupport.isComplete()) {
            return layoutSupport;
        }
        if (!value.isAccountingComplete()) {
            return MetaCacheSizeEstimate.incomplete("iceberg_manifest_accounting_incomplete");
        }
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

    private static MetaCacheSizeEstimate checkJvmObjectLayout() {
        return MetaCacheWeightUtils.isSupportedJvmObjectLayout()
                ? MetaCacheSizeEstimate.complete(1L)
                : MetaCacheSizeEstimate.incomplete("unsupported_jvm_object_alignment");
    }

    private static MetaCacheSizeEstimate checkSupportedTable(Table table) {
        if (!ICEBERG_LAZY_LAYOUT_SUPPORTED) {
            return MetaCacheSizeEstimate.incomplete("unsupported_iceberg_lazy_layout");
        }
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
        if (!areSnapshotsLoaded(metadata)) {
            return MetaCacheSizeEstimate.incomplete("iceberg_snapshots_not_loaded");
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

    /**
     * Fully accounts variable payload with a bounded amount of publication-time work. Only
     * already-parsed metadata is read; the SDK state it touches on the way (StructType, ListType
     * and MapType fieldList copies, the identifier field set) is small, accounted and O(N).
     */
    static long retainedTablePayloadBytes(Table table) {
        if (!(table instanceof HasTableOperations)) {
            return 0L;
        }
        TableMetadata metadata = ((HasTableOperations) table).operations().current();
        if (metadata == null) {
            return 0L;
        }
        if (!areSnapshotsLoaded(metadata)) {
            // snapshots()/refs() would call the catalog's lazy snapshot supplier: fail closed.
            throw new IllegalStateException("Iceberg table snapshots are not loaded");
        }

        long bytes = 0L;
        AccountingBudget budget = new AccountingBudget(
                MAX_TABLE_ACCOUNTING_ELEMENTS, MAX_TABLE_ACCOUNTING_CHARACTERS);
        for (PartitionSpec spec : metadata.specs()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, partitionSpecBytes(spec, budget));
        }
        for (SortOrder sortOrder : metadata.sortOrders()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, sortOrderBytes(sortOrder, budget));
        }
        for (Schema schema : metadata.schemas()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, schemaBytes(schema, budget));
        }
        budget.chargeElements(metadata.properties().size());
        for (Map.Entry<String, String> property : metadata.properties().entrySet()) {
            bytes = addString(bytes, property.getKey());
            bytes = addString(bytes, property.getValue());
        }
        for (Snapshot snapshot : metadata.snapshots()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, snapshotBytes(snapshot, budget));
        }
        budget.chargeElements(metadata.snapshotLog().size());
        bytes = addCount(bytes, metadata.snapshotLog().size(), SNAPSHOT_LOG_ENTRY_BYTES);
        budget.chargeElements(metadata.previousFiles().size());
        for (TableMetadata.MetadataLogEntry previousFile : metadata.previousFiles()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, METADATA_LOG_ENTRY_BYTES);
            bytes = addString(bytes, previousFile.file());
        }
        budget.chargeElements(metadata.refs().size());
        bytes = addCount(bytes, metadata.refs().size(), SNAPSHOT_REF_BYTES);
        for (String refName : metadata.refs().keySet()) {
            bytes = addString(bytes, refName);
        }
        budget.chargeElements(metadata.statisticsFiles().size());
        for (StatisticsFile statisticsFile : metadata.statisticsFiles()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, STATISTICS_FILE_BYTES);
            bytes = addString(bytes, statisticsFile.path());
            for (BlobMetadata blob : statisticsFile.blobMetadata()) {
                budget.chargeElements(MetaCacheWeightUtils.saturatedAdd(1L,
                        MetaCacheWeightUtils.saturatedAdd(
                                blob.fields().size(), blob.properties().size())));
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes, BLOB_METADATA_BYTES);
                bytes = addString(bytes, blob.type());
                bytes = addCount(bytes, blob.fields().size(), BLOB_FIELD_BYTES);
                bytes = addStringMap(bytes, blob.properties(), TABLE_PROPERTY_BYTES);
            }
        }
        budget.chargeElements(metadata.partitionStatisticsFiles().size());
        for (PartitionStatisticsFile statisticsFile : metadata.partitionStatisticsFiles()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, PARTITION_STATISTICS_FILE_BYTES);
            bytes = addString(bytes, statisticsFile.path());
        }
        budget.chargeElements(metadata.encryptionKeys().size());
        for (EncryptedKey encryptedKey : metadata.encryptionKeys()) {
            budget.chargeElements(encryptedKey.properties().size());
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, ENCRYPTED_KEY_BYTES);
            bytes = addString(bytes, encryptedKey.keyId());
            bytes = addString(bytes, encryptedKey.encryptedById());
            bytes = addBufferPayload(bytes, encryptedKey.encryptedKeyMetadata());
            bytes = addStringMap(bytes, encryptedKey.properties(), TABLE_PROPERTY_BYTES);
        }
        bytes = addString(bytes, metadata.uuid());
        return bytes;
    }

    /**
     * Account a PartitionSpec together with the lazy state that a normal scan materializes after
     * admission: fieldList, javaClasses, partitionType() with its StructType indexes, the secondary
     * Schema/Binder graph behind partitionType().asSchema() and fieldsBySourceId. Iceberg 1.10.1
     * allocates one Object[fieldCount] per distinct source id inside fieldsBySourceId, so that
     * retained graph is O(distinctSourceIds * fieldCount); it is reserved here in O(fieldCount)
     * publication work without materializing any of it.
     */
    private static long partitionSpecBytes(PartitionSpec spec, AccountingBudget budget) {
        List<PartitionField> fields = spec.fields();
        long fieldCount = fields.size();
        budget.chargeElements(MetaCacheWeightUtils.saturatedAdd(1L, fieldCount));
        long bytes = PARTITION_SPEC_BYTES;
        if (fieldCount == 0L) {
            return bytes;
        }
        Set<Integer> distinctSourceIds = new HashSet<>();
        long uncachedSourceIds = 0L;
        long uncachedFieldIds = 0L;
        long lowerCaseNameBytes = 0L;
        for (PartitionField field : fields) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, PARTITION_FIELD_BYTES);
            bytes = addTransformPayload(bytes, field.transform());
            bytes = addString(bytes, field.name());
            lowerCaseNameBytes = MetaCacheWeightUtils.saturatedAdd(
                    lowerCaseNameBytes, generatedLowerCaseNameBytes(field.name(), budget));
            if (isUncachedInteger(field.fieldId())) {
                uncachedFieldIds++;
            }
            if (distinctSourceIds.add(field.sourceId()) && isUncachedInteger(field.sourceId())) {
                uncachedSourceIds++;
            }
        }
        // Eager PartitionField[] plus lazy fieldList and javaClasses.
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, objectArrayGrowthBytes(fieldCount));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, immutableListBytes(fieldCount));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, objectArrayGrowthBytes(fieldCount));
        // partitionType(): the StructType itself also exists for an unpartitioned spec.
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, objectArrayGrowthBytes(fieldCount));
        bytes = addCount(bytes, fieldCount, NESTED_FIELD_BYTES);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                structTypeIndexBytes(fieldCount, uncachedFieldIds, lowerCaseNameBytes));
        if (!spec.schema().idsToOriginal().isEmpty()) {
            // rawPartitionType() rebuilds the struct with original ids.
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, STRUCT_TYPE_BYTES);
            bytes = MetaCacheWeightUtils.saturatedAdd(
                    bytes, MetaCacheWeightUtils.estimatedObjectArrayBytes(fieldCount));
            bytes = addCount(bytes, fieldCount, NESTED_FIELD_BYTES);
        }
        // A partition filter binds against partitionType().asSchema().
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, secondarySchemaBytes(
                SchemaShape.flat(fieldCount, uncachedFieldIds, lowerCaseNameBytes)));
        // fieldsBySourceId: HashMap<Integer, ArrayList(capacity = fieldCount)>.
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, LIST_MULTIMAP_BYTES);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, CAPTURING_SUPPLIER_BYTES);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                hashIdMapBytes(distinctSourceIds.size(), uncachedSourceIds));
        return addCount(bytes, distinctSourceIds.size(),
                MetaCacheWeightUtils.saturatedAdd(ARRAY_LIST_BYTES,
                        MetaCacheWeightUtils.estimatedObjectArrayBytes(fieldCount)));
    }

    /** Account a SortOrder: SortField[] with per-field transforms plus the lazy fieldList copy. */
    private static long sortOrderBytes(SortOrder sortOrder, AccountingBudget budget) {
        long fieldCount = sortOrder.fields().size();
        budget.chargeElements(MetaCacheWeightUtils.saturatedAdd(1L, fieldCount));
        long bytes = SORT_ORDER_BYTES;
        if (fieldCount == 0L) {
            return bytes;
        }
        for (SortField field : sortOrder.fields()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, SORT_FIELD_BYTES);
            bytes = addTransformPayload(bytes, field.transform());
        }
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, objectArrayGrowthBytes(fieldCount));
        return MetaCacheWeightUtils.saturatedAdd(bytes, immutableListBytes(fieldCount));
    }

    /** Transform instance plus the payload only some transforms retain. */
    private static long addTransformPayload(long bytes, Transform<?, ?> transform) {
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, TRANSFORM_BYTES);
        if (transform instanceof UnknownTransform) {
            return addString(bytes, transform.toString());
        }
        if (transform.toString().startsWith(TRUNCATE_TRANSFORM_PREFIX)) {
            // Truncate is package-private; its serialized name is the SPI contract.
            return MetaCacheWeightUtils.saturatedAdd(bytes, TRUNCATE_WIDTH_BYTES);
        }
        return bytes;
    }

    /** Lazy StructType indexes: fieldList, fieldsByName, fieldsByLowerCaseName and fieldsById. */
    private static long structTypeIndexBytes(
            long fieldCount, long uncachedFieldIds, long lowerCaseNameBytes) {
        long bytes = immutableListBytes(fieldCount);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                immutableNameMapBytes(fieldCount, 0L, 0L));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                immutableNameMapBytes(fieldCount, 0L, lowerCaseNameBytes));
        return MetaCacheWeightUtils.saturatedAdd(bytes,
                immutableNameMapBytes(fieldCount, uncachedFieldIds, 0L));
    }

    /**
     * The Schema created by StructType.asSchema(): its constructor materializes idToName and two
     * empty id maps; Binder and projection paths add nameToId, lowerCaseNameToId, idToField and
     * idToAccessor; its own StructType copy grows the same lazy indexes as the root struct.
     */
    private static long secondarySchemaBytes(SchemaShape shape) {
        long bytes = schemaObjectBytes(shape);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, schemaLookupBytes(shape));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, schemaLazyIndexBytes(shape));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, STRUCT_TYPE_BYTES);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedObjectArrayBytes(shape.topLevelFieldCount));
        return MetaCacheWeightUtils.saturatedAdd(bytes, structTypeIndexBytes(
                shape.topLevelFieldCount, shape.uncachedTopLevelFieldIdCount,
                shape.topLevelLowerCaseStringBytes));
    }

    /** Schema object, empty identifier int[], the two empty id maps and the eager idToName keySet. */
    private static long schemaObjectBytes(SchemaShape shape) {
        long bytes = MetaCacheWeightUtils.saturatedAdd(
                SCHEMA_BYTES, MetaCacheWeightUtils.estimatedIntArrayBytes(0L));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, HASH_MAP_BYTES);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, HASH_MAP_BYTES);
        if (shape.fieldCount == 1L) {
            return MetaCacheWeightUtils.saturatedAdd(bytes, SINGLETON_IMMUTABLE_SET_BYTES);
        }
        return shape.fieldCount > 1L
                ? MetaCacheWeightUtils.saturatedAdd(bytes, IMMUTABLE_MAP_KEY_SET_BYTES) : bytes;
    }

    /**
     * idToName (eager in the constructor), nameToId and idToField. Every map boxes uncached ids
     * itself; idToName and nameToId each retain their own copy of every nested canonical name and
     * nameToId also retains the short aliases.
     */
    private static long schemaLookupBytes(SchemaShape shape) {
        long bytes = immutableNameMapBytes(
                shape.fieldCount, shape.uncachedFieldIdCount, shape.pathStringBytes);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, immutableNameMapBytes(
                shape.nameEntryCount, shape.uncachedNameIdCount,
                MetaCacheWeightUtils.saturatedAdd(
                        shape.pathStringBytes, shape.aliasStringBytes)));
        return MetaCacheWeightUtils.saturatedAdd(bytes,
                hashIdMapBytes(shape.fieldCount, shape.uncachedFieldIdCount));
    }

    /** lowerCaseNameToId and idToAccessor, materialized by case-insensitive lookups and Binder. */
    private static long schemaLazyIndexBytes(SchemaShape shape) {
        long bytes = immutableNameMapBytes(
                shape.nameEntryCount, shape.uncachedNameIdCount, shape.lowerCaseStringBytes);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                hashIdMapBytes(shape.accessorFieldCount, shape.uncachedAccessorIdCount));
        bytes = addCount(bytes, shape.accessorFieldCount, POSITION_ACCESSOR_BYTES);
        return addCount(bytes, shape.wrappedAccessorCount, WRAPPED_ACCESSOR_BYTES);
    }

    /**
     * One table schema version with every index a normal scan can materialize afterwards. Only
     * metadata already parsed is read; nothing lazy is touched, and each field is visited once.
     */
    private static long schemaBytes(Schema schema, AccountingBudget budget) {
        budget.chargeElements(1L);
        SchemaShape shape = new SchemaShape();
        long bytes = SCHEMA_VERSION_BYTES;
        for (Types.NestedField field : schema.columns()) {
            bytes = addFieldPayload(
                    bytes, field, PathState.ROOT, FieldKind.STRUCT_FIELD, budget, shape);
        }
        Set<Integer> identifierFieldIds = schema.identifierFieldIds();
        budget.chargeElements(identifierFieldIds.size());
        bytes = addIdentifierFieldPayload(bytes, identifierFieldIds);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, shape.typeObjectBytes);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, schemaObjectBytes(shape));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, STRUCT_TYPE_BYTES);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedObjectArrayBytes(shape.topLevelFieldCount));
        if (shape.fieldCount == 0L) {
            // Nothing can be looked up in an empty schema; its indexes stay shared singletons.
            return bytes;
        }
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, schemaLookupBytes(shape));
        // Future lazy growth: main lookups, root struct indexes and the asSchema() secondary graph.
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, schemaLazyIndexBytes(shape));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, structTypeIndexBytes(
                shape.topLevelFieldCount, shape.uncachedTopLevelFieldIdCount,
                shape.topLevelLowerCaseStringBytes));
        return MetaCacheWeightUtils.saturatedAdd(bytes, secondarySchemaBytes(shape));
    }

    /** ImmutableList.copyOf(array): shared empty, singleton, or a list object plus copied array. */
    private static long immutableListBytes(long elementCount) {
        if (elementCount <= 0L) {
            return 0L;
        }
        if (elementCount == 1L) {
            return IMMUTABLE_LIST_BYTES;
        }
        return MetaCacheWeightUtils.saturatedAdd(IMMUTABLE_LIST_BYTES,
                MetaCacheWeightUtils.estimatedObjectArrayBytes(elementCount));
    }

    /** Growth of a reference array that replaces an empty array retained by the empty shape. */
    private static long objectArrayGrowthBytes(long elementCount) {
        long populated = MetaCacheWeightUtils.estimatedObjectArrayBytes(elementCount);
        long empty = MetaCacheWeightUtils.estimatedObjectArrayBytes(0L);
        return populated == Long.MAX_VALUE ? populated : populated - empty;
    }

    /** Boxed Integer keys outside the JVM Integer cache are retained per lookup map. */
    private static boolean isUncachedInteger(int value) {
        return value < -128 || value > 127;
    }

    /**
     * Retained bytes of one lower-cased copy of a name, or 0 when the name is already lower case
     * and the index reuses it. Every case-insensitive index (partition StructType, secondary
     * Schema and secondary StructType) allocates its own copy, so callers add this per index.
     */
    private static long generatedLowerCaseNameBytes(String name, AccountingBudget budget) {
        budget.chargeCharacters(name.length());
        String lowerName = name.toLowerCase(Locale.ROOT);
        if (lowerName.equals(name)) {
            return 0L;
        }
        return MetaCacheWeightUtils.estimatedGeneratedStringBytes(
                lowerName.length(), MetaCacheWeightUtils.isLatin1String(lowerName));
    }

    private static long hashIdMapBytes(long entryCount, long uncachedIds) {
        long bytes = HASH_MAP_BYTES;
        if (entryCount <= 0L) {
            // HashMap allocates its table on the first put.
            return bytes;
        }
        bytes = addCount(bytes, entryCount, HASH_MAP_NODE_BYTES);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedObjectArrayBytes(
                        hashMapCapacity(entryCount)));
        return addCount(bytes, uncachedIds, INTEGER_BYTES);
    }

    private static long immutableNameMapBytes(
            long entryCount, long uncachedIds, long generatedStringBytes) {
        long bytes = 0L;
        if (entryCount == 1L) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                    MetaCacheWeightUtils.estimatedObjectLayoutBytes(8L, 0L));
        } else if (entryCount > 1L) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                    MetaCacheWeightUtils.estimatedObjectLayoutBytes(6L, 4L));
            bytes = addCount(bytes, entryCount,
                    MetaCacheWeightUtils.estimatedObjectLayoutBytes(3L, 0L));
            bytes = MetaCacheWeightUtils.saturatedAdd(
                    bytes, MetaCacheWeightUtils.estimatedObjectArrayBytes(entryCount));
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                    MetaCacheWeightUtils.estimatedObjectArrayBytes(
                            immutableMapTableCapacity(entryCount)));
        }
        bytes = addCount(bytes, uncachedIds, INTEGER_BYTES);
        return MetaCacheWeightUtils.saturatedAdd(bytes, generatedStringBytes);
    }

    private static long snapshotBytes(Snapshot snapshot, AccountingBudget budget) {
        rejectMaterializedSnapshotPayload(snapshot);
        Map<String, String> summary = snapshot.summary();
        budget.chargeElements(MetaCacheWeightUtils.saturatedAdd(
                1L, summary == null ? 0L : summary.size()));
        long bytes = HISTORICAL_SNAPSHOT_BYTES;
        // The parsed parent id is never inside the Long cache; the row-id fields are boxed too
        // and only tiny values would share a cached instance, so each present field is charged.
        bytes = addBoxedLong(bytes, snapshot.parentId());
        bytes = addBoxedLong(bytes, snapshot.firstRowId());
        bytes = addBoxedLong(bytes, snapshot.addedRows());
        bytes = addString(bytes, snapshot.operation());
        String manifestListLocation = snapshot.manifestListLocation();
        if (manifestListLocation == null) {
            // A snapshot serialized with an inline "manifests" array (legacy writers) retains a
            // String[] of manifest locations that is only exposed through ManifestFile wrappers.
            // Reject weighted admission instead of doing IO or admitting an underestimate.
            throw new IllegalStateException(
                    "Iceberg snapshot with inline manifest list is unsupported");
        }
        bytes = addString(bytes, manifestListLocation);
        bytes = addString(bytes, snapshot.keyId());
        return addStringMap(bytes, summary, TABLE_PROPERTY_BYTES);
    }

    private static void rejectMaterializedSnapshotPayload(Snapshot snapshot) {
        if (!BASE_SNAPSHOT_CLASS_NAME.equals(snapshot.getClass().getName())) {
            throw new IllegalStateException(
                    "Unsupported Iceberg snapshot implementation: "
                            + snapshot.getClass().getName());
        }
        if (BASE_SNAPSHOT_RETAINED_CACHE_FIELDS == null) {
            throw new IllegalStateException(
                    "Iceberg BaseSnapshot retained-cache inspection is unavailable");
        }
        try {
            // The field list is resolved once per process. Publication only performs a bounded
            // number of O(1) reads and never walks a retained manifest/file graph.
            for (Field retainedCacheField : BASE_SNAPSHOT_RETAINED_CACHE_FIELDS) {
                if (retainedCacheField.get(snapshot) != null) {
                    throw new IllegalStateException(
                            "Iceberg snapshot has materialized retained payload: "
                                    + retainedCacheField.getName());
                }
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                    "Cannot inspect Iceberg BaseSnapshot retained payload", e);
        }
    }

    /** Iceberg marks snapshots loaded at construction unless a lazy supplier was configured. */
    private static boolean areSnapshotsLoaded(TableMetadata metadata) {
        if (TABLE_METADATA_SNAPSHOTS_LOADED_FIELD == null
                || TABLE_METADATA_SNAPSHOTS_SUPPLIER_FIELD == null) {
            return false;
        }
        try {
            return TABLE_METADATA_SNAPSHOTS_SUPPLIER_FIELD.get(metadata) == null
                    || TABLE_METADATA_SNAPSHOTS_LOADED_FIELD.getBoolean(metadata);
        } catch (IllegalAccessException | RuntimeException e) {
            return false;
        }
    }

    private static Field loadTableMetadataField(String name, Class<?> expectedType) {
        try {
            Field field = TableMetadata.class.getDeclaredField(name);
            if ((expectedType != null && field.getType() != expectedType)
                    || Modifier.isStatic(field.getModifiers())) {
                return null;
            }
            field.setAccessible(true);
            return field;
        } catch (ReflectiveOperationException | RuntimeException e) {
            return null;
        }
    }

    private static Field[] loadBaseSnapshotRetainedCacheFields() {
        try {
            Class<?> snapshotClass = Class.forName(
                    BASE_SNAPSHOT_CLASS_NAME, false, Snapshot.class.getClassLoader());
            List<Field> retainedCacheFields = new ArrayList<>();
            for (Field field : snapshotClass.getDeclaredFields()) {
                int modifiers = field.getModifiers();
                if (Modifier.isTransient(modifiers) && !Modifier.isStatic(modifiers)
                        && !field.getType().isPrimitive()) {
                    field.setAccessible(true);
                    retainedCacheFields.add(field);
                }
            }
            return retainedCacheFields.isEmpty()
                    ? null : retainedCacheFields.toArray(new Field[0]);
        } catch (ReflectiveOperationException | RuntimeException e) {
            return null;
        }
    }

    private static boolean checkIcebergLayout() {
        ClassLoader loader = Snapshot.class.getClassLoader();
        return MetaCacheWeightUtils.hasExpectedInstanceFields(Schema.class,
                "struct:StructType", "schemaId:int", "identifierFieldIds:int[]",
                "highestFieldId:int", "aliasToId:BiMap", "idToField:Map", "nameToId:Map",
                "lowerCaseNameToId:Map", "idToAccessor:Map", "idToName:Map",
                "identifierFieldIdSet:Set", "idsToReassigned:Map", "idsToOriginal:Map")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(PartitionSpec.class,
                        "schema:Schema", "specId:int", "fields:PartitionField[]",
                        "fieldsBySourceId:ListMultimap", "lazyJavaClasses:Class[]",
                        "lazyPartitionType:StructType", "lazyRawPartitionType:StructType",
                        "fieldList:List", "lastAssignedFieldId:int")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(PartitionField.class,
                        "sourceId:int", "fieldId:int", "name:String", "transform:Transform")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(SortOrder.class,
                        "schema:Schema", "orderId:int", "fields:SortField[]", "fieldList:List")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(SortField.class,
                        "transform:Transform", "sourceId:int", "direction:SortDirection",
                        "nullOrder:NullOrder")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(Types.StructType.class,
                        "fields:NestedField[]", "schema:Schema", "fieldList:List",
                        "fieldsByName:Map", "fieldsByLowerCaseName:Map", "fieldsById:Map")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(Types.ListType.class,
                        "elementField:NestedField", "fields:List")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(Types.MapType.class,
                        "keyField:NestedField", "valueField:NestedField", "fields:List")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(Types.NestedField.class,
                        "isOptional:boolean", "id:int", "name:String", "type:Type",
                        "doc:String", "initialDefault:Literal", "writeDefault:Literal")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(TableMetadata.class,
                        "metadataFileLocation:String", "formatVersion:int", "uuid:String",
                        "location:String", "lastSequenceNumber:long", "lastUpdatedMillis:long",
                        "lastColumnId:int", "currentSchemaId:int", "schemas:List",
                        "defaultSpecId:int", "specs:List", "lastAssignedPartitionId:int",
                        "defaultSortOrderId:int", "sortOrders:List", "properties:Map",
                        "currentSnapshotId:long", "schemasById:Map", "specsById:Map",
                        "sortOrdersById:Map", "snapshotLog:List", "previousFiles:List",
                        "statisticsFiles:List", "partitionStatisticsFiles:List", "changes:List",
                        "nextRowId:long", "encryptionKeys:List",
                        "snapshotsSupplier:SerializableSupplier", "snapshots:List",
                        "snapshotsById:Map", "refs:Map", "snapshotsLoaded:boolean")
                && MetaCacheWeightUtils.hasExpectedInstanceFields(BASE_SNAPSHOT_CLASS_NAME, loader,
                        "snapshotId:long", "parentId:Long", "sequenceNumber:long",
                        "timestampMillis:long", "manifestListLocation:String",
                        "operation:String", "summary:Map", "schemaId:Integer",
                        "v1ManifestLocations:String[]", "firstRowId:Long", "addedRows:Long",
                        "keyId:String", "allManifests:List", "dataManifests:List",
                        "deleteManifests:List", "addedDataFiles:List", "removedDataFiles:List",
                        "addedDeleteFiles:List", "removedDeleteFiles:List");
    }

    private static long addBoxedLong(long bytes, Long value) {
        return value == null ? bytes : MetaCacheWeightUtils.saturatedAdd(bytes, LONG_BYTES);
    }

    private static long addBufferPayload(long bytes, ByteBuffer buffer) {
        return buffer == null ? bytes : MetaCacheWeightUtils.saturatedAdd(bytes, buffer.capacity());
    }

    /**
     * Account one NestedField, its owned strings and its type subtree, and record the shape data
     * the lookup-map formulas need. Canonical and short names follow Iceberg's IndexByName: a
     * nested name joins its ancestors with '.', a struct-typed list element or map value is left
     * out of its children's short names (which then become aliases), and every lower-case index
     * lower-cases each entry.
     */
    private static long addFieldPayload(
            long bytes, Types.NestedField field, PathState ancestors, FieldKind kind,
            AccountingBudget budget, SchemaShape shape) {
        budget.chargeElements(1L);
        budget.chargeCharacters(field.name().length());
        String name = field.name();
        String lowerName = name.toLowerCase(Locale.ROOT);
        boolean nameLatin1 = MetaCacheWeightUtils.isLatin1String(name);
        boolean lowerLatin1 = MetaCacheWeightUtils.isLatin1String(lowerName);
        shape.addField(field.fieldId(), ancestors, name, nameLatin1, lowerName, lowerLatin1);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, NESTED_FIELD_BYTES);
        if (kind == FieldKind.STRUCT_FIELD) {
            // List element and map key/value fields are named by shared "element"/"key"/"value"
            // literals inside Iceberg's type constructors.
            bytes = addString(bytes, name);
        }
        bytes = addString(bytes, field.doc());
        bytes = addDefaultPayload(bytes, field.initialDefault());
        bytes = addDefaultPayload(bytes, field.writeDefault());
        boolean pushShortName = kind == FieldKind.STRUCT_FIELD || kind == FieldKind.MAP_KEY
                || !field.type().isStructType();
        // Only fields nested through a chain of struct fields get accessors; anything below a
        // list or map does not.
        boolean structChildren = kind == FieldKind.STRUCT_FIELD && field.type().isStructType();
        PathState children = ancestors.push(name.length(), nameLatin1, lowerName.length(),
                lowerLatin1, pushShortName, structChildren);
        return addTypePayload(bytes, field.type(), children, budget, shape);
    }

    private static long addTypePayload(
            long bytes, Type type, PathState ancestors, AccountingBudget budget,
            SchemaShape shape) {
        if (ancestors.typeDepth > MAX_TYPE_ACCOUNTING_DEPTH) {
            throw new IllegalStateException(
                    "Iceberg cache accounting type depth exceeded");
        }
        budget.chargeElements(1L);
        if (type.isStructType()) {
            List<Types.NestedField> fields = type.asStructType().fields();
            // A nested struct's fieldList is materialized by every visitor. Its own name/id
            // lookup indexes and asSchema() are not reserved: read paths resolve nested names
            // through the root Schema maps and Binder binds only root and partition structs;
            // nested-column DDL runs against a freshly loaded live table, not a cached one.
            shape.addTypeObject(MetaCacheWeightUtils.saturatedAdd(
                    MetaCacheWeightUtils.saturatedAdd(STRUCT_TYPE_BYTES,
                            MetaCacheWeightUtils.estimatedObjectArrayBytes(fields.size())),
                    immutableListBytes(fields.size())));
            for (Types.NestedField field : fields) {
                bytes = addFieldPayload(
                        bytes, field, ancestors, FieldKind.STRUCT_FIELD, budget, shape);
            }
        } else if (type.isListType()) {
            shape.addTypeObject(MetaCacheWeightUtils.saturatedAdd(
                    LIST_TYPE_BYTES, IMMUTABLE_LIST_BYTES));
            bytes = addFieldPayload(bytes, type.asListType().fields().get(0),
                    ancestors, FieldKind.LIST_ELEMENT, budget, shape);
        } else if (type.isMapType()) {
            shape.addTypeObject(MetaCacheWeightUtils.saturatedAdd(
                    MetaCacheWeightUtils.saturatedAdd(MAP_TYPE_BYTES, IMMUTABLE_LIST_BYTES),
                    MetaCacheWeightUtils.estimatedObjectArrayBytes(2L)));
            bytes = addFieldPayload(bytes, type.asMapType().fields().get(0),
                    ancestors, FieldKind.MAP_KEY, budget, shape);
            bytes = addFieldPayload(bytes, type.asMapType().fields().get(1),
                    ancestors, FieldKind.MAP_VALUE, budget, shape);
        } else if (type instanceof Types.DecimalType) {
            shape.addTypeObject(DECIMAL_TYPE_BYTES);
        } else if (type instanceof Types.FixedType) {
            shape.addTypeObject(FIXED_TYPE_BYTES);
        } else if (type instanceof Types.GeometryType) {
            shape.addTypeObject(MetaCacheWeightUtils.saturatedAdd(GEOMETRY_TYPE_BYTES,
                    MetaCacheWeightUtils.estimatedStringBytes(
                            ((Types.GeometryType) type).crs())));
        } else if (type instanceof Types.GeographyType) {
            shape.addTypeObject(MetaCacheWeightUtils.saturatedAdd(GEOGRAPHY_TYPE_BYTES,
                    MetaCacheWeightUtils.estimatedStringBytes(
                            ((Types.GeographyType) type).crs())));
        }
        // Other primitive types are shared singletons.
        return bytes;
    }

    /** Account the int[] and lazy ImmutableSet retained by Schema.identifierFieldIds(). */
    private static long addIdentifierFieldPayload(long bytes, Set<Integer> fieldIds) {
        long count = fieldIds.size();
        if (count == 0L) {
            return bytes;
        }
        long uncachedIds = 0L;
        for (int fieldId : fieldIds) {
            if (isUncachedInteger(fieldId)) {
                uncachedIds++;
            }
        }
        // The int[] grows from the empty array of a schema without identifier fields.
        long additions = MetaCacheWeightUtils.estimatedIntArrayPayloadBytes(count);
        additions = addCount(additions, uncachedIds, INTEGER_BYTES);
        if (count == 1L) {
            // ImmutableSet.copyOf(one element) is a SingletonImmutableSet.
            return MetaCacheWeightUtils.saturatedAdd(bytes,
                    MetaCacheWeightUtils.saturatedAdd(additions, SINGLETON_IMMUTABLE_SET_BYTES));
        }
        // RegularImmutableSet: the set object, its dense elements array and open-addressing table.
        // The shared empty set of an identifier-free schema stays reachable through other schemas,
        // so nothing is subtracted for it.
        additions = MetaCacheWeightUtils.saturatedAdd(additions, REGULAR_IMMUTABLE_SET_BYTES);
        additions = MetaCacheWeightUtils.saturatedAdd(
                additions, MetaCacheWeightUtils.estimatedObjectArrayBytes(count));
        additions = MetaCacheWeightUtils.saturatedAdd(additions,
                MetaCacheWeightUtils.estimatedObjectArrayBytes(
                        immutableSetTableCapacity(count)));
        return MetaCacheWeightUtils.saturatedAdd(bytes, additions);
    }

    private static long immutableSetTableCapacity(long size) {
        long capacity = 2L;
        while (MetaCacheWeightUtils.saturatedMultiply(size, 10L)
                > MetaCacheWeightUtils.saturatedMultiply(capacity, 7L)) {
            capacity = MetaCacheWeightUtils.saturatedMultiply(capacity, 2L);
            if (capacity == Long.MAX_VALUE) {
                return capacity;
            }
        }
        return capacity;
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

    private static long immutableMapTableCapacity(long size) {
        long capacity = Long.highestOneBit(size);
        return MetaCacheWeightUtils.saturatedMultiply(size, 5L)
                > MetaCacheWeightUtils.saturatedMultiply(capacity, 6L)
                ? MetaCacheWeightUtils.saturatedMultiply(capacity, 2L) : capacity;
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

    /**
     * Hard bound on publication-time estimator work. Elements bound the number of metadata
     * objects visited; characters bound the String scanning (lower-casing) performed for
     * lazy-index reservations. Exceeding either throws, which estimateSafely turns into an
     * incomplete estimate: weighted admission is rejected but the load itself succeeds.
     */
    private static final class AccountingBudget {
        private long remainingElements;
        private long remainingCharacters;

        private AccountingBudget(long elements, long characters) {
            this.remainingElements = elements;
            this.remainingCharacters = characters;
        }

        private void chargeElements(long elements) {
            if (elements < 0L || elements > remainingElements) {
                throw new IllegalStateException("Iceberg cache accounting work budget exceeded");
            }
            remainingElements -= elements;
        }

        private void chargeCharacters(long characters) {
            if (characters < 0L || characters > remainingCharacters) {
                throw new IllegalStateException(
                        "Iceberg cache accounting character budget exceeded");
            }
            remainingCharacters -= characters;
        }
    }

    private enum FieldKind {
        STRUCT_FIELD, LIST_ELEMENT, MAP_KEY, MAP_VALUE
    }

    /**
     * Immutable name-stack state of a field's ancestors: character counts and Latin-1 coders of
     * the joined canonical path, the short-alias path and both lower-cased forms.
     */
    private static final class PathState {
        private static final PathState ROOT = new PathState(
                -1L, true, -1L, true, -1L, true, -1L, true, 0, 0);

        private final long pathCharacters;
        private final boolean pathLatin1;
        private final long shortPathCharacters;
        private final boolean shortPathLatin1;
        private final long lowerPathCharacters;
        private final boolean lowerPathLatin1;
        private final long shortLowerPathCharacters;
        private final boolean shortLowerPathLatin1;
        // Struct-field ancestors of the next field, or -1 inside a list or map (no accessors).
        private final int structDepth;
        private final int typeDepth;

        private PathState(long pathCharacters, boolean pathLatin1, long shortPathCharacters,
                boolean shortPathLatin1, long lowerPathCharacters, boolean lowerPathLatin1,
                long shortLowerPathCharacters, boolean shortLowerPathLatin1, int structDepth,
                int typeDepth) {
            this.pathCharacters = pathCharacters;
            this.pathLatin1 = pathLatin1;
            this.shortPathCharacters = shortPathCharacters;
            this.shortPathLatin1 = shortPathLatin1;
            this.lowerPathCharacters = lowerPathCharacters;
            this.lowerPathLatin1 = lowerPathLatin1;
            this.shortLowerPathCharacters = shortLowerPathCharacters;
            this.shortLowerPathLatin1 = shortLowerPathLatin1;
            this.structDepth = structDepth;
            this.typeDepth = typeDepth;
        }

        private boolean isRoot() {
            return pathCharacters < 0L;
        }

        private boolean shortPathDiffers() {
            return shortPathCharacters != pathCharacters;
        }

        /**
         * Push a field name for its children; the short name is pushed only when requested and
         * accessor depth continues only for the children of a struct-typed struct field.
         */
        private PathState push(long nameCharacters, boolean nameLatin1, long lowerCharacters,
                boolean lowerLatin1, boolean pushShortName, boolean structChildren) {
            return new PathState(
                    join(pathCharacters, nameCharacters), pathLatin1 && nameLatin1,
                    pushShortName ? join(shortPathCharacters, nameCharacters) : shortPathCharacters,
                    pushShortName ? shortPathLatin1 && nameLatin1 : shortPathLatin1,
                    join(lowerPathCharacters, lowerCharacters), lowerPathLatin1 && lowerLatin1,
                    pushShortName ? join(shortLowerPathCharacters, lowerCharacters)
                            : shortLowerPathCharacters,
                    pushShortName ? shortLowerPathLatin1 && lowerLatin1 : shortLowerPathLatin1,
                    structChildren && structDepth >= 0 ? structDepth + 1 : -1, typeDepth + 1);
        }

        private static long join(long parentCharacters, long nameCharacters) {
            return parentCharacters < 0L ? nameCharacters
                    : MetaCacheWeightUtils.saturatedAdd(
                            MetaCacheWeightUtils.saturatedAdd(parentCharacters, 1L),
                            nameCharacters);
        }
    }

    /** Cardinalities and generated-String bytes that size a schema's lookup indexes. */
    private static final class SchemaShape {
        private long fieldCount;
        private long topLevelFieldCount;
        private long uncachedFieldIdCount;
        private long uncachedTopLevelFieldIdCount;
        private long nameEntryCount;
        private long uncachedNameIdCount;
        // One copy each; the formulas add a copy per index that retains it.
        private long pathStringBytes;
        private long aliasStringBytes;
        private long lowerCaseStringBytes;
        private long topLevelLowerCaseStringBytes;
        private long accessorFieldCount;
        private long uncachedAccessorIdCount;
        private long wrappedAccessorCount;
        private long typeObjectBytes;

        /** A flat struct of {@code fieldCount} top-level fields, as used by partition types. */
        private static SchemaShape flat(
                long fieldCount, long uncachedFieldIds, long lowerCaseStringBytes) {
            SchemaShape shape = new SchemaShape();
            shape.fieldCount = fieldCount;
            shape.topLevelFieldCount = fieldCount;
            shape.uncachedFieldIdCount = uncachedFieldIds;
            shape.uncachedTopLevelFieldIdCount = uncachedFieldIds;
            shape.nameEntryCount = fieldCount;
            shape.uncachedNameIdCount = uncachedFieldIds;
            shape.lowerCaseStringBytes = lowerCaseStringBytes;
            shape.topLevelLowerCaseStringBytes = lowerCaseStringBytes;
            shape.accessorFieldCount = fieldCount;
            shape.uncachedAccessorIdCount = uncachedFieldIds;
            return shape;
        }

        private void addField(int fieldId, PathState ancestors, String name, boolean nameLatin1,
                String lowerName, boolean lowerLatin1) {
            boolean uncached = isUncachedInteger(fieldId);
            fieldCount++;
            nameEntryCount++;
            if (uncached) {
                uncachedFieldIdCount++;
                uncachedNameIdCount++;
            }
            if (ancestors.structDepth >= 0) {
                accessorFieldCount++;
                wrappedAccessorCount = MetaCacheWeightUtils.saturatedAdd(
                        wrappedAccessorCount, ancestors.structDepth);
                if (uncached) {
                    uncachedAccessorIdCount++;
                }
            }
            if (ancestors.isRoot()) {
                topLevelFieldCount++;
                if (uncached) {
                    uncachedTopLevelFieldIdCount++;
                }
                if (!name.equals(lowerName)) {
                    // Lower-case indexes only allocate when toLowerCase() changes the name.
                    long lowerBytes = MetaCacheWeightUtils.estimatedGeneratedStringBytes(
                            lowerName.length(), lowerLatin1);
                    lowerCaseStringBytes = MetaCacheWeightUtils.saturatedAdd(
                            lowerCaseStringBytes, lowerBytes);
                    topLevelLowerCaseStringBytes = MetaCacheWeightUtils.saturatedAdd(
                            topLevelLowerCaseStringBytes, lowerBytes);
                }
                return;
            }
            // Nested: IndexByName joins a new canonical String, and the lower-case index keeps
            // either that joined String or its lower-cased copy.
            pathStringBytes = MetaCacheWeightUtils.saturatedAdd(pathStringBytes,
                    MetaCacheWeightUtils.estimatedGeneratedStringBytes(
                            PathState.join(ancestors.pathCharacters, name.length()),
                            ancestors.pathLatin1 && nameLatin1));
            lowerCaseStringBytes = MetaCacheWeightUtils.saturatedAdd(lowerCaseStringBytes,
                    MetaCacheWeightUtils.estimatedGeneratedStringBytes(
                            PathState.join(ancestors.lowerPathCharacters, lowerName.length()),
                            ancestors.lowerPathLatin1 && lowerLatin1));
            if (ancestors.shortPathDiffers()) {
                // A short alias exists whenever an ancestor was left out of the short path.
                // Iceberg drops an alias that collides with a canonical name; counting the rare
                // collision is conservative and avoids building name sets at publication.
                nameEntryCount++;
                if (uncached) {
                    uncachedNameIdCount++;
                }
                aliasStringBytes = MetaCacheWeightUtils.saturatedAdd(aliasStringBytes,
                        MetaCacheWeightUtils.estimatedGeneratedStringBytes(
                                PathState.join(ancestors.shortPathCharacters, name.length()),
                                ancestors.shortPathLatin1 && nameLatin1));
                lowerCaseStringBytes = MetaCacheWeightUtils.saturatedAdd(lowerCaseStringBytes,
                        MetaCacheWeightUtils.estimatedGeneratedStringBytes(
                                PathState.join(
                                        ancestors.shortLowerPathCharacters, lowerName.length()),
                                ancestors.shortLowerPathLatin1 && lowerLatin1));
            }
        }

        private void addTypeObject(long bytes) {
            typeObjectBytes = MetaCacheWeightUtils.saturatedAdd(typeObjectBytes, bytes);
        }
    }
}
