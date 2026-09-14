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
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Publication-time approximate weight formulas for Iceberg cache entries.
 *
 * <p>The formulas follow a coarse cardinality model: a rounded-up constant per stable logical
 * dimension (snapshot, schema field, partition field, metadata entry, ...) plus the
 * skew-sensitive string payload the loader already materialized. Constants are calibrated
 * offline and deliberately absorb the lazy state Iceberg materializes after admission (schema
 * name/id/lower-case/accessor indexes, partition-type graphs) instead of modeling those objects
 * individually, so weights track metadata size without depending on SDK-private layouts.
 * {@code max-weight} is an estimated admission budget, not an exact heap limit.
 */
final class IcebergCacheSizeEstimator {
    // Every metadata element visited (field, snapshot, summary entry, ...) costs a few reads;
    // the bound only guards against pathological metadata and is far above real tables (a
    // 10,000-snapshot history with 15 summary keys each is 160,000 elements). Exceeding it
    // rejects weighted admission, so it must not be reachable by ordinary long-lived tables.
    private static final long MAX_TABLE_ACCOUNTING_ELEMENTS = 2_000_000L;
    // Total name characters the estimator may account for retained name indexes.
    private static final long MAX_TABLE_ACCOUNTING_CHARACTERS = 4_000_000L;
    private static final int MAX_TYPE_ACCOUNTING_DEPTH = 128;

    private static final long KEY_BASE_WEIGHT = 256L;
    private static final long TABLE_BASE_WEIGHT = 16L * 1024L;
    // One schema version: the Schema object, its lists, the schemasById entry and the struct it
    // wraps, including their growth from singleton to regular immutable shapes.
    private static final long SCHEMA_WEIGHT = 1024L;
    // One nested field at any depth: the NestedField and its type node plus this field's share
    // of every eager and lazy schema index (idToName, nameToId, idToField, lowerCaseNameToId,
    // idToAccessor, struct field indexes and the secondary partition-type schema graph).
    private static final long FIELD_WEIGHT = 1408L;
    // Retained copies of one field path across the case-sensitive, lower-cased and alias
    // name indexes.
    private static final long NAME_INDEX_COPIES = 5L;
    // Per nesting level of a field: accessor wrappers and the enclosing struct's index shares.
    private static final long NESTED_LEVEL_WEIGHT = 128L;
    // String object and array overhead per retained generated path copy.
    private static final long STRING_OVERHEAD_WEIGHT = 48L;
    // One partition spec: the spec object, its field list, javaClasses and the partitionType()
    // graph with its own indexes.
    private static final long SPEC_WEIGHT = 2048L;
    // One partition field including its transform, index entries and the per-field share of the
    // lazily built fieldsBySourceId multimap.
    private static final long PARTITION_FIELD_WEIGHT = 1024L;
    // The lazily built fieldsBySourceId multimap grows O(distinctSourceIds * fieldCount).
    private static final long FIELDS_BY_SOURCE_SLOT_WEIGHT = 64L;
    private static final long SORT_ORDER_WEIGHT = 512L;
    private static final long SORT_FIELD_WEIGHT = 256L;
    // One entry of any retained string map (table properties, snapshot summaries, blob or key
    // properties): map node plus boxed/list slack; the strings are charged separately.
    private static final long METADATA_ENTRY_WEIGHT = 128L;
    private static final long SNAPSHOT_WEIGHT = 512L;
    private static final long CURRENT_SNAPSHOT_WEIGHT = 1024L;
    private static final long SNAPSHOT_LOG_WEIGHT = 64L;
    private static final long METADATA_LOG_WEIGHT = 160L;
    private static final long SNAPSHOT_REF_WEIGHT = 256L;
    private static final long STATISTICS_FILE_WEIGHT = 640L;
    private static final long BLOB_METADATA_WEIGHT = 256L;
    private static final long BLOB_FIELD_WEIGHT = 32L;
    private static final long PARTITION_STATISTICS_FILE_WEIGHT = 320L;
    private static final long ENCRYPTED_KEY_WEIGHT = 320L;
    // One retained IcebergPartition (value/transform lists) or one RangePartitionItem with a
    // single partition column plus its map entry; extra columns are charged by IcebergPartitionInfo.
    private static final long PARTITION_WEIGHT = 768L;
    // Outer map entry and table share of one merged-overlap group; the alias set itself and its
    // contents are charged by IcebergPartitionInfo per enclosed partition name.
    private static final long PARTITION_ALIAS_WEIGHT = 160L;
    // One name-mapping field: map node, boxed id and list object; alias arrays and Strings are
    // charged by IcebergSnapshotCacheValue when the mapping is copied.
    private static final long NAME_MAPPING_ENTRY_WEIGHT = 256L;
    // A boxed scalar default (numeric, temporal) retained by a v3 field.
    private static final long BOXED_DEFAULT_WEIGHT = 64L;
    // The frozen operations also retain the handle's EncryptionManager; scans effectively meet
    // the shared plaintext singleton, so a small fixed allowance covers the object graph.
    private static final long ENCRYPTION_MANAGER_WEIGHT = 1024L;
    /**
     * Flat allowance for the execution context a published value retains (authenticator,
     * Hadoop authentication state, credential collections). The graph is shared by every value
     * of the catalog generation; each retaining value carries the rounded-up allowance so a
     * retired generation kept alive only by old cached values is never entirely unaccounted.
     */
    private static final long AUTHENTICATION_CONTEXT_WEIGHT = 16L * 1024L;
    private static final long MANIFEST_ENTRY_BASE_WEIGHT = 512L;
    private static final long DATA_FILE_WEIGHT = 1024L;
    private static final long DELETE_FILE_WEIGHT = 1024L;
    private static final long FILE_METRIC_ENTRY_WEIGHT = 128L;

    // TableMetadata.snapshots()/snapshot(id) load lazily through a catalog supplier
    // (REST snapshot-loading-mode=refs). Publication must not perform that IO, so this single
    // reflective probe is retained as an IO guard rather than a layout model.
    private static final Field TABLE_METADATA_SNAPSHOTS_LOADED_FIELD =
            loadTableMetadataField("snapshotsLoaded", boolean.class);
    private static final Field TABLE_METADATA_SNAPSHOTS_SUPPLIER_FIELD =
            loadTableMetadataField("snapshotsSupplier", null);

    private IcebergCacheSizeEstimator() {
    }

    static MetaCacheSizeEstimate estimateTableEntry(NameMapping key, IcebergTableCacheValue value) {
        Table table = value.getRetainedIcebergTable();
        MetaCacheSizeEstimate support = checkSupportedTable(table);
        if (!support.isComplete()) {
            return support;
        }
        long bytes = MetaCacheWeightUtils.saturatedAdd(
                KEY_BASE_WEIGHT, MetaCacheWeightUtils.estimatedNameMappingBytes(key));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, estimateTable(table));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, value.getRetainedTablePayloadBytes());
        bytes = MetaCacheWeightUtils.saturatedAdd(
                bytes, value.getRetainedCurrentSnapshotPayloadBytes());
        if (value.getAuthenticator() != null) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, AUTHENTICATION_CONTEXT_WEIGHT);
        }
        return MetaCacheSizeEstimate.complete(bytes);
    }

    static MetaCacheSizeEstimate estimateSnapshotEntry(
            IcebergSnapshotEntryKey key, IcebergSnapshotCacheValue value) {
        long bytes = KEY_BASE_WEIGHT;
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedNameMappingBytes(key.getNameMapping()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedStringBytes(key.getTableUuid()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedStringBytes(key.getMetadataFileLocation()));

        IcebergPartitionInfo partitionInfo = value.getPartitionInfo();
        bytes = addCount(bytes, partitionInfo.getNameToPartitionItem().size(), PARTITION_WEIGHT);
        bytes = addCount(bytes, partitionInfo.getNameToIcebergPartition().size(), PARTITION_WEIGHT);
        bytes = addCount(bytes, partitionInfo.getNameToIcebergPartitionNames().size(),
                PARTITION_ALIAS_WEIGHT);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, partitionInfo.getRetainedPayloadBytes());
        bytes = addCount(bytes, value.getNameMapping().map(Map::size).orElse(0),
                NAME_MAPPING_ENTRY_WEIGHT);
        bytes = MetaCacheWeightUtils.saturatedAdd(
                bytes, value.getRetainedNameMappingPayloadBytes());

        if (value.getRetainedIcebergTable().isPresent()) {
            // The projection keeps its own reference to the frozen table generation. That graph
            // is charged here as well as by the table entry that produced it: the two entries have
            // independent lifetimes (TTL, weight eviction, soft collection) and either may outlive
            // the other, so each must be able to carry the graph on its own. Budgets should be
            // sized for the table metadata being counted once per dependent entry.
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
        if (value.getCapturedAuthenticator() != null) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, AUTHENTICATION_CONTEXT_WEIGHT);
        }
        return MetaCacheSizeEstimate.complete(bytes);
    }

    static MetaCacheSizeEstimate estimateManifestEntry(
            IcebergManifestEntryKey key, ManifestCacheValue value) {
        if (!value.isAccountingComplete()) {
            return MetaCacheSizeEstimate.incomplete("iceberg_manifest_accounting_incomplete");
        }
        long bytes = MetaCacheWeightUtils.saturatedAdd(
                MANIFEST_ENTRY_BASE_WEIGHT,
                MetaCacheWeightUtils.estimatedStringBytes(key.getManifestPath()));
        bytes = addCount(bytes, value.getDataFiles().size(), DATA_FILE_WEIGHT);
        bytes = addCount(bytes, value.getDeleteFiles().size(), DELETE_FILE_WEIGHT);
        bytes = addCount(bytes, value.getDataFileMetricEntryCount(), FILE_METRIC_ENTRY_WEIGHT);
        bytes = addCount(bytes, value.getDeleteFileMetricEntryCount(), FILE_METRIC_ENTRY_WEIGHT);
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
                TABLE_BASE_WEIGHT, MetaCacheWeightUtils.estimatedStringBytes(table.name()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedStringBytes(metadata.location()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedStringBytes(metadata.metadataFileLocation()));
        bytes = addCount(bytes, metadata.properties().size(), METADATA_ENTRY_WEIGHT);
        if (metadata.currentSnapshot() != null) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, CURRENT_SNAPSHOT_WEIGHT);
        }
        return MetaCacheWeightUtils.saturatedAdd(bytes, fileIoBytes(table));
    }

    /**
     * The retained operations strongly own the handle's FileIO with its configuration and any
     * vended storage credentials; those maps grow independently of table metadata, so their
     * payload is charged per owner. A FileIO that cannot expose its configuration makes the
     * estimate fail closed via {@code estimateSafely}.
     */
    private static long fileIoBytes(Table table) {
        org.apache.iceberg.io.FileIO fileIo = table.io();
        if (fileIo == null) {
            return 0L;
        }
        long bytes = addStringMapWithEntries(ENCRYPTION_MANAGER_WEIGHT, fileIo.properties());
        if (fileIo instanceof org.apache.iceberg.io.SupportsStorageCredentials) {
            for (org.apache.iceberg.io.StorageCredential credential
                    : ((org.apache.iceberg.io.SupportsStorageCredentials) fileIo).credentials()) {
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes, METADATA_ENTRY_WEIGHT);
                bytes = addString(bytes, credential.prefix());
                bytes = addStringMapWithEntries(bytes, credential.config());
            }
        }
        return bytes;
    }

    private static long addStringMapWithEntries(long bytes, Map<String, String> values) {
        if (values == null) {
            return bytes;
        }
        bytes = addCount(bytes, values.size(), METADATA_ENTRY_WEIGHT);
        return addStringMap(bytes, values);
    }

    /**
     * Weight of everything a retained table generation's metadata can grow into, computed from
     * already-parsed metadata with bounded publication-time work and no IO.
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
            budget.chargeElements(MetaCacheWeightUtils.saturatedAdd(
                    1L, sortOrder.fields().size()));
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, SORT_ORDER_WEIGHT);
            bytes = addCount(bytes, sortOrder.fields().size(), SORT_FIELD_WEIGHT);
            for (org.apache.iceberg.SortField field : sortOrder.fields()) {
                bytes = addTransformPayload(bytes, field.transform(), budget);
            }
        }
        for (Schema schema : metadata.schemas()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, schemaBytes(schema, budget));
        }
        budget.chargeElements(metadata.properties().size());
        bytes = addCount(bytes, metadata.properties().size(), METADATA_ENTRY_WEIGHT);
        for (Map.Entry<String, String> property : metadata.properties().entrySet()) {
            bytes = addString(bytes, property.getKey());
            bytes = addString(bytes, property.getValue());
        }
        for (Snapshot snapshot : metadata.snapshots()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, snapshotBytes(snapshot, budget));
        }
        budget.chargeElements(metadata.snapshotLog().size());
        bytes = addCount(bytes, metadata.snapshotLog().size(), SNAPSHOT_LOG_WEIGHT);
        budget.chargeElements(metadata.previousFiles().size());
        for (TableMetadata.MetadataLogEntry previousFile : metadata.previousFiles()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, METADATA_LOG_WEIGHT);
            bytes = addString(bytes, previousFile.file());
        }
        budget.chargeElements(metadata.refs().size());
        bytes = addCount(bytes, metadata.refs().size(), SNAPSHOT_REF_WEIGHT);
        for (String refName : metadata.refs().keySet()) {
            bytes = addString(bytes, refName);
        }
        budget.chargeElements(metadata.statisticsFiles().size());
        for (StatisticsFile statisticsFile : metadata.statisticsFiles()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, STATISTICS_FILE_WEIGHT);
            bytes = addString(bytes, statisticsFile.path());
            for (BlobMetadata blob : statisticsFile.blobMetadata()) {
                budget.chargeElements(MetaCacheWeightUtils.saturatedAdd(1L,
                        MetaCacheWeightUtils.saturatedAdd(
                                blob.fields().size(), blob.properties().size())));
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes, BLOB_METADATA_WEIGHT);
                bytes = addString(bytes, blob.type());
                bytes = addCount(bytes, blob.fields().size(), BLOB_FIELD_WEIGHT);
                bytes = addStringMapWithEntries(bytes, blob.properties());
            }
        }
        budget.chargeElements(metadata.partitionStatisticsFiles().size());
        for (PartitionStatisticsFile statisticsFile : metadata.partitionStatisticsFiles()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, PARTITION_STATISTICS_FILE_WEIGHT);
            bytes = addString(bytes, statisticsFile.path());
        }
        budget.chargeElements(metadata.encryptionKeys().size());
        for (EncryptedKey encryptedKey : metadata.encryptionKeys()) {
            budget.chargeElements(encryptedKey.properties().size());
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, ENCRYPTED_KEY_WEIGHT);
            bytes = addString(bytes, encryptedKey.keyId());
            bytes = addString(bytes, encryptedKey.encryptedById());
            if (encryptedKey.encryptedKeyMetadata() != null) {
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                        MetaCacheWeightUtils.estimatedByteArrayBytes(
                                encryptedKey.encryptedKeyMetadata().remaining()));
            }
            bytes = addStringMapWithEntries(bytes, encryptedKey.properties());
        }
        bytes = addString(bytes, metadata.uuid());
        return bytes;
    }

    private static long partitionSpecBytes(PartitionSpec spec, AccountingBudget budget) {
        List<PartitionField> fields = spec.fields();
        budget.chargeElements(MetaCacheWeightUtils.saturatedAdd(1L, fields.size()));
        long bytes = SPEC_WEIGHT;
        bytes = addCount(bytes, fields.size(), PARTITION_FIELD_WEIGHT);
        Set<Integer> sourceIds = new HashSet<>();
        for (PartitionField field : fields) {
            sourceIds.add(field.sourceId());
            budget.chargeCharacters(field.name().length());
            // The name is retained by the field and again by the partition-type name indexes.
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, MetaCacheWeightUtils.saturatedMultiply(
                    MetaCacheWeightUtils.estimatedStringBytes(field.name()), NAME_INDEX_COPIES));
            bytes = addTransformPayload(bytes, field.transform(), budget);
        }
        // Reserve the O(distinctSources * fields) growth of the lazy fieldsBySourceId index.
        bytes = addCount(bytes,
                MetaCacheWeightUtils.saturatedMultiply(sourceIds.size(), fields.size()),
                FIELDS_BY_SOURCE_SLOT_WEIGHT);
        return bytes;
    }

    /**
     * One schema version: a constant per nested field at any depth plus the retained name/doc
     * payload. The per-field constant absorbs the type node and this field's share of every
     * eager and lazily materialized schema index.
     */
    private static long schemaBytes(Schema schema, AccountingBudget budget) {
        long bytes = SCHEMA_WEIGHT;
        bytes = addCount(bytes, schema.identifierFieldIds().size(), METADATA_ENTRY_WEIGHT);
        for (Types.NestedField field : schema.columns()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(
                    bytes, fieldBytes(field, budget, 0, 0L));
        }
        return bytes;
    }

    private static long fieldBytes(
            Types.NestedField field, AccountingBudget budget, int depth, long parentPathBytes) {
        if (depth > MAX_TYPE_ACCOUNTING_DEPTH) {
            throw new IllegalStateException("Iceberg schema type nesting is too deep");
        }
        budget.chargeElements(1L);
        long bytes = MetaCacheWeightUtils.saturatedAdd(FIELD_WEIGHT,
                MetaCacheWeightUtils.saturatedMultiply(depth, NESTED_LEVEL_WEIGHT));
        String name = field.name();
        long pathBytes = parentPathBytes;
        if (name != null) {
            // The lazy name indexes retain the fully qualified dotted path of every nested
            // field, so both the payload and the character work bound follow the path length,
            // not just the local name.
            pathBytes = MetaCacheWeightUtils.saturatedAdd(pathBytes,
                    MetaCacheWeightUtils.saturatedAdd(depth > 0 ? 1L : 0L,
                            MetaCacheWeightUtils.estimatedStringPayloadBytes(name)));
            budget.chargeCharacters(pathBytes);
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, MetaCacheWeightUtils.saturatedMultiply(
                    MetaCacheWeightUtils.saturatedAdd(STRING_OVERHEAD_WEIGHT, pathBytes),
                    NAME_INDEX_COPIES));
        }
        if (field.doc() != null) {
            budget.chargeCharacters(field.doc().length());
            bytes = addString(bytes, field.doc());
        }
        bytes = addDefaultPayload(bytes, field.initialDefault(), budget);
        bytes = addDefaultPayload(bytes, field.writeDefault(), budget);
        Type type = field.type();
        if (type instanceof Types.GeometryType) {
            bytes = addString(bytes, ((Types.GeometryType) type).crs());
        } else if (type instanceof Types.GeographyType) {
            bytes = addString(bytes, ((Types.GeographyType) type).crs());
        }
        if (type != null && type.isNestedType()) {
            for (Types.NestedField nested : type.asNestedType().fields()) {
                bytes = MetaCacheWeightUtils.saturatedAdd(
                        bytes, fieldBytes(nested, budget, depth + 1, pathBytes));
            }
        }
        return bytes;
    }

    /**
     * Unrecognized metadata transform tokens are preserved verbatim in retained
     * UnknownTransform objects, so the transform string payload can grow independently of the
     * field count and must be charged against the character budget.
     */
    private static long addTransformPayload(
            long bytes, org.apache.iceberg.transforms.Transform<?, ?> transform,
            AccountingBudget budget) {
        if (transform == null) {
            return bytes;
        }
        String token = String.valueOf(transform);
        budget.chargeCharacters(token.length());
        return addString(bytes, token);
    }

    /** v3 field defaults retain arbitrary scalar payloads (strings, binary, decimals). */
    private static long addDefaultPayload(long bytes, Object defaultValue, AccountingBudget budget) {
        if (defaultValue == null) {
            return bytes;
        }
        if (defaultValue instanceof CharSequence) {
            CharSequence value = (CharSequence) defaultValue;
            budget.chargeCharacters(value.length());
            return MetaCacheWeightUtils.saturatedAdd(
                    bytes, MetaCacheWeightUtils.estimatedCharSequenceBytes(value));
        }
        if (defaultValue instanceof java.nio.ByteBuffer) {
            return MetaCacheWeightUtils.saturatedAdd(bytes,
                    MetaCacheWeightUtils.estimatedByteArrayBytes(
                            ((java.nio.ByteBuffer) defaultValue).remaining()));
        }
        if (defaultValue instanceof byte[]) {
            return MetaCacheWeightUtils.saturatedAdd(bytes,
                    MetaCacheWeightUtils.estimatedByteArrayBytes(((byte[]) defaultValue).length));
        }
        return MetaCacheWeightUtils.saturatedAdd(bytes, BOXED_DEFAULT_WEIGHT);
    }

    private static long snapshotBytes(Snapshot snapshot, AccountingBudget budget) {
        Map<String, String> summary = snapshot.summary();
        budget.chargeElements(MetaCacheWeightUtils.saturatedAdd(
                1L, summary == null ? 0L : summary.size()));
        long bytes = SNAPSHOT_WEIGHT;
        bytes = addString(bytes, snapshot.operation());
        String manifestListLocation = snapshot.manifestListLocation();
        if (manifestListLocation == null) {
            // A snapshot serialized with an inline "manifests" array (legacy v1 writers) retains
            // a manifest-location list that is only exposed through FileIO-backed wrappers.
            // Reject weighted admission instead of doing IO or admitting an underestimate.
            throw new IllegalStateException(
                    "Iceberg snapshot with inline manifest list is unsupported");
        }
        bytes = addString(bytes, manifestListLocation);
        bytes = addString(bytes, snapshot.keyId());
        if (summary != null) {
            bytes = addCount(bytes, summary.size(), METADATA_ENTRY_WEIGHT);
            bytes = addStringMap(bytes, summary);
        }
        return bytes;
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

    private static long addCount(long bytes, long count, long perElementBytes) {
        return MetaCacheWeightUtils.saturatedAdd(
                bytes, MetaCacheWeightUtils.saturatedMultiply(count, perElementBytes));
    }

    private static long addString(long bytes, String value) {
        return MetaCacheWeightUtils.saturatedAdd(
                bytes, MetaCacheWeightUtils.estimatedStringBytes(value));
    }

    private static long addStringMap(long bytes, Map<String, String> values) {
        if (values == null) {
            return bytes;
        }
        for (Map.Entry<String, String> entry : values.entrySet()) {
            bytes = addString(bytes, entry.getKey());
            bytes = addString(bytes, entry.getValue());
        }
        return bytes;
    }

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
}
