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

import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;

/**
 * Publication-time approximate weight formulas for Paimon table handles and snapshot projections.
 *
 * <p>The formulas follow a coarse cardinality model: a rounded-up constant per stable logical
 * dimension (schema field, logical type node, option, key, partition, ...) plus the
 * skew-sensitive string payload the loader already materialized. The per-node constants absorb
 * the lazy state Paimon materializes after admission (the four RowType lookup maps, the store
 * graph and its derived RowType copies) instead of modeling those objects individually, so
 * weights track metadata size without depending on SDK-private layouts. {@code max-weight} is
 * an estimated admission budget, not an exact heap limit. Estimation never opens the table
 * store and performs no IO.
 */
final class PaimonCacheSizeEstimator {
    // Bounds accounting work per publication; far above real schemas and option maps.
    private static final long MAX_TABLE_ACCOUNTING_ELEMENTS = 50_000L;
    private static final int MAX_TYPE_ACCOUNTING_DEPTH = 256;

    private static final long KEY_BASE_WEIGHT = 256L;
    private static final long SNAPSHOT_BASE_WEIGHT = 4L * 1024L;
    private static final long TABLE_BASE_WEIGHT = 16L * 1024L;
    // PaimonTableCacheValue and its size-estimate holder.
    private static final long TABLE_VALUE_BASE_WEIGHT = 128L;
    // One schema field (DataField) at any depth, including its share of the enclosing RowType's
    // four lazily built lookup maps, boxed ids and the field copies the lazily created store
    // graph derives from it (append-only row copy, trimmed key/value types, merge row type).
    private static final long FIELD_NODE_WEIGHT = 576L;
    // One bare nested type node (array/map/multiset/vector element types and unknown future
    // DataType implementations), including its store-graph copies; charged generically instead
    // of disabling weighted caching for unknown types.
    private static final long TYPE_NODE_WEIGHT = 128L;
    // One nested RowType container: the RowType, its field list, its four lazily built lookup
    // maps and the container copies the store graph derives.
    private static final long ROW_CONTAINER_WEIGHT = 2048L;
    // One schema/table-level entry: option map node, key list slot and boxes.
    private static final long OPTION_WEIGHT = 192L;
    private static final long KEY_WEIGHT = 192L;
    // Wrapper tables (privileged, fallback-read) around the concrete FileStoreTable.
    private static final long WRAPPER_WEIGHT = 512L;
    // Each concrete FileStoreTable strongly owns its FileIO graph (configuration, and for REST
    // catalogs the lazily fetched, periodically rotated vended token). Paimon's FileIO exposes
    // no IO-free way to read that state, so a generous fixed allowance is charged per owner:
    // rotation replaces the retained token rather than growing it, and typical token/config
    // payloads stay far below this bound.
    private static final long FILE_IO_WEIGHT = 16L * 1024L;
    private static final long PARTITION_WEIGHT = 320L;
    private static final long PARTITION_ITEM_WEIGHT = 768L;

    private PaimonCacheSizeEstimator() {
    }

    /**
     * Retained weight of the base table entry. The table handle is owned independently of the
     * snapshot projections that reference it (they may pin an older generation), so the same
     * table graph is charged to both owners rather than shared.
     */
    static MetaCacheSizeEstimate estimateTableEntry(NameMapping key, PaimonTableCacheValue value) {
        String unsupported = unsupportedReason(value.getPaimonTable());
        if (unsupported != null) {
            return MetaCacheSizeEstimate.incomplete(unsupported);
        }
        long bytes = MetaCacheWeightUtils.saturatedAdd(
                KEY_BASE_WEIGHT, MetaCacheWeightUtils.estimatedNameMappingBytes(key));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, TABLE_VALUE_BASE_WEIGHT);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, value.getRetainedTablePayloadBytes());
        return MetaCacheSizeEstimate.complete(
                MetaCacheWeightUtils.saturatedAdd(bytes, estimateTable(value.getPaimonTable())));
    }

    static MetaCacheSizeEstimate estimateSnapshotEntry(
            PaimonSnapshotEntryKey key, PaimonSnapshotCacheValue value) {
        Table table = value.getSnapshot().getTable();
        String unsupported = unsupportedReason(table);
        if (unsupported != null) {
            return MetaCacheSizeEstimate.incomplete(unsupported);
        }
        long bytes = MetaCacheWeightUtils.saturatedAdd(
                KEY_BASE_WEIGHT, MetaCacheWeightUtils.estimatedNameMappingBytes(key.getNameMapping()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, SNAPSHOT_BASE_WEIGHT);
        bytes = addCount(bytes, value.getPartitionInfo().getNameToPartition().size(), PARTITION_WEIGHT);
        bytes = addCount(bytes,
                value.getPartitionInfo().getNameToPartitionItem().size(), PARTITION_ITEM_WEIGHT);
        bytes = MetaCacheWeightUtils.saturatedAdd(
                bytes, value.getPartitionInfo().getRetainedPayloadBytes());
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, value.getRetainedTablePayloadBytes());
        return MetaCacheSizeEstimate.complete(
                MetaCacheWeightUtils.saturatedAdd(bytes, estimateTable(table)));
    }

    private static String unsupportedReason(Table table) {
        if (table == null) {
            return "unsupported_paimon_table:null";
        }
        if (unwrap(table) == null) {
            return "unsupported_paimon_table:" + table.getClass().getName();
        }
        return null;
    }

    /** The concrete FileStoreTable behind any known wrapper chain, or null. */
    private static FileStoreTable unwrap(Table table) {
        if (table instanceof DelegatedFileStoreTable) {
            return unwrap(((DelegatedFileStoreTable) table).wrapped());
        }
        return table instanceof FileStoreTable ? (FileStoreTable) table : null;
    }

    /** Uses TableSchema cardinalities only and deliberately never calls FileStoreTable.store(). */
    private static long estimateTable(Table table) {
        long bytes = 0L;
        Table current = table;
        while (true) {
            if (current instanceof FallbackReadFileStoreTable) {
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes, WRAPPER_WEIGHT);
                bytes = MetaCacheWeightUtils.saturatedAdd(
                        bytes, estimateTable(((FallbackReadFileStoreTable) current).other()));
                current = ((FallbackReadFileStoreTable) current).wrapped();
                continue;
            }
            if (current instanceof DelegatedFileStoreTable) {
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes, WRAPPER_WEIGHT);
                current = ((DelegatedFileStoreTable) current).wrapped();
                continue;
            }
            break;
        }
        if (!(current instanceof FileStoreTable)) {
            return bytes;
        }
        FileStoreTable fileStoreTable = (FileStoreTable) current;
        TableSchema schema = fileStoreTable.schema();
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, TABLE_BASE_WEIGHT);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, FILE_IO_WEIGHT);
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedStringBytes(current.name()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedStringBytes(fileStoreTable.location().toString()));
        // The store graph the table lazily materializes derives several RowType copies of the
        // schema; the per-node constants absorb those copies instead of modeling store classes.
        NodeCounts nodes = new NodeCounts();
        countFieldNodes(schema.fields(), 0, nodes,
                new AccountingBudget(MAX_TABLE_ACCOUNTING_ELEMENTS));
        bytes = addCount(bytes, nodes.fieldNodes, FIELD_NODE_WEIGHT);
        bytes = addCount(bytes, nodes.bareTypeNodes, TYPE_NODE_WEIGHT);
        bytes = addCount(bytes, nodes.rowContainers, ROW_CONTAINER_WEIGHT);
        bytes = addCount(bytes, schema.options().size(), OPTION_WEIGHT);
        bytes = addCount(bytes, schema.partitionKeys().size(), KEY_WEIGHT);
        bytes = addCount(bytes, schema.primaryKeys().size(), KEY_WEIGHT);
        bytes = addCount(bytes, schema.bucketKeys().size(), KEY_WEIGHT);
        for (String primaryKey : schema.primaryKeys()) {
            // Each trimmed key field of a primary-key store gets a fresh "_KEY_" + name string.
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                    MetaCacheWeightUtils.estimatedStringBytes(primaryKey));
        }
        return bytes;
    }

    private static final class NodeCounts {
        private long fieldNodes;
        private long bareTypeNodes;
        private long rowContainers;
    }

    private static void countFieldNodes(
            List<DataField> fields, int depth, NodeCounts counts, AccountingBudget budget) {
        if (depth > MAX_TYPE_ACCOUNTING_DEPTH) {
            throw new IllegalStateException("Paimon schema type nesting is too deep");
        }
        for (DataField field : fields) {
            budget.charge(1L);
            counts.fieldNodes = MetaCacheWeightUtils.saturatedAdd(counts.fieldNodes, 1L);
            countTypeNodes(field.type(), depth, counts, budget);
        }
    }

    private static void countTypeNodes(
            DataType type, int depth, NodeCounts counts, AccountingBudget budget) {
        if (depth > MAX_TYPE_ACCOUNTING_DEPTH) {
            throw new IllegalStateException("Paimon schema type nesting is too deep");
        }
        if (type instanceof RowType) {
            counts.rowContainers = MetaCacheWeightUtils.saturatedAdd(counts.rowContainers, 1L);
            countFieldNodes(((RowType) type).getFields(), depth + 1, counts, budget);
            return;
        }
        if (type != null) {
            // Known container types (array, map, multiset) contribute their children as nodes;
            // other types, including future implementations, are charged as a single node.
            for (DataType child : childTypes(type)) {
                budget.charge(1L);
                counts.bareTypeNodes = MetaCacheWeightUtils.saturatedAdd(counts.bareTypeNodes, 1L);
                countTypeNodes(child, depth + 1, counts, budget);
            }
        }
    }

    private static List<DataType> childTypes(DataType type) {
        if (type instanceof org.apache.paimon.types.ArrayType) {
            return java.util.Collections.singletonList(
                    ((org.apache.paimon.types.ArrayType) type).getElementType());
        }
        if (type instanceof org.apache.paimon.types.MultisetType) {
            return java.util.Collections.singletonList(
                    ((org.apache.paimon.types.MultisetType) type).getElementType());
        }
        if (type instanceof org.apache.paimon.types.MapType) {
            org.apache.paimon.types.MapType mapType = (org.apache.paimon.types.MapType) type;
            return java.util.Arrays.asList(mapType.getKeyType(), mapType.getValueType());
        }
        return java.util.Collections.emptyList();
    }

    /**
     * Captures skew-sensitive schema text once when the cache value is constructed. All
     * collections are already materialized in TableSchema; this never opens the table store.
     */
    static long retainedTablePayloadBytes(Table table) {
        return retainedTablePayloadBytes(
                table, new AccountingBudget(MAX_TABLE_ACCOUNTING_ELEMENTS));
    }

    private static long retainedTablePayloadBytes(Table table, AccountingBudget budget) {
        budget.charge(1L);
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable fallback = (FallbackReadFileStoreTable) table;
            return MetaCacheWeightUtils.saturatedAdd(
                    retainedTablePayloadBytes(fallback.wrapped(), budget),
                    retainedTablePayloadBytes(fallback.other(), budget));
        }
        if (table instanceof DelegatedFileStoreTable) {
            return retainedTablePayloadBytes(
                    ((DelegatedFileStoreTable) table).wrapped(), budget);
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
            bytes = addFieldPayload(bytes, field, budget, 0);
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

    private static long addFieldPayload(
            long bytes, DataField field, AccountingBudget budget, int depth) {
        if (depth > MAX_TYPE_ACCOUNTING_DEPTH) {
            throw new IllegalStateException("Paimon schema type nesting is too deep");
        }
        budget.charge(1L);
        bytes = addString(bytes, field.name());
        bytes = addString(bytes, field.description());
        bytes = addString(bytes, field.defaultValue());
        DataType type = field.type();
        if (type instanceof RowType) {
            for (DataField nested : ((RowType) type).getFields()) {
                bytes = addFieldPayload(bytes, nested, budget, depth + 1);
            }
        } else if (type != null) {
            for (DataType child : childTypes(type)) {
                bytes = addChildPayload(bytes, child, budget, depth + 1);
            }
        }
        return bytes;
    }

    private static long addChildPayload(
            long bytes, DataType type, AccountingBudget budget, int depth) {
        if (depth > MAX_TYPE_ACCOUNTING_DEPTH) {
            throw new IllegalStateException("Paimon schema type nesting is too deep");
        }
        budget.charge(1L);
        if (type instanceof RowType) {
            for (DataField nested : ((RowType) type).getFields()) {
                bytes = addFieldPayload(bytes, nested, budget, depth + 1);
            }
            return bytes;
        }
        if (type != null) {
            for (DataType child : childTypes(type)) {
                bytes = addChildPayload(bytes, child, budget, depth + 1);
            }
        }
        return bytes;
    }

    private static long addStrings(long bytes, List<String> values, AccountingBudget budget) {
        budget.charge(values.size());
        for (String value : values) {
            bytes = addString(bytes, value);
        }
        return bytes;
    }

    private static long addString(long bytes, String value) {
        return MetaCacheWeightUtils.saturatedAdd(
                bytes, MetaCacheWeightUtils.estimatedStringBytes(value));
    }

    private static long addCount(long bytes, long count, long perElementBytes) {
        return MetaCacheWeightUtils.saturatedAdd(
                bytes, MetaCacheWeightUtils.saturatedMultiply(count, perElementBytes));
    }

    private static final class AccountingBudget {
        private long remaining;

        private AccountingBudget(long remaining) {
            this.remaining = remaining;
        }

        private void charge(long elements) {
            if (elements < 0L || elements > remaining) {
                throw new IllegalStateException("Paimon cache accounting work budget exceeded");
            }
            remaining -= elements;
        }
    }
}
