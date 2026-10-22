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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;

import com.google.common.collect.ImmutableSet;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Compatibility checks for the pinned paimon-rust reader, beyond storage capabilities. */
final class PaimonRustReaderCapabilities {
    private static final Set<String> SUPPORTED_AGGREGATE_NAMES = ImmutableSet.of(
            "sum", "product", "min", "max", "last_value", "first_value", "last_non_null_value",
            "first_non_null_value", "first_not_null_value", "bool_and", "bool_or", "listagg");
    private final FileStoreTable table;
    private final TableSchema schema;
    private final boolean tableCompatible;

    // Whether this is a primary-key table with the partial-update or aggregation merge
    // engine: the engines whose writers historically retained repeated INSERT keys within
    // one file, driving the merge reader's unbounded batch retention.
    private final boolean puAggPrimaryKeyEngine;
    private final Map<Long, Boolean> compatibleFileSchemas = new ConcurrentHashMap<>();

    PaimonRustReaderCapabilities(FileStoreTable table, TupleDescriptor tuple) {
        this.table = table;
        this.schema = table.schema();
        this.tableCompatible = schema != null && hasCompatibleSchemaVersion(schema)
                && hasFullNestedProjection(tuple) && hasCompatibleAggregates(schema);
        // The merge-engine gate is split-level, not table-level: it exists for the merge
        // reader's unbounded losing-batch retention, which only a split that must merge
        // can trigger. See canRead.
        this.puAggPrimaryKeyEngine = schema != null && hasPuAggPrimaryKeyEngine(schema);
    }

    boolean canRead(DataSplit split) {
        if (!tableCompatible) {
            return false;
        }
        // paimon-rust's read_pk dispatch (pk_split_needs_merge / is_fully_materialized_pk_dv):
        // a rawConvertible split whose files are all compacted (level != 0) never reaches
        // the pinned merge reader. It streams through DataFileReader's raw path instead --
        // one file at a time, each bounded by read.batch-size, with the split's
        // deletion-vector factory masking stale rows per file. Both gates below exist
        // solely for that merge reader, whose losing-input-batch retention no delete count
        // or batch size can bound, so they must not judge a split that never merges.
        boolean readsRawWithoutMerge = split.rawConvertible()
                && split.dataFiles().stream().allMatch(file -> file.level() != 0);
        // Older partial-update/aggregate writers retained repeated INSERT keys within one
        // file, so even a single uncompacted file can drive unbounded merge retention.
        if (puAggPrimaryKeyEngine && !readsRawWithoutMerge) {
            return false;
        }
        // Across multiple files the same retention accumulates the whole split's inputs
        // until an output batch fills.
        if (!schema.primaryKeys().isEmpty() && !readsRawWithoutMerge
                && split.dataFiles().size() > 1) {
            return false;
        }
        for (DataFileMeta file : split.dataFiles()) {
            // Sort-merge retains consumed batches until it emits enough rows. Retracts can
            // produce an unbounded zero-output prefix; aggregation also rejects retracts.
            // Unknown counts must stay on JNI, including old files without this statistic.
            if (!schema.primaryKeys().isEmpty() && file.deleteRowCount().orElse(-1L) != 0L) {
                return false;
            }
            if (file.schemaId() != schema.id() && !compatibleFileSchemas.computeIfAbsent(
                    file.schemaId(), this::hasCompatibleFileSchema)) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasFullNestedProjection(TupleDescriptor tuple) {
        for (SlotDescriptor slot : tuple.getSlots()) {
            // The ABI projects only root names, while Arrow struct SerDes bind by ordinal.
            // A pruned slot must use JNI's recursive read type, even inside arrays or maps.
            if (slot.getType().isComplexType() && slot.getColumn() != null
                    && !slot.getType().equals(slot.getColumn().getType())) {
                return false;
            }
        }
        return true;
    }

    private boolean hasCompatibleFileSchema(long id) {
        try {
            TableSchema fileSchema = table.schemaManager().schema(id);
            // Match historical types by ID: renames and added fields do not require value casts.
            return fileSchema != null && hasCompatibleSchemaVersion(fileSchema)
                    && !hasIncompatibleEvolution(fileSchema.fields(), schema.fields());
        } catch (RuntimeException e) {
            // Failure to establish compatibility must not opt a historical file into Rust.
            return false;
        }
    }

    private static boolean hasCompatibleSchemaVersion(TableSchema schema) {
        // Java defaults a missing JSON version to 1; Rust requires that field when opening
        // both current and historical schemas. Metadata cannot distinguish an explicit 1.
        return schema.version() > TableSchema.PAIMON_07_VERSION;
    }

    private static boolean hasPuAggPrimaryKeyEngine(TableSchema schema) {
        CoreOptions.MergeEngine engine = new CoreOptions(schema.options()).mergeEngine();
        return !schema.primaryKeys().isEmpty()
                && (engine == CoreOptions.MergeEngine.PARTIAL_UPDATE
                        || engine == CoreOptions.MergeEngine.AGGREGATE);
    }

    private static boolean hasIncompatibleEvolution(List<DataField> oldFields, List<DataField> newFields) {
        Map<Integer, DataType> oldTypes = new HashMap<>();
        for (DataField field : oldFields) {
            oldTypes.put(field.id(), field.type());
        }
        for (DataField field : newFields) {
            DataType oldType = oldTypes.get(field.id());
            if (oldType != null && hasIncompatibleEvolution(oldType, field.type())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasIncompatibleEvolution(DataType oldType, DataType newType) {
        if (oldType.equalsIgnoreNullable(newType)) {
            return false;
        }
        if (oldType instanceof RowType && newType instanceof RowType) {
            return hasIncompatibleEvolution(((RowType) oldType).getFields(), ((RowType) newType).getFields());
        }
        if (oldType instanceof ArrayType && newType instanceof ArrayType) {
            return hasIncompatibleEvolution(((ArrayType) oldType).getElementType(),
                    ((ArrayType) newType).getElementType());
        }
        if (oldType instanceof MapType && newType instanceof MapType) {
            MapType oldMap = (MapType) oldType;
            MapType newMap = (MapType) newType;
            return hasIncompatibleEvolution(oldMap.getKeyType(), newMap.getKeyType())
                    || hasIncompatibleEvolution(oldMap.getValueType(), newMap.getValueType());
        }
        DataTypeRoot oldRoot = oldType.getTypeRoot();
        DataTypeRoot newRoot = newType.getTypeRoot();
        int oldWidth = integerWidth(oldRoot);
        int newWidth = integerWidth(newRoot);
        if (oldWidth > 0) {
            return !((newWidth >= oldWidth) || newRoot == DataTypeRoot.FLOAT
                    || newRoot == DataTypeRoot.DOUBLE || newRoot == DataTypeRoot.DECIMAL);
        }
        if (oldRoot == DataTypeRoot.FLOAT && newRoot == DataTypeRoot.DOUBLE) {
            return false;
        }
        if (oldRoot == DataTypeRoot.DECIMAL && newRoot == DataTypeRoot.DECIMAL) {
            return false;
        }
        if ((oldRoot == DataTypeRoot.CHAR || oldRoot == DataTypeRoot.VARCHAR)
                && newRoot == DataTypeRoot.VARCHAR) {
            return DataTypeChecks.getLength(oldType) > DataTypeChecks.getLength(newType);
        }
        if (oldRoot == newRoot && (newRoot == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || newRoot == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            int oldPrecision = DataTypeChecks.getPrecision(oldType);
            int newPrecision = DataTypeChecks.getPrecision(newType);
            // Arrow's nanosecond range is narrower than historical millis/micros; Java
            // preserves out-of-range dates while Rust's checked cast turns them into NULL.
            return oldPrecision > newPrecision || (oldPrecision <= 6 && newPrecision > 6);
        }
        // Only established cast equivalence may use Rust. Java's temporal casts use the
        // default zone and truncate negative epochs; its string casts format/pad values.
        // Arrow differs there, in numeric narrowing/decimal rounding, and in supported
        // constructed casts. This allowlist applies at every nested leaf and map key.
        return true;
    }

    private static int integerWidth(DataTypeRoot type) {
        switch (type) {
            case TINYINT:
                return 8;
            case SMALLINT:
                return 16;
            case INTEGER:
                return 32;
            case BIGINT:
                return 64;
            default:
                return 0;
        }
    }

    private static boolean hasCompatibleAggregates(TableSchema schema) {
        Map<String, String> options = schema.options();
        CoreOptions.MergeEngine engine = new CoreOptions(options).mergeEngine();
        if (engine != CoreOptions.MergeEngine.AGGREGATE && engine != CoreOptions.MergeEngine.PARTIAL_UPDATE) {
            return true;
        }
        // Rust validates every configured name before resolving overrides or excluding keys
        // and sequence fields. Java may accept an unused SPI function that Rust cannot open.
        for (Map.Entry<String, String> option : options.entrySet()) {
            String key = option.getKey();
            if (("fields.default-aggregate-function".equals(key)
                    || (key.startsWith("fields.") && key.endsWith(".aggregate-function")))
                    && !SUPPORTED_AGGREGATE_NAMES.contains(option.getValue())) {
                return false;
            }
        }
        Set<String> sequenceFields = new HashSet<>();
        if (engine == CoreOptions.MergeEngine.PARTIAL_UPDATE) {
            for (String key : options.keySet()) {
                if (key.startsWith("fields.") && key.endsWith(".sequence-group")) {
                    sequenceFields.addAll(Arrays.asList(key.substring("fields.".length(),
                            key.length() - ".sequence-group".length()).split(",")));
                }
            }
        }
        // Validate values and types, not just option keys. Java's SPI includes functions
        // such as collect which the pinned Rust aggregator factory does not implement.
        for (DataField field : schema.fields()) {
            if (schema.primaryKeys().contains(field.name()) || sequenceFields.contains(field.name())) {
                continue;
            }
            String function = options.get("fields." + field.name() + ".aggregate-function");
            // Partial-update applies the default too, except to keys and sequence fields.
            // A field override must win before checking the effective function's type support.
            if (function == null) {
                function = options.get("fields.default-aggregate-function");
            }
            if (function != null && !supportsAggregate(function, field.type())) {
                return false;
            }
        }
        return true;
    }

    private static boolean supportsAggregate(String function, DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (function) {
            case "sum":
                // Java wraps integer arithmetic and retains compact-decimal intermediate
                // sums beyond declared precision. Rust errors or resets the accumulator.
                return root == DataTypeRoot.FLOAT || root == DataTypeRoot.DOUBLE
                        || (type instanceof DecimalType && ((DecimalType) type).getPrecision() > 18);
            case "product":
                return root == DataTypeRoot.FLOAT || root == DataTypeRoot.DOUBLE;
            case "min":
            case "max":
                return integerWidth(root) != 0 || root == DataTypeRoot.FLOAT || root == DataTypeRoot.DOUBLE
                        || root == DataTypeRoot.DECIMAL || root == DataTypeRoot.DATE
                        || root == DataTypeRoot.TIME_WITHOUT_TIME_ZONE
                        || root == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                        || root == DataTypeRoot.CHAR || root == DataTypeRoot.VARCHAR;
            case "bool_and":
            case "bool_or":
                return root == DataTypeRoot.BOOLEAN;
            case "listagg":
                return type instanceof VarCharType && ((VarCharType) type).getLength() == Integer.MAX_VALUE;
            case "last_value":
            case "first_value":
            case "last_non_null_value":
            case "first_non_null_value":
            case "first_not_null_value":
                return true;
            default:
                return false;
        }
    }
}
