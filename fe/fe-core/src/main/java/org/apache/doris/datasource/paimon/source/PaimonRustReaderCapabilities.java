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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
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
    private final FileStoreTable table;
    private final TableSchema schema;
    private final boolean tableCompatible;
    private final Map<Long, Boolean> compatibleFileSchemas = new ConcurrentHashMap<>();

    PaimonRustReaderCapabilities(FileStoreTable table, TupleDescriptor tuple) {
        this.table = table;
        this.schema = table.schema();
        this.tableCompatible = schema != null && hasFullNestedProjection(tuple)
                && hasCompatibleAggregates(schema);
    }

    boolean canRead(DataSplit split) {
        if (!tableCompatible) {
            return false;
        }
        // The pinned merge reader retains losing input batches until an output batch fills.
        // Neither zero deletes nor a small read.batch-size bounds this across multiple files.
        if (!schema.primaryKeys().isEmpty() && split.dataFiles().size() > 1) {
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
            // Java numeric-to-integer casts can wrap; Arrow may return NULL for the same value.
            // Compare IDs recursively: renames and newly added fields are not narrowing.
            return fileSchema != null && !hasIntegerNarrowing(fileSchema.fields(), schema.fields());
        } catch (RuntimeException e) {
            // Failure to establish compatibility must not opt a historical file into Rust.
            return false;
        }
    }

    private static boolean hasIntegerNarrowing(List<DataField> oldFields, List<DataField> newFields) {
        Map<Integer, DataType> oldTypes = new HashMap<>();
        for (DataField field : oldFields) {
            oldTypes.put(field.id(), field.type());
        }
        for (DataField field : newFields) {
            DataType oldType = oldTypes.get(field.id());
            if (oldType != null && hasIntegerNarrowing(oldType, field.type())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasIntegerNarrowing(DataType oldType, DataType newType) {
        int oldWidth = integerWidth(oldType.getTypeRoot());
        int newWidth = integerWidth(newType.getTypeRoot());
        if (newWidth > 0) {
            // Floating-point and decimal sources also differ under narrowing; only integer
            // identity/widening casts have the same range and value semantics in both readers.
            return oldWidth == 0 || oldWidth > newWidth;
        }
        if (oldType instanceof RowType && newType instanceof RowType) {
            return hasIntegerNarrowing(((RowType) oldType).getFields(), ((RowType) newType).getFields());
        }
        if (oldType instanceof ArrayType && newType instanceof ArrayType) {
            return hasIntegerNarrowing(((ArrayType) oldType).getElementType(),
                    ((ArrayType) newType).getElementType());
        }
        if (oldType instanceof MapType && newType instanceof MapType) {
            MapType oldMap = (MapType) oldType;
            MapType newMap = (MapType) newType;
            return hasIntegerNarrowing(oldMap.getKeyType(), newMap.getKeyType())
                    || hasIntegerNarrowing(oldMap.getValueType(), newMap.getValueType());
        }
        return false;
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
