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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorPartitionField;
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec;
import org.apache.doris.connector.api.ddl.ConnectorSortField;

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Builds the Iceberg create-table artifacts (Schema / PartitionSpec / SortOrder / default properties)
 * from a connector-SPI {@link org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest}.
 *
 * <p>String-driven port of the legacy fe-core iceberg create path
 * ({@code DorisTypeToIcebergType} + {@code IcebergUtils.solveIcebergPartitionSpec} +
 * {@code IcebergMetadataOps.buildSortOrder} + the default-property block in {@code performCreateTable}),
 * reimplemented over the neutral {@link ConnectorType}/{@link ConnectorPartitionSpec}/{@link ConnectorSortField}
 * carriers because the connector cannot import fe-core. Mirrors {@code PaimonSchemaBuilder}'s role for paimon.</p>
 *
 * <p><b>Field ids:</b> top-level columns get id == their declaration index and nested fields draw sequential
 * ids from a counter starting at the column count — byte-faithful to legacy {@code DorisTypeToIcebergType}'s
 * scheme. (Iceberg reassigns fresh ids in {@code TableMetadata.newTableMetadata} on create, so only internal
 * consistency + name-resolvability of the partition/sort spec matters here.)</p>
 *
 * <p><b>Nested nullability:</b> the neutral {@link ConnectorType} carries no per-element nullability for
 * complex types (only the top-level {@link ConnectorColumn#isNullable()} survives the SPI), so ARRAY elements,
 * MAP values, and STRUCT fields default to OPTIONAL — matching the paimon connector's createTable precedent.
 * A NOT NULL declared inside a complex type is therefore not preserved (FU-nested-nullability); the FORMAT
 * of the top-level column is preserved.</p>
 */
public final class IcebergSchemaBuilder {

    private static final List<String> ICEBERG_TABLE_LOCATION_CHILD_DIRS = java.util.Arrays.asList("data", "metadata");

    private IcebergSchemaBuilder() {
    }

    /** The child directories under a managed iceberg table location to prune on drop (data + metadata). */
    static List<String> tableLocationChildDirs() {
        return ICEBERG_TABLE_LOCATION_CHILD_DIRS;
    }

    /**
     * Builds the Iceberg {@link Schema} from the neutral columns, allocating field ids exactly as legacy
     * {@code DorisTypeToIcebergType} (root field id == index; nested ids from a counter at the column count).
     *
     * @throws DorisConnectorException if a column type cannot be represented in Iceberg
     */
    public static Schema buildSchema(List<ConnectorColumn> columns) {
        IdAllocator ids = new IdAllocator(columns.size());
        List<Types.NestedField> fields = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            ConnectorColumn col = columns.get(i);
            Type type = convert(col.getType(), ids);
            if (col.isNullable()) {
                fields.add(Types.NestedField.optional(i, col.getName(), type, col.getComment()));
            } else {
                fields.add(Types.NestedField.required(i, col.getName(), type, col.getComment()));
            }
        }
        return new Schema(fields);
    }

    /** Recursively converts a neutral type to an Iceberg type, allocating ids for nested fields. */
    private static Type convert(ConnectorType type, IdAllocator ids) {
        String name = type.getTypeName().toUpperCase(Locale.ROOT);
        switch (name) {
            case "ARRAY": {
                // Element type/ids first, then the list's element id (post-order, matching legacy visitor).
                Type element = convert(type.getChildren().get(0), ids);
                return Types.ListType.ofOptional(ids.next(), element);
            }
            case "MAP": {
                Type key = convert(type.getChildren().get(0), ids);
                Type value = convert(type.getChildren().get(1), ids);
                int keyId = ids.next();
                int valueId = ids.next();
                return Types.MapType.ofOptional(keyId, valueId, key, value);
            }
            case "STRUCT": {
                List<ConnectorType> childTypes = type.getChildren();
                List<String> fieldNames = type.getFieldNames();
                List<Types.NestedField> sub = new ArrayList<>(childTypes.size());
                for (int i = 0; i < childTypes.size(); i++) {
                    String fieldName = i < fieldNames.size() ? fieldNames.get(i) : "col" + i;
                    Type fieldType = convert(childTypes.get(i), ids);
                    sub.add(Types.NestedField.optional(ids.next(), fieldName, fieldType));
                }
                return Types.StructType.of(sub);
            }
            default:
                return IcebergTypeMapping.toIcebergPrimitive(type);
        }
    }

    /**
     * Builds the Iceberg {@link PartitionSpec} against {@code schema} from the neutral partition spec.
     * String-driven port of {@code IcebergUtils.solveIcebergPartitionSpec}: each field's transform name +
     * integer args map to the {@link PartitionSpec.Builder} transform calls. An unset / empty spec yields
     * an unpartitioned table.
     *
     * @throws DorisConnectorException for an unsupported transform or missing transform argument
     */
    public static PartitionSpec buildPartitionSpec(ConnectorPartitionSpec spec, Schema schema) {
        if (spec == null || spec.getFields().isEmpty()) {
            return PartitionSpec.unpartitioned();
        }
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (ConnectorPartitionField field : spec.getFields()) {
            String transform = field.getTransform() == null
                    ? "identity" : field.getTransform().toLowerCase(Locale.ROOT);
            String column = field.getColumnName();
            switch (transform) {
                case "identity":
                    builder.identity(column);
                    break;
                case "bucket":
                    builder.bucket(column, intArg(transform, field.getTransformArgs()));
                    break;
                case "year":
                case "years":
                    builder.year(column);
                    break;
                case "month":
                case "months":
                    builder.month(column);
                    break;
                case "date":
                case "day":
                case "days":
                    builder.day(column);
                    break;
                case "date_hour":
                case "hour":
                case "hours":
                    builder.hour(column);
                    break;
                case "truncate":
                    builder.truncate(column, intArg(transform, field.getTransformArgs()));
                    break;
                default:
                    throw new DorisConnectorException("unsupported partition transform for iceberg: " + transform);
            }
        }
        return builder.build();
    }

    private static int intArg(String transform, List<Integer> args) {
        if (args == null || args.isEmpty()) {
            throw new DorisConnectorException(
                    "iceberg partition transform '" + transform + "' requires an integer argument");
        }
        return args.get(0);
    }

    /**
     * Builds the Iceberg {@link SortOrder} against {@code schema} from the neutral sort fields, or
     * {@code null} when there is no write order. Port of {@code IcebergMetadataOps.buildSortOrder}.
     */
    public static SortOrder buildSortOrder(List<ConnectorSortField> sortFields, Schema schema) {
        if (sortFields == null || sortFields.isEmpty()) {
            return null;
        }
        SortOrder.Builder builder = SortOrder.builderFor(schema);
        for (ConnectorSortField field : sortFields) {
            NullOrder nullOrder = field.isNullFirst() ? NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST;
            if (field.isAscending()) {
                builder.asc(field.getColumnName(), nullOrder);
            } else {
                builder.desc(field.getColumnName(), nullOrder);
            }
        }
        return builder.build();
    }

    /**
     * Returns a mutable copy of the request properties with the Doris iceberg defaults applied (only when
     * absent): {@code format-version=2} and merge-on-read for delete/update/merge. Mirrors the
     * {@code putIfAbsent} block in legacy {@code performCreateTable} — the MOR modes are functionally
     * required (Doris rejects row-level DML on copy-on-write iceberg tables).
     */
    public static Map<String, String> buildTableProperties(Map<String, String> requestProperties) {
        Map<String, String> props = new HashMap<>(requestProperties);
        props.putIfAbsent(TableProperties.FORMAT_VERSION, "2");
        props.putIfAbsent(TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        props.putIfAbsent(TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        props.putIfAbsent(TableProperties.MERGE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        return props;
    }

    /** Sequential Iceberg field-id allocator for nested fields (legacy {@code DorisTypeToIcebergType} scheme). */
    private static final class IdAllocator {
        private int next;

        IdAllocator(int start) {
            this.next = start;
        }

        int next() {
            return next++;
        }
    }
}
