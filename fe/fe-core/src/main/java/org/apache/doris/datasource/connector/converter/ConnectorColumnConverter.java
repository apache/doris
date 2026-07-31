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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Converts between the connector SPI type system ({@link ConnectorColumn}/{@link ConnectorType})
 * and the Doris internal type system ({@link Column}/{@link Type}).
 *
 * <p>This converter lives in fe-core because it depends on both the SPI types
 * (from fe-connector-spi) and the internal Doris catalog types (from fe-type/fe-core).</p>
 */
public final class ConnectorColumnConverter {

    private static final Logger LOG = LogManager.getLogger(ConnectorColumnConverter.class);

    private ConnectorColumnConverter() {
    }

    /**
     * Converts a list of {@link ConnectorColumn} to a list of Doris {@link Column}.
     */
    public static List<Column> convertColumns(List<ConnectorColumn> connectorColumns) {
        return connectorColumns.stream()
                .map(ConnectorColumnConverter::convertColumn)
                .collect(Collectors.toList());
    }

    /**
     * Converts a single {@link ConnectorColumn} to a Doris {@link Column}.
     */
    public static Column convertColumn(ConnectorColumn cc) {
        Type dorisType = convertType(cc.getType());
        Column column = new Column(cc.getName(), dorisType, cc.isKey(), null,
                cc.isNullable(), cc.getDefaultValue(),
                cc.getComment() != null ? cc.getComment() : "");
        column.setConnectorDefaultValueSql(cc.getDefaultValueSql());
        // Re-apply the WITH_TIMEZONE "Extra" marker the connector carried across the SPI boundary
        // (ConnectorColumn.withTimeZone()), matching legacy PaimonExternalTable/IcebergUtils which set it
        // via setWithTZExtraInfo() from the source TZ type. Independent of the mapped Doris type, so it is
        // shown even when the column was mapped to a plain DATETIME (timestamp_tz mapping off).
        if (cc.isWithTimeZone()) {
            column.setWithTZExtraInfo();
        }
        // Re-apply the hidden marker the connector carried across the SPI boundary
        // (ConnectorColumn.invisible()), so synthetic write columns a connector declares through the schema
        // SPI (iceberg __DORIS_ICEBERG_ROWID_COL__ / v3 row-lineage) stay hidden, matching legacy
        // Column.setIsVisible(false). A Doris Column defaults to visible, so only the false case is re-applied.
        if (!cc.isVisible()) {
            column.setIsVisible(false);
        }
        // Re-apply the reserved field id the connector carried across the SPI boundary
        // (ConnectorColumn.withUniqueId()), so synthetic write columns whose Doris identity must equal a
        // connector-reserved field id keep it (iceberg v3 row-lineage _row_id=2147483540 /
        // _last_updated_sequence_number=2147483539, matched by field id BE-side). A Doris Column defaults to
        // an unset (-1) uniqueId, so only a set (>= 0) id is re-applied.
        if (cc.getUniqueId() >= 0) {
            column.setUniqueId(cc.getUniqueId());
        }
        // Re-apply the connector-reserved passthrough marker the connector carried across the SPI boundary
        // (ConnectorColumn.reservedPassthrough()), so engine consumers (MERGE/UPDATE, sink binding) can
        // recognize a synthetic passthrough column (iceberg v3 row-lineage) generically via
        // Column.isReservedPassthrough() instead of string-matching the connector's column names. A Doris
        // Column defaults to false, so only the true case is re-applied.
        if (cc.isReservedPassthrough()) {
            column.setReservedPassthrough(true);
        }
        // Stamp the nested (STRUCT/ARRAY/MAP) child column tree with connector field metadata. ArrayType
        // cannot retain required element semantics itself, so the child Column is the canonical round-trip
        // carrier; field ids are also needed by the BE nested-field scan path.
        applyNestedFieldMetadata(column, cc.getType());
        return column;
    }

    /**
     * Recursively stamps {@code column}'s child tree (STRUCT fields / ARRAY element / MAP key+value) with the
     * per-child field ids carried on {@code type} ({@link ConnectorType#getChildFieldId(int)}). The Doris
     * child column order built by {@code Column.createChildrenColumn} matches the {@link ConnectorType}
     * children order (array element / map key,value / struct fields-in-order), so a parallel walk aligns them.
     * Nullability is always copied because ARRAY/MAP type objects cannot consistently retain it. A field id is
     * set only when the connector carries one ({@code >= 0}), leaving other ids at the default -1.
     */
    private static void applyNestedFieldMetadata(Column column, ConnectorType type) {
        List<Column> childColumns = column.getChildren();
        if (childColumns == null || childColumns.isEmpty()) {
            return;
        }
        List<ConnectorType> childTypes = type.getChildren();
        int n = Math.min(childColumns.size(), childTypes.size());
        for (int i = 0; i < n; i++) {
            Column childColumn = childColumns.get(i);
            childColumn.setIsAllowNull(type.isChildNullable(i));
            int childFieldId = type.getChildFieldId(i);
            if (childFieldId >= 0) {
                childColumn.setUniqueId(childFieldId);
            }
            applyNestedFieldMetadata(childColumn, childTypes.get(i));
        }
    }

    /**
     * Converts a list of Doris {@link Column} to a list of {@link ConnectorColumn}.
     */
    public static List<ConnectorColumn> toConnectorColumns(List<Column> columns) {
        return columns.stream()
                .map(ConnectorColumnConverter::toConnectorColumn)
                .collect(Collectors.toList());
    }

    /**
     * Converts a Doris {@link Column} to a {@link ConnectorColumn}.
     * This is the inverse of {@link #convertColumn(ConnectorColumn)}.
     *
     * <p>The {@code isKey}/{@code isAutoInc}/{@code isAggregated} flags are carried so a connector can
     * re-enforce its column-validation parity (e.g. iceberg rejects aggregated / auto-increment columns
     * in {@code ALTER TABLE ADD/MODIFY COLUMN}); without them those flags would default to {@code false}
     * and the connector could not tell an aggregated/auto-inc column apart from a plain one.</p>
     */
    public static ConnectorColumn toConnectorColumn(Column col) {
        // Preserve the complete field-id tree when a bound write schema crosses the connector boundary.
        // Iceberg field ids, rather than nested names, are the stable identity across schema evolution.
        ConnectorType connectorType = toConnectorType(col);
        ConnectorColumn result = new ConnectorColumn(
                col.getName(),
                connectorType,
                col.getComment(),
                col.isAllowNull(),
                col.getDefaultValue(),
                col.isKey(),
                col.isAutoInc(),
                col.isAggregated())
                // Thread the #65329 "specified" markers so a connector's nested MODIFY COLUMN can honor
                // omit-preserves-metadata (an omitted NULL/NOT NULL never widens the field; an omitted COMMENT
                // keeps the current doc). Inert for connectors / paths that don't read them.
                .withSpecified(col.isNullableSpecified(), col.isCommentSpecified())
                // The connector SQL is transient write metadata, distinct from Column.defaultValue. Losing it
                // on the return trip makes an unchanged remote default look like concurrent schema evolution.
                .withDefaultValueSql(col.getConnectorDefaultValueSql());
        if (!col.isVisible()) {
            result = result.invisible();
        }
        if (col.getUniqueId() >= 0) {
            result = result.withUniqueId(col.getUniqueId());
        }
        if (col.isReservedPassthrough()) {
            result = result.reservedPassthrough();
        }
        return result;
    }

    private static ConnectorType toConnectorType(Column column) {
        ConnectorType type = toConnectorType(column.getType());
        List<Column> childColumns = column.getChildren();
        if (childColumns == null || childColumns.size() != type.getChildren().size()) {
            return type;
        }
        List<ConnectorType> childTypes = new ArrayList<>(childColumns.size());
        List<Integer> childIds = new ArrayList<>(childColumns.size());
        List<Boolean> childNullable = new ArrayList<>(childColumns.size());
        List<String> childComments = new ArrayList<>(childColumns.size());
        List<Boolean> childCommentSpecified = new ArrayList<>(childColumns.size());
        for (int i = 0; i < childColumns.size(); i++) {
            Column child = childColumns.get(i);
            childTypes.add(toConnectorType(child));
            childIds.add(child.getUniqueId());
            childNullable.add(child.isAllowNull());
            childComments.add(type.getChildComment(i));
            childCommentSpecified.add(type.isChildCommentSpecified(i));
        }
        return new ConnectorType(type.getTypeName(), type.getPrecision(), type.getScale(), childTypes,
                type.getFieldNames(), childNullable, childComments, childIds, childCommentSpecified);
    }

    /**
     * Converts a Doris {@link Type} to a {@link ConnectorType}, handling
     * complex types (ARRAY, MAP, STRUCT) recursively.
     * This is the inverse of {@link #convertType(ConnectorType)}.
     */
    public static ConnectorType toConnectorType(Type dorisType) {
        if (dorisType instanceof ArrayType) {
            ArrayType arr = (ArrayType) dorisType;
            // Carry the element's nullability so a connector can preserve a NOT NULL ARRAY element
            // (e.g. iceberg CREATE TABLE / complex MODIFY COLUMN); legacy lost it (defaulted optional).
            return ConnectorType.arrayOf(toConnectorType(arr.getItemType()), arr.getContainsNull());
        } else if (dorisType instanceof MapType) {
            MapType map = (MapType) dorisType;
            // Map keys are always required; only the value nullability is carried.
            return ConnectorType.mapOf(
                    toConnectorType(map.getKeyType()),
                    toConnectorType(map.getValueType()),
                    map.getIsValueContainsNull());
        } else if (dorisType instanceof StructType) {
            StructType struct = (StructType) dorisType;
            List<String> names = new ArrayList<>();
            List<ConnectorType> types = new ArrayList<>();
            List<Boolean> nullables = new ArrayList<>();
            List<String> comments = new ArrayList<>();
            List<Boolean> commentSpecified = new ArrayList<>();
            // Carry each field's nullability + comment so a connector can preserve a NOT NULL / commented
            // STRUCT field and diff a complex MODIFY COLUMN field-by-field; legacy carried neither. Also carry
            // isCommentSpecified() so the diff can tell an omitted COMMENT (preserve the current doc) from
            // COMMENT '' (clear it) — the comment string is "" for both (#65329 omit-preserves-metadata).
            for (StructField f : struct.getFields()) {
                names.add(f.getName());
                types.add(toConnectorType(f.getType()));
                nullables.add(f.getContainsNull());
                comments.add(f.getComment());
                commentSpecified.add(f.isCommentSpecified());
            }
            return ConnectorType.structOf(names, types, nullables, comments, commentSpecified);
        } else if (dorisType instanceof ScalarType) {
            ScalarType scalar = (ScalarType) dorisType;
            PrimitiveType primitiveType = scalar.getPrimitiveType();
            // CHAR/VARCHAR store their length in `len`, not `precision`; encode it
            // into the ConnectorType precision field (matching convertScalarType and
            // the connector type convention) so CREATE TABLE requests keep the length.
            if (primitiveType == PrimitiveType.VARBINARY
                    && scalar.getLength() == ScalarType.MAX_VARBINARY_LENGTH) {
                // Doris materializes an unbounded connector VARBINARY with the maximum internal length.
                // Collapse it on the reverse path so an unchanged Iceberg BINARY schema compares equal.
                return ConnectorType.of(primitiveType.toString());
            }
            if (primitiveType == PrimitiveType.CHAR
                    || primitiveType == PrimitiveType.VARCHAR
                    || primitiveType == PrimitiveType.VARBINARY) {
                return ConnectorType.of(primitiveType.toString(),
                        scalar.getLength(), 0);
            }
            if (primitiveType.isDecimalV3Type() || primitiveType == PrimitiveType.DECIMALV2) {
                return ConnectorType.of(primitiveType.toString(),
                        scalar.getScalarPrecision(), scalar.getScalarScale());
            }
            if (primitiveType == PrimitiveType.DATETIMEV2 || primitiveType == PrimitiveType.TIMESTAMPTZ) {
                return ConnectorType.of(primitiveType.toString(), scalar.getScalarScale(), 0);
            }
            // Parameterless Doris scalars expose 0/0 through ScalarType, while connector schemas use -1/-1.
            // Canonicalizing them prevents an unchanged schema from being mistaken for concurrent drift.
            return ConnectorType.of(primitiveType.toString());
        } else {
            return ConnectorType.of(dorisType.toString(), -1, -1);
        }
    }

    /**
     * Converts a {@link ConnectorType} to a Doris {@link Type}, handling
     * complex types (ARRAY, MAP, STRUCT) recursively.
     */
    public static Type convertType(ConnectorType ct) {
        String typeName = ct.getTypeName().toUpperCase(Locale.ROOT);
        switch (typeName) {
            case "ARRAY":
                return convertArrayType(ct);
            case "MAP":
                return convertMapType(ct);
            case "STRUCT":
                return convertStructType(ct);
            default:
                return convertScalarType(typeName, ct.getPrecision(), ct.getScale());
        }
    }

    private static Type convertArrayType(ConnectorType ct) {
        List<ConnectorType> children = ct.getChildren();
        if (children.isEmpty()) {
            return ArrayType.create(Type.NULL, true);
        }
        return ArrayType.create(convertType(children.get(0)), true);
    }

    private static Type convertMapType(ConnectorType ct) {
        List<ConnectorType> children = ct.getChildren();
        if (children.size() < 2) {
            return new MapType(Type.NULL, Type.NULL);
        }
        return new MapType(convertType(children.get(0)), convertType(children.get(1)),
                ct.isChildNullable(0), ct.isChildNullable(1));
    }

    private static Type convertStructType(ConnectorType ct) {
        List<ConnectorType> children = ct.getChildren();
        List<String> fieldNames = ct.getFieldNames();
        ArrayList<StructField> fields = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            String fieldName = i < fieldNames.size() ? fieldNames.get(i) : "col" + i;
            // Thread each nested field's comment and nullability (mirror of toConnectorType, which
            // carries them onto the ConnectorType) so DESCRIBE / SHOW CREATE TABLE report a nested
            // STRUCT field's COMMENT and NOT NULL constraint. Connectors that leave these unset
            // (empty children lists) get null comment / nullable = true, identical to the prior
            // 2-arg StructField, so this is behavior-preserving for them.
            fields.add(new StructField(fieldName, convertType(children.get(i)),
                    ct.getChildComment(i), ct.isChildNullable(i)));
        }
        return new StructType(fields);
    }

    private static Type convertScalarType(String typeName, int precision, int scale) {
        switch (typeName) {
            case "CHAR":
                if (precision > 0) {
                    return ScalarType.createCharType(precision);
                }
                return ScalarType.CHAR;
            case "VARCHAR":
                if (precision > 0) {
                    return ScalarType.createVarcharType(precision);
                }
                return ScalarType.createVarcharType();
            case "DECIMAL":
            case "DECIMALV2":
                if (precision > 0) {
                    return ScalarType.createDecimalType(precision, Math.max(scale, 0));
                }
                return ScalarType.createDecimalType();
            case "DECIMALV3":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
            case "DECIMAL256":
                if (precision > 0) {
                    return ScalarType.createDecimalV3Type(precision, Math.max(scale, 0));
                }
                return ScalarType.createDecimalV3Type();
            case "DATETIMEV2":
                // Connectors encode datetime scale in the precision field of ConnectorType.
                if (precision >= 0) {
                    return ScalarType.createDatetimeV2Type(precision);
                }
                return ScalarType.DATETIMEV2;
            case "TIMESTAMP_NS":
                return ScalarType.createTimeStampNsType();
            case "TIMESTAMPTZ":
                if (precision >= 0) {
                    return ScalarType.createTimeStampTzType(precision);
                }
                return ScalarType.createTimeStampTzType(0);
            case "VARBINARY":
                if (precision > 0) {
                    return ScalarType.createVarbinaryType(precision);
                }
                return ScalarType.createVarbinaryType(ScalarType.MAX_VARBINARY_LENGTH);
            case "JSONB":
                return ScalarType.createType("JSON");
            case "UNSUPPORTED":
                return Type.UNSUPPORTED;
            default:
                try {
                    return ScalarType.createType(typeName);
                } catch (Exception e) {
                    LOG.warn("Unrecognized connector type '{}', marking as UNSUPPORTED", typeName);
                    return Type.UNSUPPORTED;
                }
        }
    }
}
