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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorPartitionField;
import org.apache.doris.connector.spi.ddl.ConnectorPartitionSpec;
import org.apache.doris.connector.spi.ddl.ConnectorSortField;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Builds the Iceberg create-table artifacts (Schema / PartitionSpec / SortOrder / default properties)
 * from a connector-SPI {@link org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest}.
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
 * <p><b>Nested nullability:</b> the neutral {@link ConnectorType} now carries per-element nullability +
 * per-STRUCT-field comments ({@link ConnectorType#isChildNullable(int)}/{@link ConnectorType#getChildComment}),
 * so a NOT NULL (or a comment) declared inside a complex type — ARRAY element, MAP value, STRUCT field — is
 * preserved on the iceberg side. When the neutral type does not carry them (legacy factories / the paimon
 * write path), every element defaults to OPTIONAL, preserving the prior behavior.</p>
 */
public final class IcebergSchemaBuilder {

    private static final List<String> ICEBERG_TABLE_LOCATION_CHILD_DIRS = java.util.Arrays.asList("data", "metadata");
    // The Flink and Spark nickname of write.format.default, which wins over it (as IcebergWriterHelper reads them).
    private static final String WRITE_FORMAT = "write-format";
    static final String NESTED_VARIANT_DDL_ERROR = "Iceberg VARIANT DDL currently supports only top-level columns";

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
            Type type = convertColumn(col.getType(), ids);
            if (col.isNullable()) {
                fields.add(Types.NestedField.optional(i, col.getName(), type, col.getComment()));
            } else {
                fields.add(Types.NestedField.required(i, col.getName(), type, col.getComment()));
            }
        }
        return new Schema(fields);
    }

    /**
     * Recursively converts a neutral type to an Iceberg type, allocating ids for nested fields.
     *
     * <p>Per-element nullability ({@link ConnectorType#isChildNullable(int)}) and per-STRUCT-field comments
     * ({@link ConnectorType#getChildComment(int)}) are honored when the neutral type carries them — closing
     * the former FU-nested-nullability gap so a NOT NULL declared inside a complex type survives. When unset
     * (legacy factories / connectors that do not thread them) every element defaults to OPTIONAL with no doc,
     * preserving the prior behavior.</p>
     */
    private static Type convert(ConnectorType type, IdAllocator ids) {
        String name = type.getTypeName().toUpperCase(Locale.ROOT);
        switch (name) {
            case "VARIANT":
            case "VARIANT_COMPUTE_V2":
                throw new DorisConnectorException(NESTED_VARIANT_DDL_ERROR);
            case "ARRAY": {
                // Element type/ids first, then the list's element id (post-order, matching legacy visitor).
                Type element = convert(type.getChildren().get(0), ids);
                int elementId = ids.next();
                return type.isChildNullable(0)
                        ? Types.ListType.ofOptional(elementId, element)
                        : Types.ListType.ofRequired(elementId, element);
            }
            case "MAP": {
                Type key = convert(type.getChildren().get(0), ids);
                Type value = convert(type.getChildren().get(1), ids);
                int keyId = ids.next();
                int valueId = ids.next();
                // Iceberg map keys are always required; child index 1 (the value) carries the nullability.
                return type.isChildNullable(1)
                        ? Types.MapType.ofOptional(keyId, valueId, key, value)
                        : Types.MapType.ofRequired(keyId, valueId, key, value);
            }
            case "STRUCT": {
                List<ConnectorType> childTypes = type.getChildren();
                List<String> fieldNames = type.getFieldNames();
                List<Types.NestedField> sub = new ArrayList<>(childTypes.size());
                for (int i = 0; i < childTypes.size(); i++) {
                    String fieldName = i < fieldNames.size() ? fieldNames.get(i) : "col" + i;
                    Type fieldType = convert(childTypes.get(i), ids);
                    String fieldDoc = type.getChildComment(i);
                    sub.add(type.isChildNullable(i)
                            ? Types.NestedField.optional(ids.next(), fieldName, fieldType, fieldDoc)
                            : Types.NestedField.required(ids.next(), fieldName, fieldType, fieldDoc));
                }
                return Types.StructType.of(sub);
            }
            default:
                return IcebergTypeMapping.toIcebergPrimitive(type);
        }
    }

    /** Converts the type of a top-level column, the only place a VARIANT may be declared. */
    private static Type convertColumn(ConnectorType type, IdAllocator ids) {
        return isVariant(type) ? Types.VariantType.get() : convert(type, ids);
    }

    /** Whether {@code type} is a VARIANT, as declared by DDL or read from an Iceberg table. */
    static boolean isVariant(ConnectorType type) {
        String name = type.getTypeName().toUpperCase(Locale.ROOT);
        return "VARIANT".equals(name) || "VARIANT_COMPUTE_V2".equals(name);
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
            // #65094: resolve the partition column back to the schema's canonical (case-preserving)
            // name; the schema now keeps the original column-name case, so a case-mismatched DDL
            // reference would otherwise fail Iceberg's case-sensitive PartitionSpec builder lookup.
            String column = resolveColumnName(schema, field.getColumnName());
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
            // #65094: resolve the sort column to the schema's canonical (case-preserving) name so a
            // case-mismatched DDL reference does not fail Iceberg's case-sensitive SortOrder lookup.
            String column = resolveColumnName(schema, field.getColumnName());
            if (field.isAscending()) {
                builder.asc(column, nullOrder);
            } else {
                builder.desc(column, nullOrder);
            }
        }
        return builder.build();
    }

    /**
     * Resolves an external column name to the schema's canonical (case-preserving) spelling, matching
     * case-insensitively. #65094: the built schema now preserves the original column-name case
     * ({@code col.getName()}), so a partition / sort column referenced in DDL with different case must be
     * mapped back to the canonical name — otherwise Iceberg's case-sensitive {@code PartitionSpec} /
     * {@code SortOrder} builder throws "Cannot find field". Mirrors {@code IcebergUtils.getIcebergColumnName}.
     */
    private static String resolveColumnName(Schema schema, String columnName) {
        Types.NestedField field = schema.caseInsensitiveFindField(columnName);
        return field == null ? columnName : field.name();
    }

    /**
     * Returns a mutable copy of the request properties with the Doris iceberg defaults applied (only when
     * absent): {@code format-version=2} and merge-on-read for delete/update/merge. Mirrors the
     * {@code putIfAbsent} block in legacy {@code performCreateTable} — the MOR modes are functionally
     * required (Doris rejects row-level DML on copy-on-write iceberg tables). Uses no catalog-level
     * defaults; prefer {@link #buildTableProperties(Map, Map)} when the catalog properties are available.
     */
    public static Map<String, String> buildTableProperties(Map<String, String> requestProperties) {
        return buildTableProperties(requestProperties, Collections.emptyMap());
    }

    /**
     * Overload that respects a catalog-level default/override iceberg format-version (upstream 25f291673f1,
     * #63825): the {@code format-version=2} default is applied ONLY when neither the table request nor the
     * catalog specifies one, so a catalog {@code table-default.format-version} /
     * {@code table-override.format-version} is no longer silently overridden to v2. Mirrors legacy
     * {@code IcebergMetadataOps.performCreateTable} + {@code IcebergUtils.hasIcebergCatalogFormatVersion}
     * (the connector cannot import fe-core IcebergUtils). The MOR modes stay unconditional (see the 1-arg
     * overload).
     */
    public static Map<String, String> buildTableProperties(Map<String, String> requestProperties,
            Map<String, String> catalogProperties) {
        Map<String, String> props = new HashMap<>(requestProperties);
        if (!props.containsKey(TableProperties.FORMAT_VERSION)
                && !hasIcebergCatalogFormatVersion(catalogProperties)) {
            props.put(TableProperties.FORMAT_VERSION, "2");
        }
        props.putIfAbsent(TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        props.putIfAbsent(TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        props.putIfAbsent(TableProperties.MERGE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        return props;
    }

    /**
     * Whether the catalog sets a table-level default/override iceberg format-version (the iceberg
     * {@code table-default.*} / {@code table-override.*} namespaces applied by the underlying catalog).
     * Mirrors fe-core {@code IcebergUtils.hasIcebergCatalogFormatVersion}.
     */
    private static boolean hasIcebergCatalogFormatVersion(Map<String, String> catalogProperties) {
        return catalogProperties.containsKey(CatalogProperties.TABLE_OVERRIDE_PREFIX + TableProperties.FORMAT_VERSION)
                || catalogProperties.containsKey(
                        CatalogProperties.TABLE_DEFAULT_PREFIX + TableProperties.FORMAT_VERSION);
    }

    /**
     * The effective iceberg format-version for a CREATE TABLE, applying the full precedence: catalog
     * {@code table-override.format-version} &gt; table request {@code format-version} &gt; catalog
     * {@code table-default.format-version} &gt; default {@code 2}. Mirrors legacy fe-core
     * {@code IcebergUtils.getEffectiveIcebergFormatVersion} — used by {@code IcebergConnectorMetadata.createTable}
     * to gate the v3 reserved row-lineage column-name rejection (moved off fe-core CreateTableInfo).
     */
    public static int getEffectiveFormatVersion(Map<String, String> requestProperties,
            Map<String, String> catalogProperties) {
        String formatVersion = catalogProperties.get(
                CatalogProperties.TABLE_OVERRIDE_PREFIX + TableProperties.FORMAT_VERSION);
        if (formatVersion == null) {
            formatVersion = requestProperties.get(TableProperties.FORMAT_VERSION);
            if (formatVersion == null) {
                formatVersion = catalogProperties.get(
                        CatalogProperties.TABLE_DEFAULT_PREFIX + TableProperties.FORMAT_VERSION);
            }
        }
        if (formatVersion == null) {
            return 2;
        }
        try {
            return Integer.parseInt(formatVersion);
        } catch (NumberFormatException ignored) {
            return 2;
        }
    }

    /**
     * The effective iceberg data file format for a CREATE TABLE: catalog {@code table-default} properties, then
     * the table request, then catalog {@code table-override} properties, each overriding the former; of the
     * resulting {@code write-format} nickname and standard {@code write.format.default}, the nickname wins, as
     * for an existing table ({@link IcebergWriterHelper#getFileFormat}). Without either, Parquet.
     */
    public static FileFormat getEffectiveFileFormat(Map<String, String> requestProperties,
            Map<String, String> catalogProperties) {
        Map<String, String> effective = new HashMap<>();
        copyFileFormat(effective, catalogProperties, CatalogProperties.TABLE_DEFAULT_PREFIX);
        copyFileFormat(effective, requestProperties, "");
        copyFileFormat(effective, catalogProperties, CatalogProperties.TABLE_OVERRIDE_PREFIX);
        String fileFormat = effective.containsKey(WRITE_FORMAT) ? effective.get(WRITE_FORMAT)
                : effective.getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name());
        return IcebergWriterHelper.toFileFormat(fileFormat);
    }

    private static void copyFileFormat(Map<String, String> effective, Map<String, String> properties,
            String prefix) {
        for (String property : new String[] {WRITE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT}) {
            String value = properties.get(prefix + property);
            if (value != null) {
                effective.put(property, value);
            }
        }
    }

    /**
     * Builds the iceberg {@link Type} for a SINGLE column added/modified on an EXISTING table, reusing the
     * same neutral-type conversion as {@link #buildSchema} (scalars via {@link IcebergTypeMapping}, plus
     * ARRAY/MAP/STRUCT recursively). Iceberg's {@code UpdateSchema.addColumn/updateColumn} assigns the field
     * ids itself, so the throwaway allocator values here are irrelevant — only the type shape matters.
     *
     * <p>Per-element nullability + per-STRUCT-field comments are honored when the neutral type carries them
     * (same as {@link #buildSchema}), so the full new complex type built here for a {@code MODIFY COLUMN}
     * faithfully drives the {@link IcebergComplexTypeDiff} field-by-field diff.</p>
     *
     * @throws DorisConnectorException if the column type cannot be represented in iceberg
     */
    public static Type buildColumnType(ConnectorType type) {
        return convertColumn(type, new IdAllocator(0));
    }

    /**
     * Like {@link #buildColumnType}, for a field nested in a STRUCT, which cannot be a VARIANT.
     *
     * @throws DorisConnectorException if the field type cannot be represented in iceberg
     */
    public static Type buildNestedFieldType(ConnectorType type) {
        return convert(type, new IdAllocator(0));
    }

    /**
     * Parses a column {@code DEFAULT} value string into an iceberg {@link Literal} of {@code type}, or
     * {@code null} when {@code value} is null (no DEFAULT clause). Byte-faithful port of the legacy fe-core
     * {@code IcebergUtils.parseIcebergLiteral} (self-contained — only iceberg + JDK types), so that an
     * {@code ADD COLUMN ... DEFAULT} keeps its initial-default after the flip.
     *
     * @throws IllegalArgumentException for an unparseable value or a type that cannot carry a default
     */
    public static Literal<?> parseDefaultLiteral(String value, Type type) {
        if (value == null) {
            return null;
        }
        switch (type.typeId()) {
            case BOOLEAN:
                try {
                    return Literal.of(Boolean.parseBoolean(value));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid Boolean string: " + value, e);
                }
            case INTEGER:
            case DATE:
                try {
                    return Literal.of(Integer.parseInt(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid Int string: " + value, e);
                }
            case TIMESTAMP_NANO:
                // ADD COLUMN carries the validated SQL datetime, not an epoch integer.
                if (!value.matches("[+-]?[0-9]+")) {
                    return Expressions.nanos((Long) IcebergPartitionUtils.parsePartitionValueFromString(
                            value, type, ZoneOffset.UTC));
                }
                return Expressions.nanos(Long.parseLong(value));
            case LONG:
            case TIME:
            case TIMESTAMP:
                try {
                    return Literal.of(Long.parseLong(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid Long string: " + value, e);
                }
            case FLOAT:
                try {
                    return Literal.of(Float.parseFloat(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid Float string: " + value, e);
                }
            case DOUBLE:
                try {
                    return Literal.of(Double.parseDouble(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid Double string: " + value, e);
                }
            case STRING:
                return Literal.of(value);
            case UUID:
                try {
                    return Literal.of(UUID.fromString(value));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid UUID string: " + value, e);
                }
            case FIXED:
            case BINARY:
            case GEOMETRY:
            case GEOGRAPHY:
                return Literal.of(ByteBuffer.wrap(value.getBytes()));
            case DECIMAL:
                try {
                    return Literal.of(new BigDecimal(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid Decimal string: " + value, e);
                }
            case VARIANT:
                throw new IllegalArgumentException("Iceberg VARIANT default values must be NULL");
            default:
                throw new IllegalArgumentException("Cannot parse unknown type: " + type);
        }
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
