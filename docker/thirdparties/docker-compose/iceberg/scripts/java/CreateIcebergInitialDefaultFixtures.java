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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

/** Adds Iceberg V3 initial-default fixtures to tables whose old-schema files already exist. */
public final class CreateIcebergInitialDefaultFixtures {
    private static final String CATALOG_NAME = "demo";
    private static final String MISSING_STRUCT_COLUMN = "missing_struct_col";
    private static final String MISSING_LIST_COLUMN = "missing_list_col";
    private static final String MISSING_MAP_COLUMN = "missing_map_col";

    private CreateIcebergInitialDefaultFixtures() {
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "Usage: CreateIcebergInitialDefaultFixtures <namespace> <parquet-table> <orc-table>");
        }

        Catalog catalog = loadCatalog();
        evolveTable(catalog, TableIdentifier.of(args[0], args[1]));
        evolveTable(catalog, TableIdentifier.of(args[0], args[2]));
    }

    private static Catalog loadCatalog() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "rest");
        properties.put("uri", "http://rest:8181");
        properties.put("warehouse", "s3://warehouse/wh/");
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.endpoint", "http://minio:9000");
        properties.put("s3.path-style-access", "true");
        properties.put("s3.region", "us-east-1");
        return CatalogUtil.buildIcebergCatalog(CATALOG_NAME, properties, null);
    }

    private static void evolveTable(Catalog catalog, TableIdentifier identifier) {
        Table table = catalog.loadTable(identifier);
        List<AddedDefault> addedDefaults = new ArrayList<>();
        UpdateSchema addColumns = table.updateSchema();

        addPrimitiveDefaults(addColumns, null, "default", addedDefaults);
        addPrimitiveDefaults(addColumns, "struct_col", "struct_default", addedDefaults);
        // Keep the list/map children optional so post-evolution files can distinguish an explicit
        // physical NULL from an absent child that must use its initial default.
        addDefault(
                addColumns,
                "list_col",
                new DefaultSpec(
                        "list_default_int",
                        Types.IntegerType.get(),
                        Literal.of(101),
                        Literal.of(201),
                        false),
                addedDefaults);
        addDefault(
                addColumns,
                "map_col",
                new DefaultSpec(
                        "map_default_int",
                        Types.IntegerType.get(),
                        Literal.of(103),
                        Literal.of(203),
                        false),
                addedDefaults);
        addMissingComplexColumns(addColumns, addedDefaults);
        addColumns.commit();

        table = catalog.loadTable(identifier);
        UpdateSchema updateWriteDefaults = table.updateSchema();
        for (AddedDefault addedDefault : addedDefaults) {
            addedDefault.canonicalName = canonicalName(table.schema(), addedDefault);
            updateWriteDefaults.updateColumnDefault(
                    addedDefault.canonicalName, addedDefault.spec.writeDefault);
        }
        updateWriteDefaults.commit();

        table = catalog.loadTable(identifier);
        verifyDefaults(table.schema(), addedDefaults);
        verifyMissingComplexParentDefaults(table.schema());
        System.out.println("Created initial-default fixture: " + identifier);
    }

    private static void addMissingComplexColumns(
            UpdateSchema update, List<AddedDefault> addedDefaults) {
        // Iceberg 1.10.1 Types.NestedField.castDefault rejects every non-null default on a nested
        // parent. The strongest fixture its public API can author is a newly added optional parent
        // with defaults carried by its children. Old files do not contain the parent at all, so
        // readers must preserve a NULL parent instead of reviving it from the child default.
        DefaultSpec structChild =
                new DefaultSpec(
                        "missing_struct_default_int",
                        Types.IntegerType.get(),
                        Literal.of(107),
                        Literal.of(207),
                        false);
        update.addColumn(
                MISSING_STRUCT_COLUMN,
                Types.StructType.of(defaultField(1, structChild)),
                "Whole missing struct initial-default fallback fixture");
        addedDefaults.add(new AddedDefault(MISSING_STRUCT_COLUMN, structChild));

        DefaultSpec listChild =
                new DefaultSpec(
                        "missing_list_default_int",
                        Types.IntegerType.get(),
                        Literal.of(109),
                        Literal.of(209),
                        false);
        update.addColumn(
                MISSING_LIST_COLUMN,
                Types.ListType.ofOptional(
                        1, Types.StructType.of(defaultField(2, listChild))),
                "Whole missing list initial-default fallback fixture");
        addedDefaults.add(new AddedDefault(MISSING_LIST_COLUMN, listChild));

        DefaultSpec mapChild =
                new DefaultSpec(
                        "missing_map_default_int",
                        Types.IntegerType.get(),
                        Literal.of(111),
                        Literal.of(211),
                        false);
        update.addColumn(
                MISSING_MAP_COLUMN,
                Types.MapType.ofOptional(
                        1,
                        2,
                        Types.StringType.get(),
                        Types.StructType.of(defaultField(3, mapChild))),
                "Whole missing map initial-default fallback fixture");
        addedDefaults.add(new AddedDefault(MISSING_MAP_COLUMN, mapChild));
    }

    private static Types.NestedField defaultField(int fieldId, DefaultSpec defaultSpec) {
        return Types.NestedField.builder()
                .withId(fieldId)
                .withName(defaultSpec.name)
                .isOptional(!defaultSpec.required)
                .ofType(defaultSpec.type)
                .withDoc("Iceberg V3 initial-default regression fixture")
                .withInitialDefault(defaultSpec.initialDefault)
                .withWriteDefault(defaultSpec.writeDefault)
                .build();
    }

    private static void addPrimitiveDefaults(
            UpdateSchema update,
            String parent,
            String prefix,
            List<AddedDefault> addedDefaults) {
        for (DefaultSpec primitiveDefault : primitiveDefaults()) {
            DefaultSpec namedDefault = primitiveDefault.withName(prefix + "_" + primitiveDefault.name);
            addDefault(update, parent, namedDefault, addedDefaults);
        }
    }

    private static void addDefault(
            UpdateSchema update,
            String parent,
            DefaultSpec defaultSpec,
            List<AddedDefault> addedDefaults) {
        String doc = "Iceberg V3 initial-default regression fixture";
        if (parent == null) {
            if (defaultSpec.required) {
                update.addRequiredColumn(
                        defaultSpec.name,
                        defaultSpec.type,
                        doc,
                        defaultSpec.initialDefault);
            } else {
                update.addColumn(
                        defaultSpec.name,
                        defaultSpec.type,
                        doc,
                        defaultSpec.initialDefault);
            }
        } else if (defaultSpec.required) {
            update.addRequiredColumn(
                    parent,
                    defaultSpec.name,
                    defaultSpec.type,
                    doc,
                    defaultSpec.initialDefault);
        } else {
            update.addColumn(
                    parent,
                    defaultSpec.name,
                    defaultSpec.type,
                    doc,
                    defaultSpec.initialDefault);
        }
        addedDefaults.add(new AddedDefault(parent, defaultSpec));
    }

    private static List<DefaultSpec> primitiveDefaults() {
        return Arrays.asList(
                new DefaultSpec(
                        "boolean", Types.BooleanType.get(), Literal.of(true), Literal.of(false), false),
                new DefaultSpec(
                        "int", Types.IntegerType.get(), Literal.of(34), Literal.of(35), false),
                new DefaultSpec(
                        "long",
                        Types.LongType.get(),
                        Literal.of(4_900_000_000L),
                        Literal.of(4_900_000_001L),
                        false),
                new DefaultSpec(
                        "float", Types.FloatType.get(), Literal.of(12.25F), Literal.of(13.5F), false),
                new DefaultSpec(
                        "double", Types.DoubleType.get(), Literal.of(-123.5D), Literal.of(456.75D), false),
                new DefaultSpec(
                        "decimal",
                        Types.DecimalType.of(20, 4),
                        Literal.of(new BigDecimal("12345.6789")),
                        Literal.of(new BigDecimal("98765.4321")),
                        false),
                new DefaultSpec(
                        "date",
                        Types.DateType.get(),
                        Literal.of(DateTimeUtil.isoDateToDays("2024-12-17")),
                        Literal.of(DateTimeUtil.isoDateToDays("2025-01-18")),
                        false),
                new DefaultSpec(
                        "timestamp",
                        Types.TimestampType.withoutZone(),
                        Literal.of(DateTimeUtil.isoTimestampToMicros("2024-12-17T23:59:59.123456")),
                        Literal.of(DateTimeUtil.isoTimestampToMicros("2025-01-18T01:02:03.654321")),
                        false),
                new DefaultSpec(
                        "timestamptz",
                        Types.TimestampType.withZone(),
                        Literal.of(
                                DateTimeUtil.isoTimestamptzToMicros(
                                        "2024-12-17T23:59:59.123456+00:00")),
                        Literal.of(
                                DateTimeUtil.isoTimestamptzToMicros(
                                        "2025-01-18T01:02:03.654321+00:00")),
                        false),
                new DefaultSpec(
                        "string",
                        Types.StringType.get(),
                        Literal.of("initial-default"),
                        Literal.of("write-default"),
                        true),
                new DefaultSpec(
                        "uuid",
                        Types.UUIDType.get(),
                        Literal.of(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")),
                        Literal.of(UUID.fromString("123e4567-e89b-12d3-a456-426614174001")),
                        false),
                new DefaultSpec(
                        "fixed",
                        Types.FixedType.ofLength(4),
                        Literal.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d})),
                        Literal.of(ByteBuffer.wrap(new byte[] {0x1a, 0x1b, 0x1c, 0x1d})),
                        false),
                new DefaultSpec(
                        "binary",
                        Types.BinaryType.get(),
                        Literal.of(ByteBuffer.wrap(new byte[] {0x2a, 0x2b, 0x2c})),
                        Literal.of(ByteBuffer.wrap(new byte[] {0x3a, 0x3b, 0x3c})),
                        false));
    }

    private static String canonicalName(Schema schema, AddedDefault addedDefault) {
        Types.NestedField field;
        if (addedDefault.parent == null) {
            field = schema.findField(addedDefault.spec.name);
        } else {
            Types.NestedField parentField = schema.findField(addedDefault.parent);
            if (parentField == null) {
                throw new IllegalStateException("Missing parent field: " + addedDefault.parent);
            }

            Type childContainer = parentField.type();
            if (childContainer.isListType()) {
                childContainer = childContainer.asListType().elementType();
            } else if (childContainer.isMapType()) {
                childContainer = childContainer.asMapType().valueType();
            }
            if (!childContainer.isStructType()) {
                throw new IllegalStateException(
                        "Parent is not a struct, list-of-struct, or map-to-struct: "
                                + addedDefault.parent);
            }

            field = null;
            for (Types.NestedField child : childContainer.asStructType().fields()) {
                if (child.name().equals(addedDefault.spec.name)) {
                    field = child;
                    break;
                }
            }
        }

        if (field == null) {
            throw new IllegalStateException("Missing added field: " + addedDefault.spec.name);
        }
        return schema.findColumnName(field.fieldId());
    }

    private static void verifyDefaults(Schema schema, List<AddedDefault> addedDefaults) {
        for (AddedDefault addedDefault : addedDefaults) {
            Types.NestedField field = schema.findField(addedDefault.canonicalName);
            if (field == null) {
                throw new IllegalStateException("Missing reloaded field: " + addedDefault.canonicalName);
            }
            if (field.isRequired() != addedDefault.spec.required) {
                throw new IllegalStateException(
                        "Unexpected requiredness for " + addedDefault.canonicalName);
            }

            Object expectedInitial = typedValue(addedDefault.spec.initialDefault, field.type());
            Object expectedWrite = typedValue(addedDefault.spec.writeDefault, field.type());
            if (!Objects.equals(expectedInitial, field.initialDefault())) {
                throw new IllegalStateException(
                        "Unexpected initial-default for "
                                + addedDefault.canonicalName
                                + ": "
                                + field.initialDefault());
            }
            if (!Objects.equals(expectedWrite, field.writeDefault())) {
                throw new IllegalStateException(
                        "Unexpected write-default for "
                                + addedDefault.canonicalName
                                + ": "
                                + field.writeDefault());
            }
            if (Objects.equals(field.initialDefault(), field.writeDefault())) {
                throw new IllegalStateException(
                        "initial-default and write-default must differ for "
                                + addedDefault.canonicalName);
            }
        }
    }

    private static void verifyMissingComplexParentDefaults(Schema schema) {
        for (String columnName :
                Arrays.asList(
                        MISSING_STRUCT_COLUMN, MISSING_LIST_COLUMN, MISSING_MAP_COLUMN)) {
            Types.NestedField field = schema.findField(columnName);
            if (field == null) {
                throw new IllegalStateException("Missing complex parent field: " + columnName);
            }
            if (field.initialDefault() != null || field.writeDefault() != null) {
                throw new IllegalStateException(
                        "Iceberg 1.10.1 nested parent default must remain null: " + columnName);
            }
        }
    }

    private static Object typedValue(Literal<?> literal, Type type) {
        Literal<?> converted = literal.to(type);
        if (converted == null) {
            throw new IllegalStateException("Cannot convert default " + literal + " to " + type);
        }
        return converted.value();
    }

    private static final class DefaultSpec {
        private final String name;
        private final Type type;
        private final Literal<?> initialDefault;
        private final Literal<?> writeDefault;
        private final boolean required;

        private DefaultSpec(
                String name,
                Type type,
                Literal<?> initialDefault,
                Literal<?> writeDefault,
                boolean required) {
            this.name = name;
            this.type = type;
            this.initialDefault = initialDefault;
            this.writeDefault = writeDefault;
            this.required = required;
        }

        private DefaultSpec withName(String replacementName) {
            return new DefaultSpec(
                    replacementName, type, initialDefault, writeDefault, required);
        }
    }

    private static final class AddedDefault {
        private final String parent;
        private final DefaultSpec spec;
        private String canonicalName;

        private AddedDefault(String parent, DefaultSpec spec) {
            this.parent = parent;
            this.spec = spec;
        }
    }
}
