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

package org.apache.doris.cdcclient.source.deserialize;

import org.apache.doris.cdcclient.utils.SchemaChangeOperation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.doris.cdcclient.common.Constants;
import org.apache.doris.job.cdc.DataSourceConfigKeys;

import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

/**
 * Unit tests for {@link PostgresDebeziumJsonDeserializer}'s event-driven schema handling. Schema
 * changes are driven by pgoutput Relation messages surfaced as {@link PostgresSchemaRecord}; DML
 * records are emitted directly without per-record schema comparison.
 */
class PostgresSchemaChangeDeserializeTest {

    private static final TableId TABLE = new TableId(null, "public", "t1");
    private static final Map<String, String> CONTEXT =
            Map.of(Constants.DORIS_TARGET_DB, "doris_db");

    @Test
    void noBaseline_dmlPassesThrough() throws Exception {
        // A DML with no stored baseline must still be emitted (the missing-baseline warn path).
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(null);

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, createDmlRecord(afterSchema("id", "name")));

        assertEquals(DeserializeResult.Type.DML, result.getType());
        assertEquals(1, result.getRecords().size());
    }

    @Test
    void relationFirstAppearance_establishesBaselineNoDdl() throws Exception {
        // No stored baseline: the table's first Relation adopts the schema as baseline, no DDL.
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(null);

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(storedTable("id", "name")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertTrue(result.getDdls().isEmpty(), "baseline registration must not emit DDL");
        assertTrue(result.getRecords().isEmpty());
        assertTrue(result.getUpdatedSchemas().containsKey(TABLE));
    }

    @Test
    void relationSchemaChangeDisabledSkipsSchemaEvent() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id", "name"));
        Map<String, String> context = new HashMap<>(CONTEXT);
        context.put(DataSourceConfigKeys.SCHEMA_CHANGE_ENABLED, "false");

        DeserializeResult result =
                deserializer.deserialize(context, schemaRecord(storedTable("id", "name", "age")));

        assertEquals(DeserializeResult.Type.EMPTY, result.getType());
        assertTrue(result.getRecords().isEmpty());
    }

    @Test
    void relationUnchanged_isNoop() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id", "name"));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(storedTable("id", "name")));

        assertEquals(DeserializeResult.Type.EMPTY, result.getType());
    }

    @Test
    void relationModifyColumnTypeRequiresConfirmation() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer =
                newDeserializer(
                        tableWith(column("id", "int4", true, null).edit().nativeType(23).create()));
        Table fresh = tableWith(column("id", "int8", true, null).edit().nativeType(20).create());

        DeserializeResult result = deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertNotNull(result.getUnsupportedReason());
        assertTrue(result.getUnsupportedReason().contains("int4 to int8"));
        assertTrue(result.getDdls().isEmpty(), "type change must not emit DDL");
        assertEquals(fresh, result.getUpdatedSchemas().get(TABLE).getTable());
    }

    @Test
    void relationTypeChangeWithSameDorisMappingRequiresConfirmation() throws Exception {
        // Both text and uuid map to Doris STRING, but the source type change needs confirmation.
        PostgresDebeziumJsonDeserializer deserializer =
                newDeserializer(
                        tableWith(column("id", "text", true, null).edit().nativeType(25).create()));
        Table fresh = tableWith(column("id", "uuid", true, null).edit().nativeType(2950).create());

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        assertNotNull(result.getUnsupportedReason());
        assertTrue(result.getUnsupportedReason().contains("text to uuid"));
        assertTrue(result.getSchemaChanges().isEmpty());
    }

    @Test
    void relationSameNativeTypeParametersDoNotRequireConfirmation() throws Exception {
        Column before =
                column("id", "numeric", true, null).edit().nativeType(1700).length(45).scale(2).create();
        for (Column after : List.of(before.edit().length(46).create(), before.edit().scale(3).create())) {
            PostgresDebeziumJsonDeserializer deserializer = newDeserializer(tableWith(before));
            DeserializeResult result =
                    deserializer.deserialize(CONTEXT, schemaRecord(tableWith(after)));

            assertNull(result.getUnsupportedReason());
            assertTrue(result.getSchemaChanges().isEmpty());
            assertEquals(tableWith(after), result.getUpdatedSchemas().get(TABLE).getTable());
        }
    }

    @Test
    void relationSameNativeTypeWithDifferentNamesDoesNotRequireConfirmation() throws Exception {
        Column before = column("id", "serial", true, null).edit().nativeType(23).create();
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(tableWith(before));

        DeserializeResult result =
                deserializer.deserialize(
                        CONTEXT, schemaRecord(tableWith(before.edit().type("int4").create())));

        assertNull(result.getUnsupportedReason());
        assertTrue(result.getSchemaChanges().isEmpty());
    }

    @Test
    void relationNativeTypeOnlyChangeRequiresConfirmation() throws Exception {
        Column before = column("id", "custom_type", true, null).edit().nativeType(16384).create();
        Table fresh = tableWith(before.edit().nativeType(16385).create());
        // Debezium Table.equals() does not compare native type IDs.
        assertEquals(tableWith(before), fresh);
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(tableWith(before));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        assertNotNull(result.getUnsupportedReason());
        assertTrue(result.getUnsupportedReason().contains("OID 16384 -> 16385"));
        assertTrue(result.getSchemaChanges().isEmpty());
    }

    @Test
    void relationExcludedNativeTypeChangeDoesNotRequireConfirmation() throws Exception {
        Column before = column("id", "int4", true, null).edit().nativeType(23).create();
        Table fresh = tableWith(before.edit().nativeType(20).create());
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(tableWith(before));
        deserializer.excludeColumnsCache = Map.of(TABLE.table(), Set.of("id"));

        DeserializeResult result = deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertNull(result.getUnsupportedReason());
        assertTrue(result.getSchemaChanges().isEmpty());
        assertEquals(20, result.getUpdatedSchemas().get(TABLE).getTable().columnWithName("id").nativeType());
        assertEquals(23, deserializer.getTableSchemas().get(TABLE).getTable().columnWithName("id").nativeType());
    }

    @Test
    void relationPrimaryKeyChangeRequiresConfirmation() throws Exception {
        Table baseline = storedTable("id", "name").edit().setPrimaryKeyNames("id").create();
        Table fresh = baseline.edit().setPrimaryKeyNames("name").create();
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(baseline);

        DeserializeResult result = deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertNotNull(result.getUnsupportedReason());
        assertTrue(result.getUnsupportedReason().contains("Primary key changes"));
        assertTrue(result.getSchemaChanges().isEmpty());
        assertEquals(fresh, result.getUpdatedSchemas().get(TABLE).getTable());
        assertEquals(baseline, deserializer.getTableSchemas().get(TABLE).getTable());
    }

    @Test
    void ignoreReturnsUpdatedSchemasWithoutDdlOrConfirmation() throws Exception {
        Table baseline = storedTable("id", "name");
        Map<String, String> context = new HashMap<>(CONTEXT);
        context.put(DataSourceConfigKeys.SCHEMA_CHANGE_BEHAVIOR, "ignore");
        for (Table fresh : List.of(
                storedTable("id", "name", "age"), storedTable("id"), storedTable("id", "nick"))) {
            PostgresDebeziumJsonDeserializer deserializer = newDeserializer(baseline);

            DeserializeResult result = deserializer.deserialize(context, schemaRecord(fresh));

            assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
            assertNull(result.getUnsupportedReason());
            assertTrue(result.getSchemaChanges().isEmpty());
            assertEquals(fresh, result.getUpdatedSchemas().get(TABLE).getTable());
            assertEquals(baseline, deserializer.getTableSchemas().get(TABLE).getTable());
        }
    }

    @Test
    void relationAddColumn_emitsAddDdl() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id", "name"));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(storedTable("id", "name", "age")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertEquals(1, result.getDdls().size());
        assertEquals(
                SchemaChangeOperation.Type.ADD,
                result.getSchemaChanges().get(0).getType());
        assertEquals("age", result.getSchemaChanges().get(0).getColumnName());
        String ddl = result.getDdls().get(0).toUpperCase();
        assertTrue(ddl.contains("ADD COLUMN"), ddl);
        assertTrue(ddl.contains("AGE"), ddl);
        // The Relation event carries no DML record of its own.
        assertTrue(result.getRecords().isEmpty());
    }

    @Test
    void relationDropColumn_emitsDropDdl() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer =
                newDeserializer(storedTable("id", "name", "age"));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(storedTable("id", "name")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertEquals(1, result.getDdls().size());
        assertEquals(
                SchemaChangeOperation.Type.DROP,
                result.getSchemaChanges().get(0).getType());
        assertEquals("age", result.getSchemaChanges().get(0).getColumnName());
        String ddl = result.getDdls().get(0).toUpperCase();
        assertTrue(ddl.contains("DROP COLUMN"), ddl);
        assertTrue(ddl.contains("AGE"), ddl);
    }

    @Test
    void relationSimultaneousAddAndDropRequiresConfirmationWithoutApplyingBaseline() throws Exception {
        // stored [id,name]; Relation [id,nick] -> name dropped + nick added -> treated as RENAME.
        Table baseline = storedTable("id", "name");
        Table fresh = storedTable("id", "nick");
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(baseline);

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertNotNull(result.getUnsupportedReason());
        assertTrue(result.getUnsupportedReason().contains("rename"));
        assertTrue(result.getDdls().isEmpty(), "rename must not emit DDL");
        assertEquals(fresh, result.getUpdatedSchemas().get(TABLE).getTable());
        assertEquals(baseline, deserializer.getTableSchemas().get(TABLE).getTable());
    }

    @Test
    void relationRenameAcrossExcludedColumnsRequiresConfirmation() throws Exception {
        Table baseline = storedTable("id", "name");
        Table fresh = storedTable("id", "secret");
        for (String excluded : List.of("name", "secret")) {
            PostgresDebeziumJsonDeserializer deserializer = newDeserializer(baseline);
            deserializer.excludeColumnsCache = Map.of(TABLE.table(), Set.of(excluded));

            DeserializeResult result = deserializer.deserialize(CONTEXT, schemaRecord(fresh));

            assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
            assertNotNull(result.getUnsupportedReason());
            assertTrue(result.getUnsupportedReason().contains("rename"));
            assertTrue(result.getDdls().isEmpty(), "rename must not emit a partial ADD or DROP");
            assertEquals(fresh, result.getUpdatedSchemas().get(TABLE).getTable());
            assertEquals(baseline, deserializer.getTableSchemas().get(TABLE).getTable());
        }
    }

    @Test
    void relationRenameBetweenExcludedColumnsSkipsDdl() throws Exception {
        Table baseline = storedTable("id", "name");
        Table fresh = storedTable("id", "secret");
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(baseline);
        deserializer.excludeColumnsCache = Map.of(TABLE.table(), Set.of("name", "secret"));

        DeserializeResult result = deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertNull(result.getUnsupportedReason());
        assertTrue(result.getDdls().isEmpty());
        assertEquals(fresh, result.getUpdatedSchemas().get(TABLE).getTable());
        assertEquals(baseline, deserializer.getTableSchemas().get(TABLE).getTable());
    }

    @Test
    void relationAddColumn_withDefault_omitsDefaultAndNotNull() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id"));
        Table fresh =
                tableWith(
                        column("id", "int4", true, null),
                        column("note", "text", false, "'foo(bar)'::text"));

        DeserializeResult result = deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        String ddl = result.getDdls().get(0);
        assertTrue(ddl.contains("ADD COLUMN"), ddl);
        // CDC records carry the value evaluated by PostgreSQL. Do not propagate the source DEFAULT
        // expression or NOT NULL constraint because existing Doris rows are not backfilled.
        assertFalse(ddl.toUpperCase().contains("DEFAULT"), ddl);
        assertFalse(ddl.toUpperCase().contains("NOT NULL"), ddl);
    }

    @Test
    void relationAddColumn_expressionDefault_omitsDefaultAndNotNull() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id"));
        Table fresh =
                tableWith(
                        column("id", "int4", true, null),
                        column("d", "date", false, "current_date"));

        DeserializeResult result = deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        String ddl = result.getDdls().get(0).toUpperCase();
        assertTrue(ddl.contains("ADD COLUMN"), ddl);
        // PostgreSQL evaluates the expression for subsequent DML; Doris does not copy it.
        assertFalse(ddl.contains("DEFAULT"), ddl);
        assertFalse(ddl.contains("NOT NULL"), ddl);
    }

    @Test
    void relationAddColumn_castDefault_omitsDefaultAndNotNull() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id"));
        Table fresh =
                tableWith(
                        column("id", "int4", true, null),
                        column("note", "text", false, "'a::b'::text"));

        DeserializeResult result = deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        String ddl = result.getDdls().get(0).toUpperCase();
        assertFalse(ddl.contains("DEFAULT"), ddl);
        assertFalse(ddl.contains("NOT NULL"), ddl);
    }

    @Test
    void relationAddExcludedColumn_skipsAddDdl() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id", "name"));
        deserializer.excludeColumnsCache = Map.of(TABLE.table(), Set.of("age"));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(storedTable("id", "name", "age")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertNull(result.getUnsupportedReason());
        assertTrue(result.getDdls().isEmpty(), "excluded ADD column must not emit DDL");
        // baseline still advances so the excluded column is not re-detected on every Relation
        assertTrue(result.getUpdatedSchemas().containsKey(TABLE));
    }

    @Test
    void relationDropExcludedColumn_skipsDropDdl() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer =
                newDeserializer(storedTable("id", "name", "age"));
        deserializer.excludeColumnsCache = Map.of(TABLE.table(), Set.of("age"));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(storedTable("id", "name")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertNull(result.getUnsupportedReason());
        assertTrue(result.getDdls().isEmpty(), "excluded DROP column must not emit DDL");
    }

    // ─── helpers ───────────────────────────────────────────────────────────────

    private static Column column(String name, String type, boolean optional, String defaultExpr) {
        io.debezium.relational.ColumnEditor editor =
                Column.editor().name(name).type(type).jdbcType(Types.OTHER).optional(optional);
        if (defaultExpr != null) {
            editor.defaultValueExpression(defaultExpr);
        }
        return editor.create();
    }

    private static Table tableWith(Column... cols) {
        TableEditor editor = Table.editor().tableId(TABLE);
        for (Column col : cols) {
            editor.addColumns(col);
        }
        return editor.create();
    }

    private PostgresDebeziumJsonDeserializer newDeserializer(Table storedTable) {
        PostgresDebeziumJsonDeserializer deserializer = new PostgresDebeziumJsonDeserializer();
        deserializer.init(new HashMap<>());
        if (storedTable != null) {
            deserializer.setTableSchemas(Map.of(TABLE, change(storedTable)));
        }
        return deserializer;
    }

    private static SourceRecord schemaRecord(Table freshTable) {
        return new PostgresSchemaRecord(freshTable);
    }

    private static TableChanges.TableChange change(Table table) {
        return new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table);
    }

    /** All columns are int4 (-> Doris INT) for simplicity. */
    private static Table storedTable(String... columns) {
        TableEditor editor = Table.editor().tableId(TABLE);
        for (String col : columns) {
            editor.addColumns(
                    Column.editor()
                            .name(col)
                            .type("int4")
                            .jdbcType(Types.INTEGER)
                            .optional(true)
                            .create());
        }
        return editor.create();
    }

    private Schema afterSchema(String... fields) {
        SchemaBuilder builder = SchemaBuilder.struct().name("public.t1.Value");
        for (String field : fields) {
            builder.field(field, Schema.OPTIONAL_STRING_SCHEMA);
        }
        return builder.build();
    }

    private SourceRecord createDmlRecord(Schema afterSchema) {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .name("source")
                        .field("schema", Schema.STRING_SCHEMA)
                        .field("table", Schema.STRING_SCHEMA)
                        .build();
        Envelope envelope =
                Envelope.defineSchema()
                        .withName("public.t1.Envelope")
                        .withRecord(afterSchema)
                        .withSource(sourceSchema)
                        .build();

        Struct after = new Struct(afterSchema);
        for (org.apache.kafka.connect.data.Field field : afterSchema.fields()) {
            after.put(field.name(), "v");
        }
        Struct source = new Struct(sourceSchema).put("schema", "public").put("table", "t1");

        Struct value = new Struct(envelope.schema());
        value.put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code());
        value.put(Envelope.FieldName.AFTER, after);
        value.put(Envelope.FieldName.SOURCE, source);
        return new SourceRecord(null, null, "t1", envelope.schema(), value);
    }
}
