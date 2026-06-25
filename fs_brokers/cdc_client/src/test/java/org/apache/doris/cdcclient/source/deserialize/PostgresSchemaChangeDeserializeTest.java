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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.doris.cdcclient.common.Constants;

import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

/**
 * Unit tests for {@link PostgresDebeziumJsonDeserializer}'s event-driven ADD/DROP column detection.
 * Schema changes are driven by pgoutput Relation messages surfaced as {@link PostgresSchemaRecord};
 * DML records are emitted directly without per-record schema comparison.
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
    void relationUnchanged_isNoop() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id", "name"));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(storedTable("id", "name")));

        assertEquals(DeserializeResult.Type.EMPTY, result.getType());
    }

    @Test
    void relationAddColumn_emitsAddDdl() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id", "name"));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(storedTable("id", "name", "age")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertEquals(1, result.getDdls().size());
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
        String ddl = result.getDdls().get(0).toUpperCase();
        assertTrue(ddl.contains("DROP COLUMN"), ddl);
        assertTrue(ddl.contains("AGE"), ddl);
    }

    @Test
    void relationSimultaneousAddAndDrop_skipsDdlToAvoidRenameDataLoss() throws Exception {
        // stored [id,name]; Relation [id,nick] -> name dropped + nick added -> treated as RENAME.
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id", "name"));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(storedTable("id", "nick")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertTrue(result.getDdls().isEmpty(), "rename must not emit DDL");
    }

    @Test
    void relationAddColumn_stringLiteralDefaultWithParens_keepsLiteral() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id"));
        Table fresh =
                tableWith(
                        column("id", "int4", true, null),
                        column("note", "text", false, "'foo(bar)'::text"));

        DeserializeResult result = deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        String ddl = result.getDdls().get(0);
        assertTrue(ddl.contains("ADD COLUMN"), ddl);
        // parenthesised string literal kept verbatim, not mistaken for a function expression
        assertTrue(ddl.contains("DEFAULT 'foo(bar)'"), ddl);
        // usable default present -> NOT NULL preserved
        assertTrue(ddl.toUpperCase().contains("NOT NULL"), ddl);
    }

    @Test
    void relationAddColumn_unrecognizedKeywordDefault_omitsDefault() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id"));
        Table fresh =
                tableWith(
                        column("id", "int4", true, null),
                        column("d", "date", false, "current_date"));

        DeserializeResult result = deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        String ddl = result.getDdls().get(0).toUpperCase();
        assertTrue(ddl.contains("ADD COLUMN"), ddl);
        // current_date is not statically mapped -> no DEFAULT (never a wrong 'current_date' literal)
        assertFalse(ddl.contains("DEFAULT"), ddl);
        // NOT NULL without a usable default -> downgraded to nullable
        assertFalse(ddl.contains("NOT NULL"), ddl);
    }

    @Test
    void relationAddColumn_castInsideStringLiteralDefault_keepsLiteral() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id"));
        Table fresh =
                tableWith(
                        column("id", "int4", true, null),
                        column("note", "text", false, "'a::b'::text"));

        DeserializeResult result = deserializer.deserialize(CONTEXT, schemaRecord(fresh));

        String ddl = result.getDdls().get(0);
        // the :: inside the literal is part of the value, not a cast — literal kept intact
        assertTrue(ddl.contains("DEFAULT 'a::b'"), ddl);
    }

    @Test
    void relationAddExcludedColumn_skipsAddDdl() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(storedTable("id", "name"));
        deserializer.excludeColumnsCache = Map.of(TABLE.table(), Set.of("age"));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, schemaRecord(storedTable("id", "name", "age")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
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
