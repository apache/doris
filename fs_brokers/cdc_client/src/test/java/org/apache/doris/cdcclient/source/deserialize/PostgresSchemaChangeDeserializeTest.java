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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.doris.cdcclient.common.Constants;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

/**
 * Unit tests for {@link PostgresDebeziumJsonDeserializer}'s WAL-based ADD/DROP column detection: the
 * various branches it takes when the record's column set diverges from the stored schema.
 */
class PostgresSchemaChangeDeserializeTest {

    private static final TableId TABLE = new TableId(null, "public", "t1");
    private static final Map<String, String> CONTEXT =
            Map.of(Constants.DORIS_TARGET_DB, "doris_db");

    @Test
    void noStoredSchema_passesThroughAsDml() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = newDeserializer(null, tid -> null);

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, createRecord(afterSchema("id", "name")));

        assertEquals(DeserializeResult.Type.DML, result.getType());
        assertEquals(1, result.getRecords().size());
    }

    @Test
    void noColumnChange_passesThroughAsDml() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer =
                newDeserializer(storedTable("id", "name"), tid -> change(storedTable("id", "name")));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, createRecord(afterSchema("id", "name")));

        assertEquals(DeserializeResult.Type.DML, result.getType());
    }

    @Test
    void addColumn_emitsAddDdl() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer =
                newDeserializer(
                        storedTable("id", "name"), tid -> change(storedTable("id", "name", "age")));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, createRecord(afterSchema("id", "name", "age")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertEquals(1, result.getDdls().size());
        String ddl = result.getDdls().get(0).toUpperCase();
        assertTrue(ddl.contains("ADD COLUMN"), ddl);
        assertTrue(ddl.contains("AGE"), ddl);
        // the triggering DML record is still carried so it gets written after the DDL
        assertEquals(1, result.getRecords().size());
    }

    @Test
    void dropColumn_emitsDropDdl() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer =
                newDeserializer(
                        storedTable("id", "name", "age"), tid -> change(storedTable("id", "name")));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, createRecord(afterSchema("id", "name")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertEquals(1, result.getDdls().size());
        String ddl = result.getDdls().get(0).toUpperCase();
        assertTrue(ddl.contains("DROP COLUMN"), ddl);
        assertTrue(ddl.contains("AGE"), ddl);
    }

    @Test
    void simultaneousAddAndDrop_skipsDdlToAvoidRenameDataLoss() throws Exception {
        // stored has [id,name]; WAL record has [id,nick] -> name dropped + nick added.
        PostgresDebeziumJsonDeserializer deserializer =
                newDeserializer(
                        storedTable("id", "name"), tid -> change(storedTable("id", "nick")));

        DeserializeResult result =
                deserializer.deserialize(CONTEXT, createRecord(afterSchema("id", "nick")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertTrue(result.getDdls().isEmpty(), "rename must not emit DDL");
        assertEquals(1, result.getRecords().size());
    }

    @Test
    void columnChangeWithoutRefresher_throws() throws Exception {
        PostgresDebeziumJsonDeserializer deserializer = new PostgresDebeziumJsonDeserializer();
        deserializer.init(new HashMap<>());
        deserializer.setTableSchemas(Map.of(TABLE, change(storedTable("id", "name"))));
        // no pgSchemaRefresher set

        SourceRecord record = createRecord(afterSchema("id", "name", "age"));
        assertThrows(NullPointerException.class, () -> deserializer.deserialize(CONTEXT, record));
    }

    // ─── helpers ───────────────────────────────────────────────────────────────

    private PostgresDebeziumJsonDeserializer newDeserializer(
            Table storedTable, Function<TableId, TableChanges.TableChange> refresher) {
        PostgresDebeziumJsonDeserializer deserializer = new PostgresDebeziumJsonDeserializer();
        deserializer.init(new HashMap<>());
        if (storedTable != null) {
            deserializer.setTableSchemas(Map.of(TABLE, change(storedTable)));
        }
        deserializer.setPgSchemaRefresher(refresher);
        return deserializer;
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

    private SourceRecord createRecord(Schema afterSchema) {
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
