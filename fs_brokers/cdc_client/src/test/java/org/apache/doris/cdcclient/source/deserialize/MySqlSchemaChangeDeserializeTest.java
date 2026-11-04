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

import static org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl.HISTORY_RECORD_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.doris.cdcclient.common.Constants;
import org.apache.doris.job.cdc.DataSourceConfigKeys;

import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;

class MySqlSchemaChangeDeserializeTest {
    private static final TableId TABLE = new TableId("db1", null, "t1");
    private static final Map<String, String> CONTEXT = Map.of(Constants.DORIS_TARGET_DB, "doris_db");
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    @Test
    void parserFailureRequiresConfirmationWithoutApplyingBaseline() throws Exception {
        Table baseline = table("id");
        MySqlDebeziumJsonDeserializer deserializer = newDeserializer(baseline);

        DeserializeResult result =
                deserializer.deserialize(
                        Collections.emptyMap(),
                        schemaChangeRecord(null, table("id", "city")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertNotNull(result.getUnsupportedReason());
        assertTrue(result.getUnsupportedReason().contains("Cannot parse schema change"));
        assertTrue(result.getSchemaChanges().isEmpty());
        assertEquals(1, result.getUpdatedSchemas().size());
        assertEquals(table("id", "city"), result.getUpdatedSchemas().get(TABLE).getTable());
        assertEquals(baseline, deserializer.getTableSchemas().get(TABLE).getTable());
    }

    @Test
    void schemaChangeAddExcludedColumnSkipsDdlAndAdvancesBaseline() throws Exception {
        MySqlDebeziumJsonDeserializer deserializer = newDeserializer(table("id", "name"));
        deserializer.excludeColumnsCache = Map.of(TABLE.table(), Set.of("secret"));
        Table fresh = table("id", "name", "secret");

        DeserializeResult result =
                deserializer.deserialize(
                        CONTEXT,
                        schemaChangeRecord(
                                "ALTER TABLE t1 ADD COLUMN secret INT",
                                fresh));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertNull(result.getUnsupportedReason());
        assertTrue(result.getSchemaChanges().isEmpty());
        assertEquals(fresh, result.getUpdatedSchemas().get(TABLE).getTable());
    }

    @Test
    void schemaChangeDropExcludedColumnSkipsDdlAndAdvancesBaseline() throws Exception {
        MySqlDebeziumJsonDeserializer deserializer = newDeserializer(table("id", "name", "secret"));
        deserializer.excludeColumnsCache = Map.of(TABLE.table(), Set.of("secret"));
        Table fresh = table("id", "name");

        DeserializeResult result =
                deserializer.deserialize(
                        CONTEXT,
                        schemaChangeRecord(
                                "ALTER TABLE t1 DROP COLUMN secret",
                                fresh));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertNull(result.getUnsupportedReason());
        assertTrue(result.getSchemaChanges().isEmpty());
        assertEquals(fresh, result.getUpdatedSchemas().get(TABLE).getTable());
    }

    @Test
    void dropTableDoesNotEmitDdlOrReplaceBaselineWithEmptyTable() throws Exception {
        Table baseline = table("id", "name");
        MySqlDebeziumJsonDeserializer deserializer = newDeserializer(baseline);

        DeserializeResult result =
                deserializer.deserialize(
                        CONTEXT,
                        schemaChangeRecord("DROP TABLE t1", new TableChanges().drop(baseline)));

        assertEquals(DeserializeResult.Type.EMPTY, result.getType());
        assertNull(result.getSchemaChanges());
        assertNull(result.getUpdatedSchemas());
        assertEquals(baseline, deserializer.getTableSchemas().get(TABLE).getTable());
    }

    @Test
    void recreatedTableReturnsCandidateSchemaWithoutDdlOrApplyingBaseline() throws Exception {
        Table baseline = table("id", "old_column");
        Table fresh = table("id", "name");
        MySqlDebeziumJsonDeserializer deserializer = newDeserializer(baseline);

        DeserializeResult result =
                deserializer.deserialize(
                        CONTEXT,
                        schemaChangeRecord(
                                "CREATE TABLE t1 (id INT, name INT)", new TableChanges().create(fresh)));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertNull(result.getUnsupportedReason());
        assertTrue(result.getSchemaChanges().isEmpty());
        assertEquals(
                TableChanges.TableChangeType.CREATE,
                result.getUpdatedSchemas().get(TABLE).getType());
        assertEquals(fresh, result.getUpdatedSchemas().get(TABLE).getTable());
        assertEquals(baseline, deserializer.getTableSchemas().get(TABLE).getTable());
    }

    @Test
    void mixedAddColumnAndIndexEmitsOnlyColumnDdl() throws Exception {
        MySqlDebeziumJsonDeserializer deserializer = newDeserializer(table("id"));
        Table fresh = table("id", "age");

        DeserializeResult result =
                deserializer.deserialize(
                        CONTEXT,
                        schemaChangeRecord(
                                "ALTER TABLE t1 ADD COLUMN age INT, ADD INDEX idx_age (age)", fresh));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertNull(result.getUnsupportedReason());
        assertEquals(
                Collections.singletonList("ALTER TABLE `doris_db`.`t1` ADD COLUMN `age` INT"),
                result.getDdls());
        assertEquals(fresh, result.getUpdatedSchemas().get(TABLE).getTable());
    }

    @Test
    void commentOnlyModifyRequiresConfirmationUnlessIgnored() throws Exception {
        Column city = Column.editor().name("city").type("VARCHAR").jdbcType(Types.VARCHAR)
                .length(30).optional(true).comment("old comment").create();
        Table baseline = table("id").edit().addColumn(city).create();
        Table fresh = baseline.edit()
                .addColumn(baseline.columnWithName("city").edit().comment("new comment").create())
                .create();
        for (String behavior : new String[] {"evolve", "ignore"}) {
            MySqlDebeziumJsonDeserializer deserializer = newDeserializer(baseline);
            Map<String, String> context = new HashMap<>(CONTEXT);
            context.put(DataSourceConfigKeys.SCHEMA_CHANGE_BEHAVIOR, behavior);

            DeserializeResult result =
                    deserializer.deserialize(
                            context,
                            schemaChangeRecord(
                                    "ALTER TABLE t1 MODIFY COLUMN city VARCHAR(30) COMMENT 'new comment'",
                                    fresh));

            assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
            if ("evolve".equals(behavior)) {
                assertNotNull(result.getUnsupportedReason());
                assertTrue(result.getUnsupportedReason().contains("MODIFY COLUMN"));
            } else {
                assertNull(result.getUnsupportedReason());
            }
            assertTrue(result.getSchemaChanges().isEmpty());
            assertEquals(fresh, result.getUpdatedSchemas().get(TABLE).getTable());
            assertEquals(
                    "new comment",
                    result.getUpdatedSchemas().get(TABLE).getTable().columnWithName("city").comment());
            assertEquals(
                    "old comment",
                    deserializer.getTableSchemas().get(TABLE).getTable().columnWithName("city").comment());
        }
    }

    @Test
    void schemaChangeMixedAddAndSameNameChangeRequiresConfirmation() throws Exception {
        Table baseline = table("id", "age");
        Table fresh = table("id", "age", "city");
        for (String alter : new String[] {"MODIFY COLUMN age INT", "CHANGE COLUMN age age INT"}) {
            MySqlDebeziumJsonDeserializer deserializer = newDeserializer(baseline);
            DeserializeResult result =
                    deserializer.deserialize(
                            CONTEXT,
                            schemaChangeRecord("ALTER TABLE t1 ADD COLUMN city INT, " + alter, fresh));

            assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
            assertNotNull(result.getUnsupportedReason());
            assertTrue(result.getSchemaChanges().isEmpty(), "mixed DDL must not be partially applied");
            assertEquals(fresh, result.getUpdatedSchemas().get(TABLE).getTable());
            assertEquals(baseline, deserializer.getTableSchemas().get(TABLE).getTable());
        }
    }

    @Test
    void ignoreReturnsUpdatedSchemasWithoutDdlOrConfirmation() throws Exception {
        Table baseline = table("id", "name");
        Map<String, String> context = new HashMap<>(CONTEXT);
        context.put(DataSourceConfigKeys.SCHEMA_CHANGE_BEHAVIOR, "ignore");
        String[] ddls = {
            "ALTER TABLE t1 ADD COLUMN city INT",
            "ALTER TABLE t1 DROP COLUMN name",
            "ALTER TABLE t1 CHANGE COLUMN name renamed INT"
        };
        Table[] fresh = {table("id", "name", "city"), table("id"), table("id", "renamed")};
        for (int i = 0; i < ddls.length; i++) {
            MySqlDebeziumJsonDeserializer deserializer = newDeserializer(baseline);

            DeserializeResult result =
                    deserializer.deserialize(context, schemaChangeRecord(ddls[i], fresh[i]));

            assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
            assertNull(result.getUnsupportedReason());
            assertTrue(result.getSchemaChanges().isEmpty());
            assertEquals(fresh[i], result.getUpdatedSchemas().get(TABLE).getTable());
            assertEquals(baseline, deserializer.getTableSchemas().get(TABLE).getTable());
        }
    }

    @Test
    void schemaChangeExcludedModifyDoesNotRequireConfirmation() throws Exception {
        Table baseline = table("id", "age");
        MySqlDebeziumJsonDeserializer deserializer = newDeserializer(baseline);
        deserializer.excludeColumnsCache = Map.of(TABLE.table(), Set.of("age"));

        DeserializeResult result =
                deserializer.deserialize(
                        CONTEXT, schemaChangeRecord("ALTER TABLE t1 MODIFY COLUMN age INT", baseline));

        assertNull(result.getUnsupportedReason());
        assertTrue(result.getSchemaChanges().isEmpty());
    }

    @Test
    void schemaChangeDisabledSkipsSchemaEvent() throws Exception {
        MySqlDebeziumJsonDeserializer deserializer = newDeserializer(table("id", "name"));
        Map<String, String> context = new HashMap<>(CONTEXT);
        context.put(DataSourceConfigKeys.SCHEMA_CHANGE_ENABLED, "false");

        DeserializeResult result =
                deserializer.deserialize(
                        context,
                        schemaChangeRecord(
                                "ALTER TABLE t1 ADD COLUMN city INT",
                                table("id", "name", "city")));

        assertEquals(DeserializeResult.Type.EMPTY, result.getType());
        assertTrue(result.getRecords().isEmpty());
    }

    private static MySqlDebeziumJsonDeserializer newDeserializer(Table table) {
        MySqlDebeziumJsonDeserializer deserializer = new MySqlDebeziumJsonDeserializer();
        Map<String, String> props = new HashMap<>();
        props.put(DataSourceConfigKeys.DATABASE, "db1");
        deserializer.init(props);
        deserializer.setTableSchemas(Collections.singletonMap(TABLE, tableChange(table)));
        return deserializer;
    }

    private static SourceRecord schemaChangeRecord(String ddl, Table table) throws Exception {
        return schemaChangeRecord(ddl, new TableChanges().alter(table));
    }

    private static SourceRecord schemaChangeRecord(String ddl, TableChanges tableChanges) throws Exception {
        Schema keySchema = SchemaBuilder.struct().name(RecordUtils.SCHEMA_CHANGE_EVENT_KEY_NAME).build();
        Struct key = new Struct(keySchema);
        Schema valueSchema =
                SchemaBuilder.struct()
                        .field(HISTORY_RECORD_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .build();
        Struct value =
                new Struct(valueSchema)
                        .put(HISTORY_RECORD_FIELD, historyRecord(ddl, tableChanges).toString());
        return new SourceRecord(
                Collections.singletonMap("server", "mysql"),
                Collections.singletonMap("pos", 1L),
                "mysql.schema-changes",
                keySchema,
                key,
                valueSchema,
                value);
    }

    private static HistoryRecord historyRecord(String ddl, TableChanges tableChanges) throws Exception {
        io.debezium.document.Document document =
                new HistoryRecord(
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                TABLE.catalog(),
                                TABLE.table(),
                                ddl,
                                null)
                        .document();
        document.setArray(HistoryRecord.Fields.TABLE_CHANGES, TABLE_CHANGE_SERIALIZER.serialize(tableChanges));
        return new HistoryRecord(document);
    }

    private static TableChanges.TableChange tableChange(Table table) {
        return new TableChanges.TableChange(TableChanges.TableChangeType.ALTER, table);
    }

    private static Table table(String... columns) {
        TableEditor editor = Table.editor().tableId(TABLE);
        for (String column : columns) {
            editor.addColumns(
                    Column.editor()
                            .name(column)
                            .type("INT")
                            .jdbcType(Types.INTEGER)
                            .optional(true)
                            .create());
        }
        return editor.create();
    }
}
