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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;

class MySqlSchemaChangeDeserializeTest {
    private static final TableId TABLE = new TableId("db1", null, "t1");
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    @Test
    void parserFailureSkipsDdlAndAdvancesBaseline() throws Exception {
        MySqlDebeziumJsonDeserializer deserializer = new MySqlDebeziumJsonDeserializer();
        Map<String, String> props = new HashMap<>();
        props.put(DataSourceConfigKeys.DATABASE, "db1");
        deserializer.init(props);
        deserializer.setTableSchemas(Collections.singletonMap(TABLE, tableChange(table("id"))));

        DeserializeResult result =
                deserializer.deserialize(
                        Collections.emptyMap(),
                        schemaChangeRecord(null, table("id", "city")));

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertTrue(result.getSchemaChanges().isEmpty());
        assertEquals(1, result.getUpdatedSchemas().size());
        assertEquals(table("id", "city"), result.getUpdatedSchemas().get(TABLE).getTable());
    }

    private static SourceRecord schemaChangeRecord(String ddl, Table table) throws Exception {
        Schema keySchema = SchemaBuilder.struct().name(RecordUtils.SCHEMA_CHANGE_EVENT_KEY_NAME).build();
        Struct key = new Struct(keySchema);
        Schema valueSchema =
                SchemaBuilder.struct()
                        .field(HISTORY_RECORD_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .build();
        Struct value =
                new Struct(valueSchema)
                        .put(HISTORY_RECORD_FIELD, historyRecord(ddl, table).toString());
        return new SourceRecord(
                Collections.singletonMap("server", "mysql"),
                Collections.singletonMap("pos", 1L),
                "mysql.schema-changes",
                keySchema,
                key,
                valueSchema,
                value);
    }

    private static HistoryRecord historyRecord(String ddl, Table table) throws Exception {
        io.debezium.document.Document document =
                new HistoryRecord(
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                table.id().catalog(),
                                table.id().table(),
                                ddl,
                                null)
                        .document();
        TableChanges tableChanges = new TableChanges();
        tableChanges.alter(table);
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
