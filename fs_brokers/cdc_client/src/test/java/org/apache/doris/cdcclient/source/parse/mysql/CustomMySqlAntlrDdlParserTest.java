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

package org.apache.doris.cdcclient.source.parse.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.Test;

class CustomMySqlAntlrDdlParserTest {
    private static final TableId TABLE = new TableId("db1", null, "t1");

    @Test
    void parseAddColumn() {
        Tables tables = tables(table("id"));
        CustomMySqlAntlrDdlParser parser = parser();

        parser.parse("ALTER TABLE t1 ADD COLUMN age INT", tables);

        List<MySqlSchemaChange> changes = parser.getAndClearParsedChanges();
        assertEquals(1, changes.size());
        assertEquals(MySqlSchemaChange.Type.ADD, changes.get(0).getType());
        assertEquals(TABLE, changes.get(0).getTableId());
        assertEquals("age", changes.get(0).getColumn().name());
        assertTrue(parser.getAndClearParsedChanges().isEmpty());
    }

    @Test
    void parseDropColumns() {
        Tables tables = tables(table("id", "age", "city"));
        CustomMySqlAntlrDdlParser parser = parser();

        parser.parse("ALTER TABLE t1 DROP COLUMN age, DROP city", tables);

        List<MySqlSchemaChange> changes = parser.getAndClearParsedChanges();
        assertEquals(2, changes.size());
        assertEquals(MySqlSchemaChange.Type.DROP, changes.get(0).getType());
        assertEquals("age", changes.get(0).getColumnName());
        assertEquals("city", changes.get(1).getColumnName());
    }

    @Test
    void parseUnsupportedColumnChanges() {
        assertUnsupported("ALTER TABLE t1 RENAME COLUMN age TO years");
        assertUnsupported("ALTER TABLE t1 CHANGE COLUMN age years BIGINT");
        assertUnsupported("ALTER TABLE t1 MODIFY COLUMN age BIGINT");
    }

    @Test
    void parseMixedAddDropColumns() {
        Tables tables = tables(table("id", "age"));
        CustomMySqlAntlrDdlParser parser = parser();

        parser.parse("ALTER TABLE t1 ADD COLUMN city VARCHAR(30), DROP COLUMN age", tables);

        List<MySqlSchemaChange> changes = parser.getAndClearParsedChanges();
        assertEquals(2, changes.size());
        assertEquals(MySqlSchemaChange.Type.ADD, changes.get(0).getType());
        assertEquals("city", changes.get(0).getColumn().name());
        assertEquals(MySqlSchemaChange.Type.DROP, changes.get(1).getType());
        assertEquals("age", changes.get(1).getColumnName());
    }

    private static void assertUnsupported(String ddl) {
        Tables tables = tables(table("id", "age"));
        CustomMySqlAntlrDdlParser parser = parser();
        parser.parse(ddl, tables);
        List<MySqlSchemaChange> changes = parser.getAndClearParsedChanges();
        assertEquals(MySqlSchemaChange.Type.UNSUPPORTED, changes.get(0).getType());
    }

    private static CustomMySqlAntlrDdlParser parser() {
        CustomMySqlAntlrDdlParser parser = new CustomMySqlAntlrDdlParser();
        parser.setCurrentDatabase("db1");
        return parser;
    }

    private static Tables tables(Table table) {
        Tables tables = new Tables();
        tables.overwriteTable(table);
        return tables;
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
