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

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.util.List;

final class CustomAlterTableParserListener extends MySqlParserBaseListener {
    private final MySqlAntlrDdlParser parser;
    private final List<MySqlSchemaChange> changes;
    private TableId currentTableId;

    CustomAlterTableParserListener(
            MySqlAntlrDdlParser parser, List<MySqlSchemaChange> changes) {
        this.parser = parser;
        this.changes = changes;
    }

    @Override
    public void enterAlterTable(MySqlParser.AlterTableContext ctx) {
        currentTableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
        super.enterAlterTable(ctx);
    }

    @Override
    public void exitAlterTable(MySqlParser.AlterTableContext ctx) {
        currentTableId = null;
        super.exitAlterTable(ctx);
    }

    @Override
    public void exitAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        String columnName = parser.parseName(ctx.uid(0));
        Table table = parser.databaseTables().forTable(currentTableId);
        changes.add(MySqlSchemaChange.add(currentTableId, table.columnWithName(columnName)));
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void exitAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        for (MySqlParser.UidContext uidContext : ctx.uid()) {
            String columnName = parser.parseName(uidContext);
            Table table = parser.databaseTables().forTable(currentTableId);
            changes.add(MySqlSchemaChange.add(currentTableId, table.columnWithName(columnName)));
        }
        super.exitAlterByAddColumns(ctx);
    }

    @Override
    public void enterAlterByDropColumn(MySqlParser.AlterByDropColumnContext ctx) {
        changes.add(MySqlSchemaChange.drop(currentTableId, parser.parseName(ctx.uid())));
        super.enterAlterByDropColumn(ctx);
    }

    @Override
    public void enterAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        changes.add(
                MySqlSchemaChange.unsupported(
                        currentTableId, "CHANGE", "CHANGE COLUMN is not supported"));
        super.enterAlterByChangeColumn(ctx);
    }

    @Override
    public void enterAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
        changes.add(
                MySqlSchemaChange.unsupported(
                        currentTableId, "MODIFY", "MODIFY COLUMN is not supported"));
        super.enterAlterByModifyColumn(ctx);
    }

    @Override
    public void enterAlterByRenameColumn(MySqlParser.AlterByRenameColumnContext ctx) {
        changes.add(
                MySqlSchemaChange.unsupported(
                        currentTableId, "RENAME", "RENAME COLUMN is not supported"));
        super.enterAlterByRenameColumn(ctx);
    }
}
