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

import java.util.ArrayList;
import java.util.List;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.antlr.listener.ColumnDefinitionParserListener;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import org.antlr.v4.runtime.tree.ParseTreeListener;

final class CustomAlterTableParserListener extends MySqlParserBaseListener {
    private static final int STARTING_INDEX = 1;

    private final MySqlAntlrDdlParser parser;
    private final List<ParseTreeListener> listeners;
    private final List<MySqlSchemaChange> changes;
    private List<ColumnEditor> columnEditors;
    private ColumnDefinitionParserListener columnDefinitionListener;
    private TableEditor tableEditor;
    private TableId currentTableId;
    private int parsingColumnIndex = STARTING_INDEX;

    CustomAlterTableParserListener(
            MySqlAntlrDdlParser parser,
            List<ParseTreeListener> listeners,
            List<MySqlSchemaChange> changes) {
        this.parser = parser;
        this.listeners = listeners;
        this.changes = changes;
    }

    @Override
    public void enterAlterTable(MySqlParser.AlterTableContext ctx) {
        currentTableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
        tableEditor = parser.databaseTables().editTable(currentTableId);
        super.enterAlterTable(ctx);
    }

    @Override
    public void exitAlterTable(MySqlParser.AlterTableContext ctx) {
        currentTableId = null;
        tableEditor = null;
        super.exitAlterTable(ctx);
    }

    @Override
    public void enterAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        String columnName = parser.parseName(ctx.uid(0));
        ColumnEditor columnEditor = Column.editor().name(columnName);
        columnDefinitionListener =
                new ColumnDefinitionParserListener(tableEditor, columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);
        super.enterAlterByAddColumn(ctx);
    }

    @Override
    public void exitAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    changes.add(
                            MySqlSchemaChange.add(
                                    currentTableId, columnDefinitionListener.getColumn()));
                    listeners.remove(columnDefinitionListener);
                },
                columnDefinitionListener);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void enterAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        columnEditors = new ArrayList<>(ctx.uid().size());
        for (MySqlParser.UidContext uidContext : ctx.uid()) {
            String columnName = parser.parseName(uidContext);
            columnEditors.add(Column.editor().name(columnName));
        }
        columnDefinitionListener =
                new ColumnDefinitionParserListener(
                        tableEditor, columnEditors.get(0), parser, listeners);
        listeners.add(columnDefinitionListener);
        super.enterAlterByAddColumns(ctx);
    }

    @Override
    public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (columnEditors.size() > parsingColumnIndex) {
                        columnDefinitionListener.setColumnEditor(
                                columnEditors.get(parsingColumnIndex++));
                    }
                },
                columnEditors);
        super.exitColumnDefinition(ctx);
    }

    @Override
    public void exitAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        parser.runIfNotNull(
                () -> {
                    for (ColumnEditor columnEditor : columnEditors) {
                        changes.add(MySqlSchemaChange.add(currentTableId, columnEditor.create()));
                    }
                    listeners.remove(columnDefinitionListener);
                    columnEditors = null;
                    parsingColumnIndex = STARTING_INDEX;
                },
                columnEditors);
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
