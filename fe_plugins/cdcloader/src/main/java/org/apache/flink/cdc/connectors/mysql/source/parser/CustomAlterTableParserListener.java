/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.source.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.types.DataType;
import static org.apache.flink.cdc.connectors.mysql.utils.MySqlTypeUtils.fromDbzColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copied from:
 * https://github.com/apache/flink-cdc/blob/release-3.1.1/flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql/src/main/java/org/apache/flink/cdc/connectors/mysql/source/parser/CustomAlterTableParserListener.java
 *
 * */
public class CustomAlterTableParserListener extends MySqlParserBaseListener {

    private static final int STARTING_INDEX = 1;

    private static final Logger LOG = LoggerFactory.getLogger(CustomAlterTableParserListener.class);

    private final MySqlAntlrDdlParser parser;
    private final List<ParseTreeListener> listeners;
    private final LinkedList<SchemaChangeEvent> changes;
    private org.apache.flink.cdc.common.event.TableId currentTable;
    private List<ColumnEditor> columnEditors;

    private CustomColumnDefinitionParserListener columnDefinitionListener;

    private int parsingColumnIndex = STARTING_INDEX;

    public CustomAlterTableParserListener(
            MySqlAntlrDdlParser parser,
            List<ParseTreeListener> listeners,
            LinkedList<SchemaChangeEvent> changes) {
        this.parser = parser;
        this.listeners = listeners;
        this.changes = changes;
    }

    @Override
    public void enterAlterTable(MySqlParser.AlterTableContext ctx) {
        this.currentTable = toCdcTableId(parser.parseQualifiedTableId(ctx.tableName().fullId()));
        super.enterAlterTable(ctx);
    }

    @Override
    public void exitAlterTable(MySqlParser.AlterTableContext ctx) {
        listeners.remove(columnDefinitionListener);
        super.exitAlterTable(ctx);
        this.currentTable = null;
    }

    @Override
    public void enterAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        String columnName = parser.parseName(ctx.uid(0));
        ColumnEditor columnEditor = Column.editor().name(columnName);
        columnDefinitionListener =
                new CustomColumnDefinitionParserListener(columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void exitAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    if (ctx.FIRST() != null) {
                        changes.add(
                                new AddColumnEvent(
                                        currentTable,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        toCdcColumn(column),
                                                        AddColumnEvent.ColumnPosition.FIRST,
                                                        null))));
                    } else if (ctx.AFTER() != null) {
                        String afterColumn = parser.parseName(ctx.uid(1));
                        changes.add(
                                new AddColumnEvent(
                                        currentTable,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        toCdcColumn(column),
                                                        AddColumnEvent.ColumnPosition.AFTER,
                                                        afterColumn))));
                    } else {
                        changes.add(
                                new AddColumnEvent(
                                        currentTable,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        toCdcColumn(column)))));
                    }
                    listeners.remove(columnDefinitionListener);
                },
                columnDefinitionListener);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void enterAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        // multiple columns are added. Initialize a list of column editors for them
        columnEditors = new ArrayList<>(ctx.uid().size());
        for (MySqlParser.UidContext uidContext : ctx.uid()) {
            String columnName = parser.parseName(uidContext);
            columnEditors.add(Column.editor().name(columnName));
        }
        columnDefinitionListener =
                new CustomColumnDefinitionParserListener(columnEditors.get(0), parser, listeners);
        listeners.add(columnDefinitionListener);
        super.enterAlterByAddColumns(ctx);
    }

    @Override
    public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (columnEditors != null) {
                        // column editor list is not null when a multiple columns are parsed in one
                        // statement
                        if (columnEditors.size() > parsingColumnIndex) {
                            // assign next column editor to parse another column definition
                            columnDefinitionListener.setColumnEditor(
                                    columnEditors.get(parsingColumnIndex++));
                        }
                    }
                },
                columnEditors);
        super.exitColumnDefinition(ctx);
    }

    @Override
    public void exitAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        parser.runIfNotNull(
                () -> {
                    List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
                    columnEditors.forEach(
                            columnEditor -> {
                                Column column = columnEditor.create();
                                addedColumns.add(
                                        new AddColumnEvent.ColumnWithPosition(toCdcColumn(column)));
                            });
                    changes.add(new AddColumnEvent(currentTable, addedColumns));
                    listeners.remove(columnDefinitionListener);
                    columnEditors = null;
                    parsingColumnIndex = STARTING_INDEX;
                },
                columnEditors);
        super.exitAlterByAddColumns(ctx);
    }

    @Override
    public void enterAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        String oldColumnName = parser.parseName(ctx.oldColumn);
        ColumnEditor columnEditor = Column.editor().name(oldColumnName);
        columnEditor.unsetDefaultValueExpression();

        columnDefinitionListener =
                new CustomColumnDefinitionParserListener(columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);
        super.enterAlterByChangeColumn(ctx);
    }

    @Override
    public void exitAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    String newColumnName = parser.parseName(ctx.newColumn);

                    Map<String, DataType> typeMapping = new HashMap<>();
                    typeMapping.put(column.name(), fromDbzColumn(column));
                    changes.add(new AlterColumnTypeEvent(currentTable, typeMapping));

                    if (newColumnName != null && !column.name().equalsIgnoreCase(newColumnName)) {
                        Map<String, String> renameMap = new HashMap<>();
                        renameMap.put(column.name(), newColumnName);
                        changes.add(new RenameColumnEvent(currentTable, renameMap));
                    }
                    listeners.remove(columnDefinitionListener);
                },
                columnDefinitionListener);
        super.exitAlterByChangeColumn(ctx);
    }

    @Override
    public void enterAlterByDropColumn(MySqlParser.AlterByDropColumnContext ctx) {
        String removedColName = parser.parseName(ctx.uid());
        changes.add(new DropColumnEvent(currentTable, Collections.singletonList(removedColName)));
        super.enterAlterByDropColumn(ctx);
    }

    @Override
    public void enterAlterByRenameColumn(MySqlParser.AlterByRenameColumnContext ctx) {
        String oldColumnName = parser.parseName(ctx.oldColumn);
        ColumnEditor columnEditor = Column.editor().name(oldColumnName);
        columnDefinitionListener =
                new CustomColumnDefinitionParserListener(columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);
        super.enterAlterByRenameColumn(ctx);
    }

    @Override
    public void exitAlterByRenameColumn(MySqlParser.AlterByRenameColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    String newColumnName = parser.parseName(ctx.newColumn);
                    if (newColumnName != null && !column.name().equalsIgnoreCase(newColumnName)) {
                        Map<String, String> renameMap = new HashMap<>();
                        renameMap.put(column.name(), newColumnName);
                        changes.add(new RenameColumnEvent(currentTable, renameMap));
                    }
                    listeners.remove(columnDefinitionListener);
                },
                columnDefinitionListener);
        super.exitAlterByRenameColumn(ctx);
    }

    private org.apache.flink.cdc.common.schema.Column toCdcColumn(Column dbzColumn) {
        return org.apache.flink.cdc.common.schema.Column.physicalColumn(
                dbzColumn.name(), fromDbzColumn(dbzColumn), dbzColumn.comment());
    }

    private org.apache.flink.cdc.common.event.TableId toCdcTableId(TableId dbzTableId) {
        return org.apache.flink.cdc.common.event.TableId.tableId(
                dbzTableId.catalog(), dbzTableId.table());
    }
}
