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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Default;
import org.apache.doris.nereids.trees.expressions.DefaultValueSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Neutral engine helpers for request-scoped connector writer schemas and DEFAULT expressions. */
final class ConnectorWriteSchemaUtils {

    private ConnectorWriteSchemaUtils() {
    }

    static List<Column> pinAndGet(ConnectContext context, ExternalTable table) {
        if (!(table instanceof PluginDrivenExternalTable)) {
            return table.getBaseSchema(true);
        }
        PluginDrivenExternalTable pluginTable = (PluginDrivenExternalTable) table;
        Optional<List<Column>> pinned =
                context.getStatementContext().getConnectorWriteSchema(table.getId());
        if (!pinned.isPresent()) {
            pluginTable.resolveWriteColumns(Optional.empty())
                    .ifPresent(columns -> context.getStatementContext()
                            .setConnectorWriteSchema(table.getId(), columns));
            pinned = context.getStatementContext().getConnectorWriteSchema(table.getId());
        }
        List<Column> dataColumns = pinned.orElseGet(() -> table.getBaseSchema(true));
        List<Column> writerColumns = new ArrayList<>(dataColumns);
        // Row-level sinks also carry connector-reserved passthrough columns such as Iceberg v3 row lineage.
        // They are not user data and therefore are absent from getWriteColumns().
        for (Column column : table.getBaseSchema(true)) {
            if (column.isReservedPassthrough()
                    && writerColumns.stream().noneMatch(
                            existing -> existing.getUniqueId() == column.getUniqueId())) {
                writerColumns.add(column);
            }
        }
        return ImmutableList.copyOf(writerColumns);
    }

    static Expression resolveDefaultReferences(Expression expression, List<Column> columns,
            ExternalTable targetTable, ConnectContext context,
            List<String> targetNameParts, String targetAlias) {
        return expression.rewriteDownShortCircuit(candidate -> {
            if (!(candidate instanceof Default)) {
                return candidate;
            }
            Expression reference = candidate.child(0);
            Column column;
            if (reference instanceof UnboundSlot) {
                List<String> nameParts = ((UnboundSlot) reference).getNameParts();
                UpdateCommand.checkAssignmentColumn(
                        context, nameParts, targetNameParts, targetAlias);
                column = findColumn(columns, nameParts.get(nameParts.size() - 1));
            } else if (reference instanceof SlotReference
                    && ((SlotReference) reference).getOriginalColumn().isPresent()
                    && ((SlotReference) reference).getOriginalTable()
                            .map(table -> table.getId() == targetTable.getId()).orElse(false)) {
                Column referencedColumn = ((SlotReference) reference).getOriginalColumn().get();
                column = columns.stream()
                        .filter(candidateColumn ->
                                candidateColumn.getUniqueId() == referencedColumn.getUniqueId())
                        .findFirst()
                        .orElse(null);
            } else {
                throw new AnalysisException("DEFAULT requires a target column reference");
            }
            if (column == null) {
                throw new AnalysisException("Cannot find column information for DEFAULT("
                        + reference.toSql() + ")");
            }
            return resolveDefault(column);
        });
    }

    static Expression resolveDefault(Column column) {
        String defaultSql = column.getDefaultValueSql();
        if (defaultSql == null) {
            throw new AnalysisException(
                    "Column has no default value, column=" + column.getName());
        }
        Expression expression = new NereidsParser().parseExpression(defaultSql);
        return expression instanceof UnboundAlias ? expression.child(0) : expression;
    }

    static Expression resolveExplicitDefault(Expression expression, Column column) {
        return expression instanceof DefaultValueSlot ? resolveDefault(column) : expression;
    }

    private static Column findColumn(List<Column> columns, String name) {
        return columns.stream()
                .filter(column -> column.getName().equalsIgnoreCase(name))
                .findFirst()
                .orElse(null);
    }
}
