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
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergWriteSchemaContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Default;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.qe.ConnectContext;

import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.TableProperties;

import java.util.Map;
import java.util.Optional;

/**
 * Helpers for Iceberg row-level DML commands.
 */
final class IcebergDmlCommandUtils {
    private IcebergDmlCommandUtils() {
    }

    static void checkDeleteMode(IcebergExternalTable table) {
        checkNotCopyOnWrite(table, "DELETE", TableProperties.DELETE_MODE,
                TableProperties.DELETE_MODE_DEFAULT);
    }

    static void checkUpdateMode(IcebergExternalTable table) {
        checkNotCopyOnWrite(table, "UPDATE", TableProperties.UPDATE_MODE,
                TableProperties.UPDATE_MODE_DEFAULT);
    }

    static void checkMergeMode(IcebergExternalTable table) {
        checkNotCopyOnWrite(table, "MERGE INTO", TableProperties.MERGE_MODE,
                TableProperties.MERGE_MODE_DEFAULT);
    }

    static Optional<IcebergWriteSchemaContext> installWriteSchemaContext(
            ConnectContext context, IcebergWriteSchemaContext writeSchemaContext) {
        Optional<IcebergWriteSchemaContext> previous =
                context.getStatementContext().getIcebergWriteSchemaContext();
        context.getStatementContext().setIcebergWriteSchemaContext(Optional.of(writeSchemaContext));
        return previous;
    }

    static void restoreWriteSchemaContext(
            ConnectContext context, Optional<IcebergWriteSchemaContext> previous) {
        context.getStatementContext().setIcebergWriteSchemaContext(previous);
    }

    static Expression resolveDefaultReferences(
            Expression expression, IcebergWriteSchemaContext writeSchemaContext) {
        return expression.rewriteDownShortCircuit(candidate -> {
            if (!(candidate instanceof Default)) {
                return candidate;
            }
            Expression reference = candidate.child(0);
            Column column;
            if (reference instanceof UnboundSlot) {
                String columnName = ((UnboundSlot) reference).getNameParts()
                        .get(((UnboundSlot) reference).getNameParts().size() - 1);
                column = writeSchemaContext.getColumns().stream()
                        .filter(targetColumn -> targetColumn.getName().equalsIgnoreCase(columnName))
                        .findFirst()
                        .orElseThrow(() -> new AnalysisException(
                                "Cannot find column information for DEFAULT(" + columnName + ")"));
            } else if (reference instanceof SlotReference
                    && ((SlotReference) reference).getOriginalColumn().isPresent()) {
                column = ((SlotReference) reference).getOriginalColumn().get();
                if (!writeSchemaContext.findField(column).isPresent()) {
                    throw new AnalysisException(
                            "Cannot find column information for DEFAULT(" + column.getName() + ")");
                }
            } else {
                throw new AnalysisException("DEFAULT requires a column reference");
            }
            return writeSchemaContext.resolveWriteDefault(column);
        });
    }

    private static void checkNotCopyOnWrite(IcebergExternalTable table, String operation,
            String modeProperty, String defaultMode) {
        Map<String, String> properties = table.getIcebergTable().properties();
        String mode = properties.getOrDefault(modeProperty, defaultMode);
        if (RowLevelOperationMode.COPY_ON_WRITE.modeName().equalsIgnoreCase(mode)) {
            throw new AnalysisException(String.format(
                    "Doris does not support %s on Iceberg copy-on-write tables. "
                            + "Set table property '%s' to 'merge-on-read'.",
                    operation, modeProperty));
        }
    }
}
