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

import org.apache.doris.cdcclient.common.Constants;
import org.apache.doris.cdcclient.source.parse.mysql.CustomMySqlAntlrDdlParser;
import org.apache.doris.cdcclient.source.parse.mysql.MySqlSchemaChange;
import org.apache.doris.cdcclient.utils.SchemaChangeHelper;
import org.apache.doris.cdcclient.utils.SchemaChangeOperation;
import org.apache.doris.job.cdc.DataSourceConfigKeys;

import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import io.debezium.document.Array;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL-specific deserializer that handles DDL schema change events.
 *
 * <p>When a schema change event is detected, it parses the HistoryRecord, computes the diff against
 * stored tableSchemas, generates Doris ALTER TABLE SQL, and returns a SCHEMA_CHANGE result.
 */
public class MySqlDebeziumJsonDeserializer extends DebeziumJsonDeserializer {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MySqlDebeziumJsonDeserializer.class);
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    private String targetDb;

    @Override
    public void init(Map<String, String> props) {
        super.init(props);
        this.targetDb = props.get(DataSourceConfigKeys.DATABASE);
    }

    @Override
    public DeserializeResult deserialize(Map<String, String> context, SourceRecord record)
            throws IOException {
        if (RecordUtils.isSchemaChangeEvent(record)) {
            if (!isSchemaChangeEnabled(context)) {
                LOG.info("[SCHEMA-CHANGE-SKIPPED] MySQL schema change is disabled.");
                return DeserializeResult.empty();
            }
            return handleSchemaChangeEvent(record, context);
        }
        return super.deserialize(context, record);
    }

    private DeserializeResult handleSchemaChangeEvent(
            SourceRecord record, Map<String, String> context) throws IOException {
        HistoryRecord history = RecordUtils.getHistoryRecord(record);
        String database = history.document().getString(HistoryRecord.Fields.DATABASE_NAME);
        String ddl = history.document().getString(HistoryRecord.Fields.DDL_STATEMENTS);
        Map<TableId, TableChanges.TableChange> freshSchemas = deserializeTableChanges(history);
        LOG.info(
                "[SCHEMA-CHANGE] MySQL deserializer received schema change, baselineSchemas={},"
                        + " freshSchemas={}, ddl={}",
                tableSchemas == null ? 0 : tableSchemas.size(),
                freshSchemas.size(),
                ddl);
        if (freshSchemas.isEmpty()) {
            LOG.warn(
                    "[SCHEMA-CHANGE-SKIPPED] MySQL schema change event has no table schema "
                            + "change. DDL: {}",
                    ddl);
            return DeserializeResult.empty();
        }

        if (tableSchemas == null || tableSchemas.isEmpty()) {
            LOG.info(
                    "[SCHEMA-CHANGE] MySQL schema baseline is empty, adopting fresh schemas as "
                            + "baseline without emitting Doris DDL. DDL: {}",
                    ddl);
            return DeserializeResult.schemaChange(Collections.emptyList(), freshSchemas);
        }
        for (TableId tableId : freshSchemas.keySet()) {
            if (!tableSchemas.containsKey(tableId)) {
                LOG.info(
                        "[SCHEMA-CHANGE] MySQL table {} has no baseline, adopting fresh schemas as "
                                + "baseline without emitting Doris DDL. DDL: {}",
                        tableId.identifier(),
                        ddl);
                return DeserializeResult.schemaChange(Collections.emptyList(), freshSchemas);
            }
        }

        if (isSchemaChangeIgnored(context)) {
            LOG.info(
                    "[SCHEMA-CHANGE-IGNORED] MySQL target DDL skipped for tables {}",
                    freshSchemas.keySet());
            return DeserializeResult.schemaChange(Collections.emptyList(), freshSchemas);
        }

        for (Map.Entry<TableId, TableChanges.TableChange> entry : freshSchemas.entrySet()) {
            if (entry.getValue().getType() != TableChanges.TableChangeType.ALTER) {
                continue;
            }
            TableId tableId = entry.getKey();
            if (!tableSchemas
                    .get(tableId)
                    .getTable()
                    .primaryKeyColumnNames()
                    .equals(entry.getValue().getTable().primaryKeyColumnNames())) {
                return unsupportedSchemaChange(
                        record, ddl, "Primary key changes are not supported", freshSchemas);
            }
        }

        Tables tables = toTables(tableSchemas);
        List<MySqlSchemaChange> changes = Collections.emptyList();
        try {
            CustomMySqlAntlrDdlParser parser = new CustomMySqlAntlrDdlParser();
            parser.setCurrentDatabase(database);
            parser.parse(ddl, tables);
            changes = parser.getAndClearParsedChanges();
        } catch (Exception e) {
            LOG.warn("Failed to parse MySQL schema change DDL: {}", ddl, e);
            return unsupportedSchemaChange(record, ddl, "Cannot parse schema change", freshSchemas);
        }
        if (changes.isEmpty()) {
            for (Map.Entry<TableId, TableChanges.TableChange> entry : freshSchemas.entrySet()) {
                if (entry.getValue().getType() != TableChanges.TableChangeType.ALTER) {
                    continue;
                }
                TableId tableId = entry.getKey();
                Set<String> excluded =
                        excludeColumnsCache.getOrDefault(tableId.table(), Collections.emptySet());
                Set<String> before =
                        tableSchemas.get(tableId).getTable().columns().stream()
                                .map(Column::name)
                                .filter(name -> !excluded.contains(name))
                                .collect(Collectors.toSet());
                Set<String> after =
                        entry.getValue().getTable().columns().stream()
                                .map(Column::name)
                                .filter(name -> !excluded.contains(name))
                                .collect(Collectors.toSet());
                if (!before.equals(after)) {
                    return unsupportedSchemaChange(
                            record, ddl, "Unrecognized column change", freshSchemas);
                }
            }
            return DeserializeResult.schemaChange(Collections.emptyList(), freshSchemas);
        }

        List<MySqlSchemaChange> supportedChanges = new ArrayList<>();
        for (MySqlSchemaChange change : changes) {
            MySqlSchemaChange.Type type = change.getType();
            if (type == MySqlSchemaChange.Type.ADD || type == MySqlSchemaChange.Type.DROP) {
                supportedChanges.add(change);
                continue;
            }

            TableId tableId = change.getTableId();
            String columnName = change.getColumnName();
            String newColumnName = change.getNewColumnName();
            Set<String> excluded =
                    excludeColumnsCache.getOrDefault(tableId.table(), Collections.emptySet());
            if (excluded.contains(columnName) && excluded.contains(newColumnName)) {
                continue;
            }
            return unsupportedSchemaChange(
                    record,
                    ddl,
                    type + " COLUMN is not supported (" + columnName + " -> " + newColumnName + ")",
                    freshSchemas);
        }
        if (supportedChanges.isEmpty()) {
            return DeserializeResult.schemaChange(Collections.emptyList(), freshSchemas);
        }
        for (MySqlSchemaChange change : supportedChanges) {
            TableId tableId = change.getTableId();
            if (tableId == null || !tableSchemas.containsKey(tableId)) {
                LOG.warn(
                        "[SCHEMA-CHANGE-SKIPPED] MySQL schema change table {} has no baseline."
                                + " No Doris DDL emitted, FE baseline will advance to the schema"
                                + " carried by the history record. DDL: {}",
                        tableId == null ? "<unknown>" : tableId.identifier(),
                        ddl);
                return DeserializeResult.schemaChange(Collections.emptyList(), freshSchemas);
            }
        }

        List<SchemaChangeOperation> schemaChanges = new ArrayList<>();
        for (MySqlSchemaChange change : supportedChanges) {
            TableId tableId = change.getTableId();
            Set<String> excludedCols =
                    excludeColumnsCache.getOrDefault(tableId.table(), Collections.emptySet());
            String targetTable = resolveTargetTable(tableId.table());
            String db = context.get(Constants.DORIS_TARGET_DB);
            if (change.getType() == MySqlSchemaChange.Type.ADD) {
                String columnName = change.getColumn().name();
                if (excludedCols.contains(columnName)) {
                    LOG.info(
                            "[SCHEMA-CHANGE] MySQL table {}: added column '{}' is excluded,"
                                    + " skipping ADD",
                            tableId.identifier(),
                            columnName);
                    continue;
                }
                String colType = SchemaChangeHelper.mysqlColumnToDorisType(change.getColumn());
                schemaChanges.add(
                        SchemaChangeOperation.addColumn(
                                targetTable,
                                columnName,
                                SchemaChangeHelper.buildAddColumnSql(
                                        db,
                                        targetTable,
                                        columnName,
                                        colType,
                                        change.getColumn().comment())));
            } else {
                String columnName = change.getColumnName();
                if (excludedCols.contains(columnName)) {
                    LOG.info(
                            "[SCHEMA-CHANGE] MySQL table {}: dropped column '{}' is excluded,"
                                    + " skipping DROP",
                            tableId.identifier(),
                            columnName);
                    continue;
                }
                schemaChanges.add(
                        SchemaChangeOperation.dropColumn(
                                targetTable,
                                columnName,
                                SchemaChangeHelper.buildDropColumnSql(
                                        db, targetTable, columnName)));
            }
        }

        LOG.info(
                "MySQL schema change for DDL '{}': {}",
                ddl,
                schemaChanges.stream()
                        .map(SchemaChangeOperation::getSql)
                        .collect(Collectors.toList()));
        return DeserializeResult.schemaChange(schemaChanges, freshSchemas);
    }

    private Map<TableId, TableChanges.TableChange> deserializeTableChanges(HistoryRecord history) {
        Array tableChanges = history.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
        TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
        Map<TableId, TableChanges.TableChange> result = new ConcurrentHashMap<>();
        for (TableChanges.TableChange tableChange : changes) {
            // DROP has no post-change table schema and does not produce target DDL.
            if (tableChange.getType() == TableChanges.TableChangeType.DROP) {
                continue;
            }
            result.put(tableChange.getTable().id(), tableChange);
        }
        return result;
    }

    private DeserializeResult unsupportedSchemaChange(
            SourceRecord record,
            String ddl,
            String reason,
            Map<TableId, TableChanges.TableChange> freshSchemas) {
        LOG.info(
                "[SCHEMA-CHANGE-DETAIL] MySQL tables={}: {}. DDL: {}. sourceOffset={}",
                freshSchemas.keySet(),
                reason,
                ddl,
                record.sourceOffset());
        return DeserializeResult.unsupportedSchemaChange(reason, tableSchemas, freshSchemas);
    }

    private Tables toTables(Map<TableId, TableChanges.TableChange> schemas) {
        Tables tables = new Tables();
        for (TableChanges.TableChange change : schemas.values()) {
            tables.overwriteTable(change.getTable());
        }
        return tables;
    }
}
