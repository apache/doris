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
import org.apache.doris.cdcclient.utils.SchemaChangeHelper;

import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;

import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostgreSQL-specific deserializer with event-driven schema change handling.
 *
 * <p>Schema changes are detected from pgoutput Relation messages, which flink-cdc surfaces as
 * {@link PostgresSchemaRecord} (the source is created with {@code includeSchemaChanges(true)}). The
 * carried Debezium {@link Table} is the full post-change schema and is diffed against the stored
 * baseline — the Doris table's current full schema, loaded from FE — to derive ADD/DROP column DDL.
 * Regular DML records are emitted directly without per-record schema comparison.
 *
 * <p>The baseline is also established by Relation events: when a table first appears (e.g. a stream
 * started directly from an offset without a snapshot), pgoutput sends its Relation before the first
 * DML, and the {@code stored == null} branch of {@link #handleSchemaChangeEvent} adopts the current
 * schema as the baseline (no DDL). No JDBC fallback is needed — a DML can only reach this
 * deserializer after Debezium has resolved its Relation (otherwise Debezium drops it as a
 * NoopMessage), and that Relation has already established the baseline.
 *
 * <p>Only ADD and DROP column are emitted. A simultaneous ADD+DROP is treated as a possible RENAME
 * and skipped (RENAME manually in Doris). MODIFY column type is not emitted.
 *
 * <p>The emitted DDL is only applied on the from-to (at-least-once) write path; the TVF
 * (exactly-once) fetch path consumes DML only and does not execute schema-change records, so
 * automatic schema change is effective for from-to mode.
 */
public class PostgresDebeziumJsonDeserializer extends DebeziumJsonDeserializer {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(PostgresDebeziumJsonDeserializer.class);

    @Override
    public DeserializeResult deserialize(Map<String, String> context, SourceRecord record)
            throws IOException {
        // 1. Schema change event (pgoutput Relation -> PostgresSchemaRecord).
        if (SourceRecordUtils.isSchemaChangeEvent(record)) {
            return handleSchemaChangeEvent(context, record);
        }
        // 2. Non-DML (heartbeat / watermark / etc.).
        if (!RecordUtils.isDataChangeRecord(record)) {
            return DeserializeResult.empty();
        }
        // 3. DML. The baseline is always established by the table's preceding Relation event
        //    (pgoutput sends R before a table's first DML on every replication connection; a DML
        //    whose Relation was never seen is dropped inside Debezium as a NoopMessage and never
        //    reaches here). A missing baseline is therefore unreachable; warn and still emit data.
        TableId tableId = extractTableId(record);
        TableChanges.TableChange stored = tableSchemas != null ? tableSchemas.get(tableId) : null;
        if (stored == null || stored.getTable() == null) {
            LOG.warn(
                    "No schema baseline for table {} on DML; expected the preceding Relation event"
                            + " to have established it. Emitting data without a baseline.",
                    tableId.identifier());
        }
        return super.deserialize(context, record);
    }

    /**
     * Handle a pgoutput Relation-driven schema change: diff the post-change PG schema (carried by
     * {@link PostgresSchemaRecord}) against the stored Doris baseline and emit ADD/DROP column DDL.
     * When no baseline exists yet (first appearance of the table), adopt the fresh schema as the
     * baseline without emitting any DDL.
     */
    private DeserializeResult handleSchemaChangeEvent(
            Map<String, String> context, SourceRecord record) {
        Table freshTable = ((PostgresSchemaRecord) record).getTable();
        // Debezium PG TableId is (catalog=null, schema, table) — matches the tableSchemas key.
        TableId tableId = freshTable.id();
        TableChanges.TableChange stored = tableSchemas != null ? tableSchemas.get(tableId) : null;

        // changeType is not consumed inside cdc_client — downstream only reads getTable() and
        // serializeTableSchemas does not persist it — so ALTER is used uniformly, including for the
        // first-time baseline below (which is semantically a CREATE).
        TableChanges.TableChange freshChange =
                new TableChanges.TableChange(TableChanges.TableChangeType.ALTER, freshTable);
        Map<TableId, TableChanges.TableChange> updatedSchemas = new HashMap<>();

        // No baseline yet: adopt the fresh schema as baseline, no DDL.
        if (stored == null || stored.getTable() == null) {
            updatedSchemas.put(tableId, freshChange);
            return DeserializeResult.schemaChange(
                    Collections.emptyList(), updatedSchemas, Collections.emptyList());
        }

        List<Column> added = new ArrayList<>();
        List<String> dropped = new ArrayList<>();
        for (Column col : freshTable.columns()) {
            if (stored.getTable().columnWithName(col.name()) == null) {
                added.add(col);
            }
        }
        for (Column col : stored.getTable().columns()) {
            if (freshTable.columnWithName(col.name()) == null) {
                dropped.add(col.name());
            }
        }

        // No-op: Relation re-emitted but schema unchanged vs the FE baseline (idempotent).
        if (added.isEmpty() && dropped.isEmpty()) {
            return DeserializeResult.empty();
        }

        updatedSchemas.put(tableId, freshChange);

        // Rename guard: simultaneous ADD+DROP may be a RENAME — skip DDL to avoid data loss.
        if (!added.isEmpty() && !dropped.isEmpty()) {
            LOG.warn(
                    "[SCHEMA-CHANGE-SKIPPED] Table {}: simultaneous DROP {} and ADD {} looks like a"
                            + " RENAME; no DDL emitted, please RENAME column(s) manually in Doris.",
                    tableId.identifier(),
                    dropped,
                    added.stream().map(Column::name).collect(Collectors.toList()));
            return DeserializeResult.schemaChange(
                    Collections.emptyList(), updatedSchemas, Collections.emptyList());
        }

        String db = context.get(Constants.DORIS_TARGET_DB);
        Set<String> excludedCols =
                excludeColumnsCache.getOrDefault(tableId.table(), Collections.emptySet());
        List<String> ddls = new ArrayList<>();

        for (String colName : dropped) {
            if (excludedCols.contains(colName)) {
                LOG.info(
                        "[SCHEMA-CHANGE] Table {}: dropped column '{}' is excluded, skipping DROP",
                        tableId.identifier(),
                        colName);
                continue;
            }
            ddls.add(
                    SchemaChangeHelper.buildDropColumnSql(
                            db, resolveTargetTable(tableId.table()), colName));
        }

        for (Column col : added) {
            if (excludedCols.contains(col.name())) {
                LOG.info(
                        "[SCHEMA-CHANGE] Table {}: added column '{}' is excluded, skipping ADD",
                        tableId.identifier(),
                        col.name());
                continue;
            }
            String colType = SchemaChangeHelper.columnToDorisType(col);
            // Do not propagate source DEFAULT expressions or NOT NULL. PostgreSQL evaluates
            // defaults before writing new rows to WAL, so subsequent DML carries the actual value.
            // Existing Doris rows are not backfilled and must remain valid with NULL in this column.
            ddls.add(
                    SchemaChangeHelper.buildAddColumnSql(
                            db,
                            resolveTargetTable(tableId.table()),
                            col.name(),
                            colType,
                            null,
                            null));
        }

        LOG.info(
                "Postgres schema change (event-driven) for table {}: added={}, dropped={}. DDLs: {}",
                tableId.identifier(),
                added.stream().map(Column::name).collect(Collectors.toList()),
                dropped,
                ddls);
        return DeserializeResult.schemaChange(ddls, updatedSchemas, Collections.emptyList());
    }

    private TableId extractTableId(SourceRecord record) {
        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String schemaName = source.getString(SCHEMA_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(null, schemaName, tableName);
    }

}
