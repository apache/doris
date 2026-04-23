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

import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;

import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostgreSQL-specific deserializer that detects schema changes (ADD/DROP column only) by comparing
 * the record's Kafka Connect schema field names with stored tableSchemas.
 *
 * <p>Because PostgreSQL does not emit DDL events in the WAL stream, schema detection is done by
 * comparing the "after" struct field names in each DML record against the known column set.
 *
 * <p>Type comparison is intentionally skipped to avoid false positives caused by Kafka Connect type
 * ambiguity (e.g. text/varchar/json/uuid all appear as STRING). When a column add or drop is
 * detected, the accurate column types are fetched directly from PostgreSQL via the injected {@link
 * #pgSchemaRefresher} callback.
 *
 * <p>MODIFY column type is not supported — users must manually execute ALTER TABLE ... MODIFY
 * COLUMN in Doris when a PG column type changes.
 */
public class PostgresDebeziumJsonDeserializer extends DebeziumJsonDeserializer {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(PostgresDebeziumJsonDeserializer.class);

    /**
     * Callback to fetch the current PG table schema for a single table via JDBC. Injected by {@link
     * org.apache.doris.cdcclient.source.reader.postgres.PostgresSourceReader} after initialization.
     */
    private transient Function<TableId, TableChanges.TableChange> pgSchemaRefresher;

    public void setPgSchemaRefresher(Function<TableId, TableChanges.TableChange> refresher) {
        this.pgSchemaRefresher = refresher;
    }

    @Override
    public DeserializeResult deserialize(Map<String, String> context, SourceRecord record)
            throws IOException {
        if (!RecordUtils.isDataChangeRecord(record)) {
            return DeserializeResult.empty();
        }

        Schema valueSchema = record.valueSchema();
        if (valueSchema == null) {
            return super.deserialize(context, record);
        }

        Field afterField = valueSchema.field(Envelope.FieldName.AFTER);
        if (afterField == null) {
            return super.deserialize(context, record);
        }

        Schema afterSchema = afterField.schema();
        TableId tableId = extractTableId(record);
        TableChanges.TableChange stored = tableSchemas != null ? tableSchemas.get(tableId) : null;

        // No baseline schema available — cannot detect changes, fall through to normal
        // deserialization
        if (stored == null || stored.getTable() == null) {
            LOG.debug(
                    "No stored schema for table {}, skipping schema change detection.",
                    tableId.identifier());
            return super.deserialize(context, record);
        }

        // First pass: name-only diff — fast, in-memory, no type comparison, no false positives
        SchemaChangeHelper.SchemaDiff nameDiff =
                SchemaChangeHelper.diffSchemaByName(afterSchema, stored);
        if (nameDiff.isEmpty()) {
            return super.deserialize(context, record);
        }

        Preconditions.checkNotNull(
                pgSchemaRefresher,
                "pgSchemaRefresher callback is not set. Cannot fetch fresh PG schema for change detection.");

        // the last fresh schema
        TableChanges.TableChange fresh = pgSchemaRefresher.apply(tableId);
        if (fresh == null || fresh.getTable() == null) {
            // Cannot proceed: DDL must be executed before the triggering DML record is written,
            // otherwise new column data in this record would be silently dropped.
            // Throwing here causes the batch to be retried from the same offset.
            throw new IOException(
                    "Failed to fetch fresh schema for table "
                            + tableId.identifier()
                            + "; cannot apply schema change safely. Will retry.");
        }

        // Second diff: use afterSchema as the source of truth for which columns the current WAL
        // record is aware of. Only process additions/drops visible in afterSchema — columns that
        // exist in fresh (JDBC) but are absent from afterSchema belong to a later DDL that has not
        // yet produced a DML record, and will be processed when that DML record arrives.
        //
        // pgAdded: present in afterSchema but absent from stored → look up Column in fresh for
        //          accurate PG type metadata. If fresh doesn't have the column yet (shouldn't
        //          happen normally), skip it.
        // pgDropped: present in stored but absent from afterSchema.
        List<Column> pgAdded = new ArrayList<>();
        List<String> pgDropped = new ArrayList<>();

        for (Field field : afterSchema.fields()) {
            if (stored.getTable().columnWithName(field.name()) == null) {
                Column freshCol = fresh.getTable().columnWithName(field.name());
                if (freshCol != null) {
                    pgAdded.add(freshCol);
                }
            }
        }

        for (Column col : stored.getTable().columns()) {
            if (afterSchema.field(col.name()) == null) {
                pgDropped.add(col.name());
            }
        }

        // Second diff is empty: nameDiff was a false positive (PG schema unchanged vs stored).
        // This happens when pgSchemaRefresher returns a schema ahead of the current WAL position
        // (e.g. a later DDL was already applied in PG while we're still consuming older DML
        // records).
        // No DDL needed, no tableSchema update, no extra stream load — just process the DML
        // normally.
        if (pgAdded.isEmpty() && pgDropped.isEmpty()) {
            return super.deserialize(context, record);
        }

        // Build updatedSchemas from fresh filtered to afterSchema columns only, so that the stored
        // cache does not jump ahead to include columns not yet seen by any DML record. Those
        // unseen columns will trigger their own schema change when their first DML record arrives.
        TableEditor editor = fresh.getTable().edit();
        for (Column col : fresh.getTable().columns()) {
            if (afterSchema.field(col.name()) == null) {
                editor.removeColumn(col.name());
            }
        }
        TableChanges.TableChange filteredChange =
                new TableChanges.TableChange(TableChanges.TableChangeType.ALTER, editor.create());
        Map<TableId, TableChanges.TableChange> updatedSchemas = new HashMap<>();
        updatedSchemas.put(tableId, filteredChange);

        // Rename guard: simultaneous ADD+DROP may be a column RENAME — skip DDL to avoid data loss.
        // Users must manually RENAME the column in Doris.
        if (!pgAdded.isEmpty() && !pgDropped.isEmpty()) {
            LOG.warn(
                    "[SCHEMA-CHANGE-SKIPPED] Table: {}\n"
                            + "Potential RENAME detected (simultaneous DROP+ADD).\n"
                            + "Dropped columns: {}\n"
                            + "Added columns:   {}\n"
                            + "No DDL emitted to prevent data loss.\n"
                            + "Action required: manually RENAME column(s) in Doris,"
                            + " then data will resume.",
                    tableId.identifier(),
                    pgDropped,
                    pgAdded.stream().map(Column::name).collect(Collectors.toList()));
            List<String> dmlRecords = super.deserialize(context, record).getRecords();
            return DeserializeResult.schemaChange(
                    Collections.emptyList(), updatedSchemas, dmlRecords);
        }

        // Generate DDLs using accurate PG column types
        String db = context.get(Constants.DORIS_TARGET_DB);
        List<String> ddls = new ArrayList<>();
        Set<String> excludedCols =
                excludeColumnsCache.getOrDefault(tableId.table(), Collections.emptySet());

        for (String colName : pgDropped) {
            if (excludedCols.contains(colName)) {
                // The column is excluded from sync — skip DDL; updatedSchemas already
                // reflects the drop since it is built from afterSchema.
                LOG.info(
                        "[SCHEMA-CHANGE] Table {}: dropped column '{}' is excluded from sync,"
                                + " skipping DROP DDL",
                        tableId.identifier(),
                        colName);
                continue;
            }
            ddls.add(
                    SchemaChangeHelper.buildDropColumnSql(
                            db, resolveTargetTable(tableId.table()), colName));
        }

        for (Column col : pgAdded) {
            if (excludedCols.contains(col.name())) {
                // The column is excluded from sync — Doris table does not have it,
                // so skip the ADD DDL.
                // case: An excluded column was dropped and then re-added.
                LOG.info(
                        "[SCHEMA-CHANGE] Table {}: added column '{}' is excluded from sync,"
                                + " skipping ADD DDL",
                        tableId.identifier(),
                        col.name());
                continue;
            }
            String colType = SchemaChangeHelper.columnToDorisType(col);
            String nullable = col.isOptional() ? "" : " NOT NULL";
            // pgAdded only contains columns present in afterSchema, so field lookup is safe.
            // afterSchema.defaultValue() returns an already-deserialized Java object
            // (e.g. String "hello", Integer 42) — no PG SQL cast suffix to strip.
            // PG WAL DML records do not carry column comment metadata.
            Object defaultObj = afterSchema.field(col.name()).schema().defaultValue();
            ddls.add(
                    SchemaChangeHelper.buildAddColumnSql(
                            db,
                            resolveTargetTable(tableId.table()),
                            col.name(),
                            colType + nullable,
                            defaultObj != null ? String.valueOf(defaultObj) : null,
                            null));
        }

        List<String> dmlRecords = super.deserialize(context, record).getRecords();

        LOG.info(
                "Postgres schema change detected for table {}: added={}, dropped={}. DDLs: {}",
                tableId.identifier(),
                pgAdded.stream().map(Column::name).collect(Collectors.toList()),
                pgDropped,
                ddls);

        return DeserializeResult.schemaChange(ddls, updatedSchemas, dmlRecords);
    }

    private TableId extractTableId(SourceRecord record) {
        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String schemaName = source.getString(SCHEMA_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(null, schemaName, tableName);
    }
}
