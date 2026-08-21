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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TPaimonTableSink;
import org.apache.doris.thrift.TPaimonWriteMode;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/** Plans Paimon INSERT, INSERT OVERWRITE and row-level DELETE writes for the plugin-driven sink. */
public class PaimonWritePlanProvider implements ConnectorWritePlanProvider {

    /**
     * Synthetic row-id column name. Must equal the fe-core constant
     * {@code PaimonRowLevelDmlColumns.ROWID_COL} — the two halves of the same contract, kept as separate
     * literals because a connector must not be a compile-time dependency of fe-core.
     */
    private static final String DORIS_PAIMON_ROWID_COL = "__DORIS_PAIMON_ROWID_COL__";

    /**
     * The row-id STRUCT a row-level DML scan projects: which data file a row lives in, and its ordinal
     * within that file — exactly the pair a Paimon deletion vector indexes. Declared for every table (see
     * {@link #getSyntheticWriteColumns}), so it is a shared immutable instance.
     */
    private static final List<ConnectorColumn> SYNTHETIC_WRITE_COLUMNS =
            Collections.singletonList(buildRowIdColumn());

    private static ConnectorColumn buildRowIdColumn() {
        ConnectorType rowIdStruct = ConnectorType.structOf(
                Arrays.asList("file_path", "row_position"),
                Arrays.asList(ConnectorType.of("STRING"), ConnectorType.of("BIGINT")));
        // Nullable: only unaware-bucket append scans read raw single-file tasks and materialize a
        // real (file, ordinal) locator. A primary-key table's merge-on-read task merges several
        // files, so its scan materializes NULL here — and its row-level DML never consumes the
        // locator anyway (deletes and upserts address rows BY KEY).
        return new ConnectorColumn(DORIS_PAIMON_ROWID_COL, rowIdStruct,
                "Paimon row position metadata", true, null, false).invisible();
    }

    private final PaimonCatalogProperties catalogProperties;
    private final PaimonCatalogOps catalogOps;
    private final ConnectorContext context;
    private final PaimonTypeMapping.Options typeMappingOptions;

    public PaimonWritePlanProvider(PaimonCatalogProperties catalogProperties,
            PaimonCatalogOps catalogOps, ConnectorContext context) {
        this.catalogProperties = catalogProperties;
        this.catalogOps = catalogOps;
        this.context = context;
        this.typeMappingOptions = new PaimonTypeMapping.Options(
                catalogProperties.isEnableMappingVarbinary(),
                catalogProperties.isEnableMappingTimestampTz());
    }

    @Override
    public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
        WriteOperation operation = handle.isOverwrite()
                ? WriteOperation.OVERWRITE : handle.getWriteOperation();
        // DELETE and MERGE are plannable end to end: a keyed RowKind.DELETE record / deletion-vector
        // marks for DELETE, and RowKind dispatch on the operation-tagged stream for MERGE (UPDATE
        // arrives as MERGE — the translator threads WriteOperation.MERGE for both). Anything else
        // row-level (e.g. REWRITE) stays rejected; validateRowLevelDmlMode gates per table shape at
        // analysis time and this is the execution-time backstop.
        if (operation != WriteOperation.INSERT && operation != WriteOperation.OVERWRITE
                && operation != WriteOperation.DELETE && operation != WriteOperation.MERGE) {
            throw new DorisConnectorException("Unsupported Paimon write operation: " + operation);
        }

        PaimonTableHandle tableHandle = (PaimonTableHandle) handle.getTableHandle();
        Optional<String> branchName = handle.getBranchName();
        FileStoreTable resolved = resolveWriteTable(session, tableHandle, branchName);
        List<ConnectorColumn> currentColumns = mapWriteColumns(resolved);
        validateBoundColumns(handle.getBoundTargetColumns(), currentColumns);
        validateWriteColumns(handle.getColumns(), currentColumns);

        Map<String, String> staticPartition = canonicalStaticPartition(
                resolved, handle.getStaticPartitionSpec());
        FileStoreTable writeTable = PaimonWriteBinding.configureTableForWrite(
                resolved, handle.isOverwrite(), staticPartition);
        Identifier identifier = identifier(tableHandle, branchName);
        PaimonWriteBinding binding = new PaimonWriteBinding(
                identifier, writeTable, buildHadoopConfig(), handle.isOverwrite(),
                staticPartition, writeMetadataIdentity(writeTable));

        PaimonConnectorTransaction transaction = currentTransaction(session);
        transaction.bind(binding);

        TPaimonTableSink sink = new TPaimonTableSink();
        sink.setSerializedTable(binding.getSerializedTable());
        sink.setHadoopConfig(binding.getHadoopConfig());
        // BE overwrites the projected block's column names BY POSITION from this list, so its size and
        // order must equal the sink's output-expr order. For INSERT/OVERWRITE that is normally the
        // handle's column order — EXCEPT a statically partitioned write: BindSink additionally
        // materializes the PARTITION-clause literal into the row (requiresMaterializeStaticPartitionValues),
        // so the actual projected block carries the FULL bound schema (handle.getBoundTargetColumns()),
        // not the INSERT column list (handle.getColumns(), which is deliberately the partition-column-
        // excluded subset — see ConnectorWriteHandle#getColumns). Using the subset here under-counts the
        // static-partition write and trips BE's column-count DORIS_CHECK. DELETE carries [data
        // columns..., locator] (PaimonRowLevelDeletePlanBuilder): the keyed writer re-tags the data row
        // RowKind.DELETE and skips the locator, while the deletion-vector writer reads the locator and
        // ignores the data columns. Same trailing position here so BE's by-position rename matches the
        // plan's output order. A MERGE stream is [operation, row locator, data columns...] — the shape
        // the merge plan builders synthesize — so the two synthetic leaders are placed first and the
        // locator is removed from wherever the handle carried it. Static partitions are not reachable
        // for DELETE/MERGE (Doris has no PARTITION(...) clause on those statements), so only the
        // INSERT/OVERWRITE arm needs the bound-schema switch.
        List<ConnectorColumn> insertColumns =
                staticPartition.isEmpty() ? handle.getColumns() : handle.getBoundTargetColumns();
        List<String> columnNames = new ArrayList<>(insertColumns.size() + 1);
        if (operation == WriteOperation.MERGE) {
            columnNames.add("operation");
            columnNames.add(DORIS_PAIMON_ROWID_COL);
            for (ConnectorColumn column : handle.getColumns()) {
                if (!DORIS_PAIMON_ROWID_COL.equalsIgnoreCase(column.getName())) {
                    columnNames.add(column.getName());
                }
            }
        } else if (operation == WriteOperation.DELETE) {
            for (ConnectorColumn column : handle.getColumns()) {
                if (!DORIS_PAIMON_ROWID_COL.equalsIgnoreCase(column.getName())) {
                    columnNames.add(column.getName());
                }
            }
            columnNames.add(DORIS_PAIMON_ROWID_COL);
        } else {
            for (ConnectorColumn column : insertColumns) {
                columnNames.add(column.getName());
            }
        }
        sink.setColumnNames(columnNames);
        sink.setWriteMode(resolveWriteMode(handle));
        sink.setTransactionId(transaction.getTransactionId());
        sink.setCommitUser(transaction.getCommitUser());

        TDataSink dataSink = new TDataSink(TDataSinkType.PAIMON_TABLE_SINK);
        dataSink.setPaimonTableSink(sink);
        return new ConnectorSinkPlan(dataSink);
    }

    /**
     * Maps the statement's write operation onto the sink's write mode.
     *
     * <p>DELETE is checked BEFORE the overwrite flag: the two are mutually exclusive, and reading the flag
     * first would silently downgrade a delete to an append on any handle that leaves it false.
     *
     * <p>MERGE (which also carries SQL UPDATE — the translator threads MERGE for both) maps to its own
     * mode: the stream is operation-tagged and the writer dispatches RowKind per row. Stamping every row
     * as a delete (the DELETE mode) would drop the new values.
     */
    private static TPaimonWriteMode resolveWriteMode(ConnectorWriteHandle handle) {
        if (handle.getWriteOperation() == WriteOperation.DELETE) {
            return TPaimonWriteMode.DELETE;
        }
        if (handle.getWriteOperation() == WriteOperation.MERGE) {
            return TPaimonWriteMode.MERGE;
        }
        return handle.isOverwrite() ? TPaimonWriteMode.OVERWRITE : TPaimonWriteMode.APPEND;
    }

    @Override
    public Optional<List<ConnectorColumn>> getWriteColumns(ConnectorSession session,
            ConnectorTableHandle tableHandle, Optional<String> branchName) {
        return Optional.of(mapWriteColumns(resolveWriteTable(
                session, (PaimonTableHandle) tableHandle, branchName)));
    }

    @Override
    public String getWriteMetadataIdentity(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        return writeMetadataIdentity(resolveWriteTable(
                session, (PaimonTableHandle) tableHandle, Optional.empty()));
    }

    @Override
    public void appendExplainInfo(StringBuilder output, String prefix,
            ConnectorSession session, ConnectorWriteHandle handle) {
        PaimonTableHandle table = (PaimonTableHandle) handle.getTableHandle();
        output.append(prefix).append("  PAIMON TABLE: ")
                .append(table.getDatabaseName()).append(".").append(table.getTableName())
                .append("\n");
    }

    /**
     * Declares the row-level trio on top of the append/overwrite pair. The declaration is what admits the
     * table into {@code RowLevelDmlRegistry} (the registry probes this set, not the table type), which is
     * what routes the statement to the connector's OWN error messages instead of the native "olapTable"
     * rejection.
     *
     * <p>Declared does not mean admitted: {@link PaimonConnectorMetadata#validateRowLevelDmlMode} gates all
     * three ops per table shape — a primary-key table carries every one, an unaware-bucket append-only
     * table with deletion vectors carries every one (the operation-tagged merge stream drives a combined
     * deletion-vector-plus-append write), and every other append-only shape is rejected there.
     * {@link #planWrite}'s operation check is the execution-time backstop for the same contract.</p>
     */
    @Override
    public Set<WriteOperation> supportedOperations() {
        return EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE,
                WriteOperation.DELETE, WriteOperation.UPDATE, WriteOperation.MERGE);
    }

    /**
     * Declares the row-id locator column for EVERY paimon table.
     *
     * <p>Only an append-only row-level write actually consumes it — a DELETE, and the removal half of an
     * UPDATE/MERGE, need the physical address (data file + ordinal) to mark in the deletion vector. A
     * primary-key write addresses rows BY KEY and the writer ignores the locator — but the fe-core
     * row-level plan builders inject the locator unconditionally (the
     * plan shape is shared with iceberg, where every table has one), so a table that declares none
     * fails at bind time with an unresolved slot. Declaring it uniformly costs the PK scan one extra
     * projected STRUCT and buys a single plan shape.</p>
     */
    @Override
    public List<ConnectorColumn> getSyntheticWriteColumns(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        return SYNTHETIC_WRITE_COLUMNS;
    }

    @Override
    public boolean supportsWriteBranch() {
        return true;
    }

    @Override
    public boolean requiresMaterializeStaticPartitionValues() {
        // Paimon's SDK derives the target partition from the complete row.
        return true;
    }

    @Override
    public boolean requiresParallelWrite() {
        // Dynamic-bucket assigners must observe one ordered key stream. Keep every Paimon table on
        // the GATHER path until Doris can shuffle by Paimon's effective bucket-assignment key.
        return false;
    }

    private FileStoreTable resolveWriteTable(ConnectorSession session,
            PaimonTableHandle handle, Optional<String> branchName) {
        String branch = branchName.orElse("");
        String key = "paimon:write:" + session.getCatalogId() + ":"
                + handle.getDatabaseName() + ":" + handle.getTableName() + ":" + branch;
        return session.getStatementScope().computeIfAbsent(key, () -> {
            try {
                Table table = executeAuthenticated(
                        () -> catalogOps.getTable(identifier(handle, branchName)));
                if (!(table instanceof FileStoreTable)) {
                    throw new DorisConnectorException(
                            "Paimon write requires a file store table: " + handle);
                }
                // A caching catalog can retain a Table object after schema evolution. Pin the latest
                // schema once for the statement, then use this same object through sink binding.
                return ((FileStoreTable) table).copyWithLatestSchema();
            } catch (DorisConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new DorisConnectorException(
                        "Failed to load Paimon write target " + handle + ": " + e.getMessage(), e);
            }
        });
    }

    private List<ConnectorColumn> mapWriteColumns(FileStoreTable table) {
        Set<String> primaryKeys = new HashSet<>();
        for (String key : table.primaryKeys()) {
            primaryKeys.add(key.toLowerCase(Locale.ROOT));
        }
        List<ConnectorColumn> columns = new ArrayList<>();
        for (DataField field : table.rowType().getFields()) {
            ConnectorType type = PaimonTypeMapping.toConnectorType(field.type(), typeMappingOptions);
            ConnectorColumn column = new ConnectorColumn(
                    field.name(), type, field.description(), field.type().isNullable(), null,
                    primaryKeys.contains(field.name().toLowerCase(Locale.ROOT)))
                    .withUniqueId(field.id());
            if (field.defaultValue() != null) {
                // Paimon stores column defaults as SQL literal text and validates that the
                // literal can be cast from STRING to the field type. Preserve that literal so
                // Doris expands omitted columns before the sink instead of materializing NULL.
                column = column.withDefaultValueSql(field.defaultValue());
            }
            if (field.type().getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                column = column.withTimeZone();
            }
            columns.add(column);
        }
        return Collections.unmodifiableList(columns);
    }

    private static void validateBoundColumns(List<ConnectorColumn> bound,
            List<ConnectorColumn> current) {
        if (!bound.isEmpty() && !bound.equals(current)) {
            throw new DorisConnectorException(
                    "Paimon write metadata changed after the write was bound; retry the statement");
        }
    }

    private static void validateWriteColumns(List<ConnectorColumn> writeColumns,
            List<ConnectorColumn> currentColumns) {
        Map<String, ConnectorColumn> current = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ConnectorColumn column : currentColumns) {
            current.put(column.getName(), column);
        }
        Set<String> seen = new HashSet<>();
        for (ConnectorColumn column : writeColumns) {
            ConnectorColumn target = current.get(column.getName());
            if (target == null || !target.getType().equals(column.getType())) {
                throw new DorisConnectorException(
                        "Paimon write column no longer matches the target schema: " + column.getName());
            }
            if (!seen.add(column.getName().toLowerCase(Locale.ROOT))) {
                throw new DorisConnectorException("Duplicate Paimon write column: " + column.getName());
            }
        }
    }

    static String writeMetadataIdentity(FileStoreTable table) {
        String uuid = table.catalogEnvironment() == null
                ? null : table.catalogEnvironment().uuid();
        return String.valueOf(uuid) + "|" + table.location() + "|" + table.schema().id();
    }

    private static Identifier identifier(PaimonTableHandle handle,
            Optional<String> branchName) {
        return branchName.isPresent()
                ? new Identifier(handle.getDatabaseName(), handle.getTableName(), branchName.get())
                : Identifier.create(handle.getDatabaseName(), handle.getTableName());
    }

    private static Map<String, String> canonicalStaticPartition(FileStoreTable table,
            Map<String, String> requested) {
        if (requested == null || requested.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> canonical = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String partitionKey : table.partitionKeys()) {
            canonical.put(partitionKey, partitionKey);
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : requested.entrySet()) {
            String name = canonical.get(entry.getKey());
            if (name == null) {
                throw new DorisConnectorException("Column '" + entry.getKey()
                        + "' is not a partition column of Paimon table");
            }
            result.put(name, entry.getValue());
        }
        return result;
    }

    private Map<String, String> buildHadoopConfig() {
        Map<String, String> storageConfig = new HashMap<>();
        if (context != null) {
            for (StorageProperties properties : storage().getStorageProperties()) {
                properties.toHadoopProperties().ifPresent(hadoop ->
                        storageConfig.putAll(hadoop.toHadoopConfigurationMap()));
            }
        }
        Configuration configuration = PaimonCatalogFactory.buildHadoopConfiguration(
                catalogProperties.getRaw(), storageConfig);
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : configuration) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    private PaimonConnectorTransaction currentTransaction(ConnectorSession session) {
        Optional<ConnectorTransaction> transaction = session.getCurrentTransaction();
        if (!transaction.isPresent() || !(transaction.get() instanceof PaimonConnectorTransaction)) {
            throw new DorisConnectorException(
                    "Paimon write requires an active Paimon connector transaction");
        }
        return (PaimonConnectorTransaction) transaction.get();
    }

    private <T> T executeAuthenticated(java.util.concurrent.Callable<T> callable) throws Exception {
        return context == null ? callable.call() : context.executeAuthenticated(callable);
    }

    private ConnectorStorageContext storage() {
        return context.getStorageContext();
    }
}
