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

/** Plans Paimon INSERT and INSERT OVERWRITE writes for the plugin-driven sink. */
public class PaimonWritePlanProvider implements ConnectorWritePlanProvider {

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
        if (operation != WriteOperation.INSERT && operation != WriteOperation.OVERWRITE) {
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
        List<String> columnNames = new ArrayList<>(handle.getColumns().size());
        for (ConnectorColumn column : handle.getColumns()) {
            columnNames.add(column.getName());
        }
        sink.setColumnNames(columnNames);
        sink.setWriteMode(handle.isOverwrite()
                ? TPaimonWriteMode.OVERWRITE : TPaimonWriteMode.APPEND);
        sink.setTransactionId(transaction.getTransactionId());
        sink.setCommitUser(transaction.getCommitUser());

        TDataSink dataSink = new TDataSink(TDataSinkType.PAIMON_TABLE_SINK);
        dataSink.setPaimonTableSink(sink);
        return new ConnectorSinkPlan(dataSink);
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

    @Override
    public Set<WriteOperation> supportedOperations() {
        return EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE);
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
