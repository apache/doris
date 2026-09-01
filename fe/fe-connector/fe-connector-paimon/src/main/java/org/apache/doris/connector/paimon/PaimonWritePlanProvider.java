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
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorRowChangeStyle;
import org.apache.doris.connector.spi.write.ConnectorRowLevelDmlRequest;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.connector.spi.write.ConnectorWriteDistribution;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TPaimonTableSink;
import org.apache.doris.thrift.TPaimonWriteBackendType;
import org.apache.doris.thrift.TPaimonWriteMode;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;

import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/** Builds the JNI-backed Paimon sink and binds it to the active connector transaction. */
public class PaimonWritePlanProvider implements ConnectorWritePlanProvider {

    static final String ROW_KIND_COLUMN = "__DORIS_PAIMON_ROW_KIND__";

    private final PaimonCatalogProperties catalogProperties;
    private final PaimonCatalogOps catalogOps;
    private final ConnectorContext context;

    PaimonWritePlanProvider(PaimonCatalogProperties catalogProperties,
            PaimonCatalogOps catalogOps, ConnectorContext context) {
        this.catalogProperties = catalogProperties;
        this.catalogOps = catalogOps;
        this.context = context;
    }

    @Override
    public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
        PaimonTableHandle tableHandle = (PaimonTableHandle) handle.getTableHandle();
        FileStoreTable table = resolveTable(tableHandle);
        PaimonConnectorTransaction transaction = currentTransaction(session);
        PaimonWriteBinding binding = PaimonWriteBinding.create(
                tableHandle, table, buildHadoopConfig(), handle.isOverwrite(),
                handle.getStaticPartitionSpec());
        transaction.bind(binding);

        TPaimonTableSink sink = new TPaimonTableSink();
        sink.setSerializedTable(binding.getSerializedTable());
        sink.setHadoopConfig(binding.getHadoopConfig());
        sink.setColumnNames(outputColumnNames(handle));
        sink.setBackendType(TPaimonWriteBackendType.JNI);
        sink.setWriteMode(writeMode(handle));
        sink.setTransactionId(transaction.getTransactionId());
        sink.setCommitUser(transaction.getCommitUser());

        TDataSink dataSink = new TDataSink(TDataSinkType.PAIMON_TABLE_SINK);
        dataSink.setPaimonTableSink(sink);
        return new ConnectorSinkPlan(dataSink);
    }

    @Override
    public Set<WriteOperation> supportedOperations() {
        return EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE,
                WriteOperation.DELETE, WriteOperation.UPDATE, WriteOperation.MERGE);
    }

    @Override
    public ConnectorRowChangeStyle getRowChangeStyle() {
        return ConnectorRowChangeStyle.CHANGELOG;
    }

    @Override
    public List<String> getRowLevelPrimaryKeyColumns(ConnectorSession session,
            ConnectorTableHandle connectorHandle) {
        return resolveTable((PaimonTableHandle) connectorHandle).primaryKeys();
    }

    @Override
    public void validateRowLevelDml(ConnectorSession session, ConnectorTableHandle connectorHandle,
            ConnectorRowLevelDmlRequest request) {
        FileStoreTable table = resolveTable((PaimonTableHandle) connectorHandle);
        requirePrimaryKey(table, request.getOperation());
        CoreOptions options = CoreOptions.fromMap(table.options());
        if (options.rowkindField().isPresent()) {
            throw new DorisConnectorException("Paimon " + request.getOperation()
                    + " is not supported when rowkind.field is configured because it overrides "
                    + "the row-change operation");
        }
        if (table.bucketMode() == BucketMode.KEY_DYNAMIC
                && options.crossPartitionUpsertIndexTtl() != null) {
            throw new DorisConnectorException("Paimon " + request.getOperation()
                    + " is not supported when cross-partition-upsert.index-ttl is configured "
                    + "because row-change DML requires a complete key-dynamic index");
        }
        if (request.containsUpdate()) {
            validateUpdate(table, options, request.getUpdatedColumns());
        }
        if (request.containsDelete()) {
            validateDelete(table);
        }
    }

    private void requirePrimaryKey(FileStoreTable table, WriteOperation operation) {
        if (table.primaryKeys().isEmpty()) {
            throw new DorisConnectorException("Paimon " + operation + " requires a primary-key table");
        }
    }

    private void validateUpdate(FileStoreTable table, CoreOptions options, Set<String> updatedColumns) {
        if (options.changelogProducer() == CoreOptions.ChangelogProducer.INPUT) {
            throw new DorisConnectorException("Paimon UPDATE is not supported when "
                    + "changelog-producer=input because both UPDATE_BEFORE and UPDATE_AFTER "
                    + "records are required");
        }
        Set<String> primaryKeys = caseInsensitiveSet(table.primaryKeys());
        Set<String> partitionKeys = caseInsensitiveSet(table.partitionKeys());
        Set<String> sequenceFields = caseInsensitiveSet(options.sequenceField());
        for (String column : updatedColumns) {
            if (primaryKeys.contains(column)) {
                throw new DorisConnectorException(
                        "Paimon UPDATE cannot modify primary-key column '" + column + "'");
            }
            if (sequenceFields.contains(column)) {
                throw new DorisConnectorException(
                        "Paimon UPDATE cannot modify sequence-field column '" + column + "'");
            }
            if (partitionKeys.contains(column)) {
                if (options.bucket() != -1) {
                    throw new DorisConnectorException("Paimon UPDATE cannot modify partition column '"
                            + column + "' unless bucket=-1 because the old partition row cannot be "
                            + "removed without an UPDATE_BEFORE record");
                }
                if (options.ignoreDelete()) {
                    throw new DorisConnectorException("Paimon UPDATE cannot modify partition column '"
                            + column + "' when ignore-delete=true because removing the old "
                            + "partition row requires a delete record");
                }
            }
        }
        if (options.mergeEngine() != CoreOptions.MergeEngine.DEDUPLICATE) {
            throw new DorisConnectorException("Paimon UPDATE only supports merge-engine=deduplicate; "
                    + "merge-engine=" + options.mergeEngine() + " cannot preserve SQL UPDATE semantics");
        }
    }

    private void validateDelete(FileStoreTable table) {
        Options options = Options.fromMap(table.options());
        if (options.get(CoreOptions.IGNORE_DELETE)) {
            throw new DorisConnectorException("Paimon DELETE is not supported when ignore-delete=true "
                    + "because the delete record would be ignored");
        }
        CoreOptions.MergeEngine engine = options.get(CoreOptions.MERGE_ENGINE);
        switch (engine) {
            case DEDUPLICATE:
                return;
            case PARTIAL_UPDATE:
                if (options.get(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE)) {
                    return;
                }
                throw new DorisConnectorException("Paimon DELETE on merge-engine=partial-update requires "
                        + "partial-update.remove-record-on-delete=true because "
                        + "partial-update.remove-record-on-sequence-group does not guarantee "
                        + "whole-row deletion");
            case AGGREGATE:
                if (options.get(CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE)) {
                    return;
                }
                break;
            default:
                break;
        }
        throw new DorisConnectorException("Paimon DELETE does not support merge-engine=" + engine
                + " with the current table options");
    }

    private Set<String> caseInsensitiveSet(List<String> values) {
        Set<String> result = new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        result.addAll(values);
        return result;
    }

    @Override
    public boolean requiresParallelWrite() {
        return true;
    }

    @Override
    public boolean requiresFullSchemaWriteOrder() {
        return true;
    }

    @Override
    public boolean requiresMaterializeStaticPartitionValues() {
        return true;
    }

    @Override
    public ConnectorWriteDistribution getWriteDistribution(ConnectorSession session,
            ConnectorTableHandle connectorHandle) {
        FileStoreTable table = resolveTable((PaimonTableHandle) connectorHandle);
        ConnectorWriteDistribution fixedBucket = fixedBucketDistribution(table);
        if (fixedBucket != null) {
            return fixedBucket;
        }
        if (table.bucketMode() == BucketMode.BUCKET_UNAWARE) {
            return ConnectorWriteDistribution.simple(
                    ConnectorWriteDistribution.Mode.EXECUTION_ANY);
        }
        if (requiresSingleWriter(table)) {
            return ConnectorWriteDistribution.simple(ConnectorWriteDistribution.Mode.GATHER);
        }
        if (!table.primaryKeys().isEmpty()) {
            return ConnectorWriteDistribution.hash(table.primaryKeys());
        }
        return ConnectorWriteDistribution.simple(
                ConnectorWriteDistribution.Mode.EXTERNAL_UNPARTITIONED);
    }

    private ConnectorWriteDistribution fixedBucketDistribution(FileStoreTable table) {
        if (table.bucketMode() != BucketMode.HASH_FIXED) {
            return null;
        }
        TableSchema schema = table.schema();
        CoreOptions options = CoreOptions.fromMap(schema.options());
        if (options.bucketFunctionType() != CoreOptions.BucketFunctionType.DEFAULT
                || schema.numBuckets() <= 0 || schema.bucketKeys().isEmpty()) {
            return null;
        }
        Map<String, DataField> fields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (DataField field : schema.fields()) {
            fields.put(field.name(), field);
        }
        List<String> routeColumns = new ArrayList<>();
        Map<String, Integer> routeIndexes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<Integer> partitionIndexes = appendRouteFields(
                schema.partitionKeys(), fields, routeColumns, routeIndexes);
        List<Integer> bucketIndexes = appendRouteFields(
                schema.bucketKeys(), fields, routeColumns, routeIndexes);
        if (partitionIndexes == null || bucketIndexes == null || bucketIndexes.isEmpty()) {
            return null;
        }
        return ConnectorWriteDistribution.paimonFixedBucket(
                routeColumns, schema.numBuckets(), partitionIndexes, bucketIndexes);
    }

    private List<Integer> appendRouteFields(List<String> fieldNames,
            Map<String, DataField> fields, List<String> routeColumns,
            Map<String, Integer> routeIndexes) {
        List<Integer> indexes = new ArrayList<>();
        for (String fieldName : fieldNames) {
            DataField field = fields.get(fieldName);
            if (field == null || field.defaultValue() != null
                    || !supportsNativeRouting(field.type().getTypeRoot())) {
                return null;
            }
            Integer index = routeIndexes.get(fieldName);
            if (index == null) {
                index = routeColumns.size();
                routeColumns.add(field.name());
                routeIndexes.put(field.name(), index);
            }
            indexes.add(index);
        }
        return indexes;
    }

    private boolean supportsNativeRouting(DataTypeRoot type) {
        switch (type) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return true;
            default:
                return false;
        }
    }

    private boolean requiresSingleWriter(FileStoreTable table) {
        BucketMode bucketMode = table.bucketMode();
        CoreOptions options = CoreOptions.fromMap(table.options());
        if (bucketMode == BucketMode.HASH_DYNAMIC || bucketMode == BucketMode.KEY_DYNAMIC
                || (bucketMode == BucketMode.HASH_FIXED
                        && (!table.primaryKeys().isEmpty() || !options.writeOnly()))) {
            return true;
        }
        return !options.writeOnly()
                && (options.needLookup()
                        || options.changelogProducer() == CoreOptions.ChangelogProducer.FULL_COMPACTION);
    }

    private FileStoreTable resolveTable(PaimonTableHandle handle) {
        if (handle.isSystemTable()) {
            throw new DorisConnectorException("Cannot write to a Paimon system table");
        }
        Table table = handle.getPaimonTable();
        if (table == null) {
            try {
                table = context.executeAuthenticated(() -> catalogOps.getTable(
                        Identifier.create(handle.getDatabaseName(), handle.getTableName())));
            } catch (Catalog.TableNotExistException e) {
                throw new DorisConnectorException("Paimon table does not exist: "
                        + handle.getDatabaseName() + "." + handle.getTableName(), e);
            } catch (Exception e) {
                throw new DorisConnectorException("Failed to load Paimon write table: "
                        + handle.getDatabaseName() + "." + handle.getTableName(), e);
            }
        }
        if (!(table instanceof FileStoreTable)) {
            throw new DorisConnectorException("Paimon table does not support file-store writes: "
                    + handle.getDatabaseName() + "." + handle.getTableName());
        }
        return (FileStoreTable) table;
    }

    private PaimonConnectorTransaction currentTransaction(ConnectorSession session) {
        Optional<ConnectorTransaction> current = session.getCurrentTransaction();
        if (!current.isPresent()) {
            throw new DorisConnectorException(
                    "Paimon write requires an active connector transaction bound to the session");
        }
        if (!(current.get() instanceof PaimonConnectorTransaction)) {
            throw new DorisConnectorException("Active connector transaction is not a Paimon transaction");
        }
        return (PaimonConnectorTransaction) current.get();
    }

    private List<String> outputColumnNames(ConnectorWriteHandle handle) {
        List<String> names = new ArrayList<>();
        WriteOperation operation = handle.getWriteOperation();
        if (operation == WriteOperation.DELETE
                || operation == WriteOperation.UPDATE
                || operation == WriteOperation.MERGE) {
            names.add(ROW_KIND_COLUMN);
        }
        for (ConnectorColumn column : handle.getColumns()) {
            names.add(column.getName());
        }
        return names;
    }

    private TPaimonWriteMode writeMode(ConnectorWriteHandle handle) {
        WriteOperation operation = handle.getWriteOperation();
        if (operation == WriteOperation.DELETE
                || operation == WriteOperation.UPDATE
                || operation == WriteOperation.MERGE) {
            return TPaimonWriteMode.CHANGELOG;
        }
        return handle.isOverwrite() ? TPaimonWriteMode.OVERWRITE : TPaimonWriteMode.APPEND;
    }

    private Map<String, String> buildHadoopConfig() {
        Map<String, String> config = new LinkedHashMap<>();
        for (StorageProperties storage : context.getStorageContext().getStorageProperties()) {
            storage.toHadoopProperties()
                    .ifPresent(properties -> config.putAll(properties.toHadoopConfigurationMap()));
        }
        for (Map.Entry<String, String> entry : catalogProperties.getRaw().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("hadoop.") || key.startsWith("dfs.")
                    || key.startsWith("fs.") || key.startsWith("ipc.")) {
                config.put(key, entry.getValue());
            }
        }
        String warehouse = catalogProperties.getRaw().get(CatalogOptions.WAREHOUSE.key());
        if (warehouse != null) {
            URI uri = URI.create(warehouse);
            if (uri.getScheme() != null && uri.getAuthority() != null
                    && !config.containsKey("fs.defaultFS")) {
                config.put("fs.defaultFS", uri.getScheme() + "://" + uri.getAuthority());
            }
        }
        String user = config.getOrDefault("hadoop.username",
                config.getOrDefault("hadoop.user.name", "hadoop"));
        config.put("hadoop.username", user);
        config.put("hadoop.user.name", user);
        return config;
    }
}
