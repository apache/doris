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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THiveColumn;
import org.apache.doris.thrift.THiveColumnType;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THiveTableSink;
import org.apache.doris.thrift.TNetworkAddress;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Produces the existing generic BE Parquet sink through the current connector write-plan SPI. */
final class DeltaWritePlanProvider implements ConnectorWritePlanProvider {
    private final DeltaCatalogAdapter adapter;
    private final ConnectorContext context;

    DeltaWritePlanProvider(DeltaCatalogAdapter adapter, ConnectorContext context) {
        this.adapter = adapter;
        this.context = context;
    }

    @Override
    public Optional<List<ConnectorColumn>> getWriteColumns(ConnectorSession session,
            ConnectorTableHandle tableHandle, Optional<String> branchName) {
        rejectBranch(branchName);
        DeltaTableHandle handle = (DeltaTableHandle) tableHandle;
        return Optional.of(DeltaConnectorMetadata.toColumns(adapter.loadSnapshot(handle).getSchema()));
    }

    @Override
    public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
        rejectBranch(handle.getBranchName());
        if (!handle.getStaticPartitionSpec().isEmpty()) {
            throw new UnsupportedOperationException("Native Delta static-partition writes are not supported");
        }
        if (handle.getWriteOperation() != WriteOperation.INSERT
                && handle.getWriteOperation() != WriteOperation.OVERWRITE) {
            throw new DorisConnectorException("Native Delta row-level DML must use its copy-on-write plan");
        }
        DeltaConnectorTransaction transaction = (DeltaConnectorTransaction) session.getCurrentTransaction()
                .orElseThrow(() -> new DorisConnectorException("Delta write requires a statement transaction"));
        DeltaWriteConfig config = transaction.beginWrite(session, handle);
        DeltaTableHandle table = (DeltaTableHandle) handle.getTableHandle();
        THiveTableSink sink = new THiveTableSink();
        sink.setConnectorFileSink(true);
        sink.setDbName(table.getDatabaseName());
        sink.setTableName(table.getTableName());
        Set<String> partitionColumns = new HashSet<>(config.getPartitionColumns());
        List<THiveColumn> columns = new ArrayList<>();
        for (ConnectorColumn column : handle.getColumns()) {
            THiveColumn thriftColumn = new THiveColumn();
            thriftColumn.setName(column.getName());
            thriftColumn.setColumnType(partitionColumns.contains(column.getName())
                    ? THiveColumnType.PARTITION_KEY : THiveColumnType.REGULAR);
            columns.add(thriftColumn);
        }
        sink.setColumns(columns);
        sink.setConnectorPartitionColumns(config.getPartitionColumns());
        sink.setFileFormat(TFileFormatType.FORMAT_PARQUET);
        sink.setCompressionType(TFileCompressType.SNAPPYBLOCK);
        THiveLocationParams location = new THiveLocationParams();
        location.setWritePath(config.getWriteLocation());
        location.setOriginalWritePath(config.getWriteLocation());
        location.setTargetPath(config.getWriteLocation());
        TFileType fileType = TFileType.valueOf(context.getStorageContext()
                .getBackendFileType(config.getWriteLocation(), config.getProperties()));
        location.setFileType(fileType);
        sink.setLocation(location);
        if (fileType == TFileType.FILE_BROKER) {
            List<TNetworkAddress> brokers = new ArrayList<>();
            context.getStorageContext().getBrokerAddresses().forEach(
                    broker -> brokers.add(new TNetworkAddress(broker.getHost(), broker.getPort())));
            sink.setBrokerAddresses(brokers);
        }
        // Delta commits append and remove actions atomically. The BE must never truncate a storage path.
        sink.setOverwrite(false);
        sink.setHadoopConfig(config.getProperties());
        TDataSink dataSink = new TDataSink(TDataSinkType.HIVE_TABLE_SINK);
        dataSink.setHiveTableSink(sink);
        return new ConnectorSinkPlan(dataSink);
    }

    @Override
    public Set<WriteOperation> supportedOperations() {
        return EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE,
                WriteOperation.DELETE, WriteOperation.UPDATE, WriteOperation.MERGE);
    }

    @Override
    public boolean supportsCopyOnWriteDml() {
        return true;
    }

    @Override
    public boolean requiresFullSchemaWriteOrder() {
        return true;
    }

    private static void rejectBranch(Optional<String> branchName) {
        if (branchName.isPresent()) {
            throw new UnsupportedOperationException("Native Delta does not support write branches");
        }
    }
}
