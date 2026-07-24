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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TMaxComputeTableSink;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Write plan provider for MaxCompute (ODPS).
 *
 * <p>Builds the opaque {@link TMaxComputeTableSink} for a bound DML write: it
 * creates the ODPS Storage API write session, binds it to the current connector
 * transaction (so commit / block allocation can act on it), and stamps the
 * engine transaction id and write session id into the sink.</p>
 *
 * <p>Ported from the legacy fe-core write path — {@code MCTransaction.beginInsert()}
 * (write-session creation) and {@code MaxComputeTableSink.bindDataSink()} /
 * {@code setWriteContext()} (sink field population). The legacy split between
 * {@code finalizeSink} (sink fields) and {@code MCInsertExecutor.beforeExec}
 * (runtime {@code txn_id} / {@code write_session_id} injection) collapses into
 * this single {@code planWrite} call, which runs at {@code finalizeSink} time when
 * the engine transaction id already exists and the write session can be created
 * in place (see P4-T04 design, OQ-2 / Approach A).</p>
 *
 * <p><b>Runtime block-id allocation</b> ({@code block_id_start} / {@code block_id_count})
 * is intentionally <i>not</i> stamped here: BE allocates it at run time through the
 * engine transaction ({@link MaxComputeConnectorTransaction#allocateWriteBlockRange})
 * keyed by {@code txn_id}.</p>
 *
 * <p><b>Gate-closed / dormant.</b> Nothing routes plugin-driven MaxCompute writes
 * through this provider until the {@code max_compute} cutover. In particular
 * {@link #planWrite} requires the session to carry the connector transaction
 * (bound by the executor wiring added at cutover); it fails loud if absent.</p>
 */
public class MaxComputeWritePlanProvider implements ConnectorWritePlanProvider {

    private static final Logger LOG = LogManager.getLogger(MaxComputeWritePlanProvider.class);

    private final MaxComputeDorisConnector connector;

    public MaxComputeWritePlanProvider(MaxComputeDorisConnector connector) {
        this.connector = connector;
    }

    @Override
    public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
        MaxComputeTableHandle mcHandle = (MaxComputeTableHandle) handle.getTableHandle();
        Table odpsTable = mcHandle.getOdpsTable();
        // Reject external tables / logical views before opening a write session (mirrors legacy
        // MCTransaction.beginInsert): the ODPS Storage API cannot write to them.
        mcHandle.checkOperationSupported("Writing");
        TableIdentifier tableId = mcHandle.getTableIdentifier();

        boolean isOverwrite = handle.isOverwrite();
        // Static partition spec carried as a col -> val map in the write context (D-5).
        Map<String, String> staticPartitionSpec = handle.getWriteContext();
        boolean isStaticPartition = staticPartitionSpec != null && !staticPartitionSpec.isEmpty();

        // Partition column names, taken from the ODPS table (DV-012: legacy reads
        // the fe-core Doris columns; the values — partition column names — are identical).
        List<String> partitionColumnNames = odpsTable.getSchema().getPartitionColumns()
                .stream().map(Column::getName).collect(Collectors.toList());
        boolean isDynamicPartition = !partitionColumnNames.isEmpty();

        EnvironmentSettings settings = connector.getSettings();

        String writeSessionId = createWriteSession(
                tableId, settings, partitionColumnNames, staticPartitionSpec,
                isStaticPartition, isDynamicPartition, isOverwrite, mcHandle.getTableName());

        // Bind the write session to the current connector transaction (T03 slot),
        // so block allocation and commit can act on it.
        MaxComputeConnectorTransaction transaction = currentTransaction(session);
        transaction.setWriteSession(writeSessionId, tableId, settings);

        TMaxComputeTableSink tSink = new TMaxComputeTableSink();
        tSink.setProperties(connector.getProperties());
        tSink.setEndpoint(connector.getEndpoint());
        tSink.setProject(connector.getDefaultProject());
        tSink.setTableName(mcHandle.getTableName());
        tSink.setQuota(connector.getQuota());
        tSink.setConnectTimeout(getConnectTimeout());
        tSink.setReadTimeout(getReadTimeout());
        tSink.setRetryCount(getRetryTimes());
        if (!partitionColumnNames.isEmpty()) {
            tSink.setPartitionColumns(partitionColumnNames);
        }
        if (isStaticPartition) {
            tSink.setStaticPartitionSpec(staticPartitionSpec);
        }
        tSink.setWriteSessionId(writeSessionId);
        tSink.setTxnId(transaction.getTransactionId());
        // block_id_start / block_id_count are left unset: BE allocates them at run
        // time via the engine transaction (keyed by txn_id).

        TDataSink dataSink = new TDataSink(TDataSinkType.MAXCOMPUTE_TABLE_SINK);
        dataSink.setMaxComputeTableSink(tSink);
        return new ConnectorSinkPlan(dataSink);
    }

    @Override
    public Set<WriteOperation> supportedOperations() {
        return EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE);
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
    public boolean requiresPartitionLocalSort() {
        return true;
    }

    /**
     * Creates the ODPS Storage API batch write session and returns its id. Ports
     * {@code MCTransaction.beginInsert()}: a static partition pins the target
     * partition, otherwise a partitioned table uses dynamic partitioning; overwrite
     * is applied when requested. Note the write path uses MILLI/MILLI Arrow units
     * (the scan path differs).
     */
    private String createWriteSession(TableIdentifier tableId, EnvironmentSettings settings,
            List<String> partitionColumnNames, Map<String, String> staticPartitionSpec,
            boolean isStaticPartition, boolean isDynamicPartition, boolean isOverwrite,
            String tableName) {
        try {
            TableWriteSessionBuilder builder = new TableWriteSessionBuilder()
                    .identifier(tableId)
                    .withSettings(settings)
                    .withMaxFieldSize(getMaxFieldSize())
                    .withArrowOptions(ArrowOptions.newBuilder()
                            .withDatetimeUnit(TimestampUnit.MILLI)
                            .withTimestampUnit(TimestampUnit.MILLI)
                            .build());

            if (isStaticPartition) {
                builder.partition(new PartitionSpec(
                        buildStaticPartitionSpecString(partitionColumnNames, staticPartitionSpec)));
            } else if (isDynamicPartition) {
                builder.withDynamicPartitionOptions(DynamicPartitionOptions.createDefault());
            }

            if (isOverwrite) {
                builder.overwrite(true);
            }

            TableBatchWriteSession writeSession = builder.buildBatchWriteSession();
            String writeSessionId = writeSession.getId();
            LOG.info("Created MaxCompute write session {} for table {} (overwrite={}, "
                    + "staticPartition={}, dynamicPartition={})",
                    writeSessionId, tableName, isOverwrite, isStaticPartition, isDynamicPartition);
            return writeSessionId;
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to create MaxCompute write session for table " + tableName
                            + ": " + e.getMessage(), e);
        }
    }

    /**
     * Joins the static partition spec into {@code "col=val,col=val"} following the
     * table's partition column order (mirrors {@code MCTransaction.beginInsert}).
     */
    private String buildStaticPartitionSpecString(List<String> partitionColumnNames,
            Map<String, String> staticPartitionSpec) {
        return partitionColumnNames.stream()
                .filter(staticPartitionSpec::containsKey)
                .map(name -> name + "=" + staticPartitionSpec.get(name))
                .collect(Collectors.joining(","));
    }

    private MaxComputeConnectorTransaction currentTransaction(ConnectorSession session) {
        Optional<ConnectorTransaction> transaction = session.getCurrentTransaction();
        if (!transaction.isPresent()) {
            throw new DorisConnectorException(
                    "MaxCompute write requires an active connector transaction bound to the session; "
                            + "none is present. The executor must open it via beginTransaction and bind "
                            + "it to the session (wired at the max_compute cutover).");
        }
        return (MaxComputeConnectorTransaction) transaction.get();
    }

    private int getConnectTimeout() {
        return Integer.parseInt(connector.getProperties().getOrDefault(
                MCConnectorProperties.CONNECT_TIMEOUT,
                MCConnectorProperties.DEFAULT_CONNECT_TIMEOUT));
    }

    private int getReadTimeout() {
        return Integer.parseInt(connector.getProperties().getOrDefault(
                MCConnectorProperties.READ_TIMEOUT,
                MCConnectorProperties.DEFAULT_READ_TIMEOUT));
    }

    private int getRetryTimes() {
        return Integer.parseInt(connector.getProperties().getOrDefault(
                MCConnectorProperties.RETRY_COUNT,
                MCConnectorProperties.DEFAULT_RETRY_COUNT));
    }

    private long getMaxFieldSize() {
        return Long.parseLong(connector.getProperties().getOrDefault(
                MCConnectorProperties.MAX_FIELD_SIZE,
                MCConnectorProperties.DEFAULT_MAX_FIELD_SIZE));
    }
}
