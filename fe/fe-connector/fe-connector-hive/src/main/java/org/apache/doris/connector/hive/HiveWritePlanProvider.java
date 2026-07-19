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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.ConnectorBrokerAddress;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THiveBucket;
import org.apache.doris.thrift.THiveColumn;
import org.apache.doris.thrift.THiveColumnType;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartition;
import org.apache.doris.thrift.THiveTableSink;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Write plan provider for hive non-ACID INSERT / INSERT OVERWRITE.
 *
 * <p>Builds the opaque {@link THiveTableSink} for a bound write and binds the write to the current
 * {@link HiveConnectorTransaction}: it loads the table under the catalog auth context, opens the write via
 * {@link HiveConnectorTransaction#beginWrite} (which re-loads the table and applies the begin-guards, incl.
 * the transactional-table reject), then assembles the sink Thrift from the loaded table. The Thrift is
 * byte-identical to the legacy fe-core {@code planner.HiveTableSink.bindDataSink} (zero BE change), so the
 * BE writer is unaffected by the migration.</p>
 *
 * <p><b>Scope.</b> INSERT / OVERWRITE only ({@code THiveTableSink}); an overwriting INSERT is promoted to the
 * OVERWRITE operation in {@link #buildWriteContext}. The write distribution capability
 * {@link #requiresPartitionHashWrite()} (hash-by-partition, no local sort) matches the legacy
 * {@code PhysicalHiveTableSink}.</p>
 *
 * <p><b>Gate-closed / dormant.</b> Hive is not in {@code SPI_READY_TYPES} until the P7.4/P7.5 cutover, so
 * nothing routes hive writes through this provider yet; {@link #planWrite} requires the executor-bound
 * connector transaction and fails loud if absent.</p>
 */
public class HiveWritePlanProvider implements ConnectorWritePlanProvider {

    // Staging-directory keys (connector-local copies of HMSExternalCatalog.HIVE_STAGING_DIR /
    // DEFAULT_STAGING_BASE_DIR — connectors must not import fe-core).
    private static final String HIVE_STAGING_DIR = "hive.staging_dir";
    private static final String DEFAULT_STAGING_BASE_DIR = "/tmp/.doris_staging";

    private final HmsClient hmsClient;
    private final Map<String, String> properties;
    private final ConnectorContext context;

    public HiveWritePlanProvider(HmsClient hmsClient, Map<String, String> properties, ConnectorContext context) {
        this.hmsClient = hmsClient;
        this.properties = properties;
        this.context = context;
    }

    @Override
    public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
        HiveTableHandle tableHandle = (HiveTableHandle) handle.getTableHandle();
        HiveConnectorTransaction transaction = currentTransaction(session);

        // Load the table under the catalog auth context; it drives both the location resolution
        // (buildWriteContext) and the sink assembly (buildSink). beginWrite re-loads it for its own
        // begin-guard — the double-load is accepted (mirrors iceberg), keeping the flow simple.
        HmsTableInfo table = loadTable(tableHandle);
        HiveWriteContext writeContext = buildWriteContext(session, tableHandle, table, handle);
        transaction.beginWrite(session, tableHandle.getDbName(), tableHandle.getTableName(), writeContext);

        THiveTableSink sink = buildSink(session, tableHandle, table, handle, writeContext);
        TDataSink dataSink = new TDataSink(TDataSinkType.HIVE_TABLE_SINK);
        dataSink.setHiveTableSink(sink);
        return new ConnectorSinkPlan(dataSink);
    }

    @Override
    public void appendExplainInfo(StringBuilder output, String prefix,
            ConnectorSession session, ConnectorWriteHandle handle) {
        // Surface the connector-specific write detail the generic plugin-driven sink line cannot (mirrors the
        // legacy HiveTableSink.getExplainString "HIVE TABLE SINK" block).
        HiveTableHandle tableHandle = (HiveTableHandle) handle.getTableHandle();
        output.append(prefix).append("  HIVE TABLE: ")
                .append(tableHandle.getDbName()).append(".").append(tableHandle.getTableName()).append("\n");
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
    public boolean requiresPartitionHashWrite() {
        return true;
    }

    /**
     * Builds the op-context threaded into {@link HiveConnectorTransaction#beginWrite}. Port of the legacy
     * {@code HiveTableSink.bindDataSink} location block: resolves the BE file type from the raw table
     * location and the write path (an in-place normalized path for an object store, or a staging temp path
     * for HDFS/local/broker). An overwriting INSERT is promoted to the OVERWRITE operation. Package-private so
     * the promotion is directly assertable.
     */
    HiveWriteContext buildWriteContext(ConnectorSession session, HiveTableHandle tableHandle,
            HmsTableInfo table, ConnectorWriteHandle handle) {
        String rawLocation = table.getLocation();
        TFileType fileType = TFileType.valueOf(context.getBackendFileType(rawLocation, Collections.emptyMap()));
        String writePath = fileType == TFileType.FILE_S3
                ? context.normalizeStorageUri(rawLocation, Collections.emptyMap())
                : createTempPath(session, rawLocation);
        WriteOperation op = handle.getWriteOperation();
        if (op == WriteOperation.INSERT && handle.isOverwrite()) {
            op = WriteOperation.OVERWRITE;
        }
        return new HiveWriteContext(op, handle.isOverwrite(), handle.getWriteContext(),
                session.getQueryId(), fileType, writePath);
    }

    // Re-impl of legacy HiveTableSink.createTempPath + LocationPath.getTempWritePath (pure hadoop Path + UUID):
    // <rawLoc>/<stagingBaseDir>/<user>/<uuid>. A relative stagingBaseDir resolves under rawLoc; an absolute one
    // keeps rawLoc's scheme/authority.
    private String createTempPath(ConnectorSession session, String rawLocation) {
        String user = session.getUser();
        String stagingBaseDir = properties.getOrDefault(HIVE_STAGING_DIR, DEFAULT_STAGING_BASE_DIR);
        Path prefix = new Path(stagingBaseDir, user);
        Path temp = new Path(new Path(rawLocation, prefix), UUID.randomUUID().toString().replace("-", ""));
        return temp.toString();
    }

    private THiveTableSink buildSink(ConnectorSession session, HiveTableHandle tableHandle,
            HmsTableInfo table, ConnectorWriteHandle handle, HiveWriteContext writeContext) {
        THiveTableSink tSink = new THiveTableSink();
        tSink.setDbName(tableHandle.getDbName());
        tSink.setTableName(tableHandle.getTableName());

        // Columns: data columns tagged REGULAR first, then partition keys tagged PARTITION_KEY (HMS order).
        tSink.setColumns(buildColumns(table));

        // Existing partitions (partitioned table only; empty otherwise).
        tSink.setPartitions(buildExistingPartitions(table));

        // Bucket info.
        THiveBucket bucketInfo = new THiveBucket();
        bucketInfo.setBucketedBy(table.getBucketCols());
        bucketInfo.setBucketCount(table.getNumBuckets());
        tSink.setBucketInfo(bucketInfo);

        // File format (ports the LZO reject + supported-set validation) + compression.
        TFileFormatType formatType = HiveSinkHelper.getTFileFormatType(table.getInputFormat());
        tSink.setFileFormat(formatType);
        setCompressType(tSink, formatType, table, session);

        // SerDe delimiters.
        tSink.setSerdeProperties(HiveSinkHelper.buildSerDeProperties(table));

        // Location: an object-store write goes in-place (write == target == normalized, original == raw); an
        // HDFS/local/broker write goes to a staging temp path (write == original == staging, target == raw).
        THiveLocationParams locationParams = new THiveLocationParams();
        String rawLocation = table.getLocation();
        if (writeContext.getFileType() == TFileType.FILE_S3) {
            locationParams.setWritePath(writeContext.getWritePath());
            locationParams.setOriginalWritePath(rawLocation);
            locationParams.setTargetPath(writeContext.getWritePath());
        } else {
            locationParams.setWritePath(writeContext.getWritePath());
            locationParams.setOriginalWritePath(writeContext.getWritePath());
            locationParams.setTargetPath(rawLocation);
        }
        locationParams.setFileType(writeContext.getFileType());
        tSink.setLocation(locationParams);

        // Broker addresses only for a broker backend (fails loud when empty, mirroring legacy).
        if (writeContext.getFileType() == TFileType.FILE_BROKER) {
            tSink.setBrokerAddresses(resolveBrokerAddresses());
        }

        // Hadoop config (BE-canonical static creds; hive has no vended overlay).
        tSink.setHadoopConfig(buildHadoopConfig());

        tSink.setOverwrite(handle.isOverwrite());
        return tSink;
    }

    private static List<THiveColumn> buildColumns(HmsTableInfo table) {
        List<THiveColumn> columns = new ArrayList<>();
        for (ConnectorColumn col : table.getColumns()) {
            THiveColumn tHiveColumn = new THiveColumn();
            tHiveColumn.setName(col.getName());
            tHiveColumn.setColumnType(THiveColumnType.REGULAR);
            columns.add(tHiveColumn);
        }
        for (ConnectorColumn col : table.getPartitionKeys()) {
            THiveColumn tHiveColumn = new THiveColumn();
            tHiveColumn.setName(col.getName());
            tHiveColumn.setColumnType(THiveColumnType.PARTITION_KEY);
            columns.add(tHiveColumn);
        }
        return columns;
    }

    // Port of legacy HiveTableSink.setPartitionValues: for a partitioned table, list live partitions and
    // convert each to a THivePartition (values + per-partition file format + in-place location). Live/uncached
    // (matches the scan path; registered deviation DV-INC4-livepart). HmsClient calls self-authenticate.
    private List<THivePartition> buildExistingPartitions(HmsTableInfo table) {
        List<THivePartition> partitions = new ArrayList<>();
        if (table.getPartitionKeys().isEmpty()) {
            return partitions;
        }
        List<String> partitionNames = hmsClient.listPartitionNames(
                table.getDbName(), table.getTableName(), -1);
        List<HmsPartitionInfo> hmsPartitions = hmsClient.getPartitions(
                table.getDbName(), table.getTableName(), partitionNames);
        for (HmsPartitionInfo partition : hmsPartitions) {
            THivePartition hivePartition = new THivePartition();
            hivePartition.setValues(partition.getValues());
            hivePartition.setFileFormat(HiveSinkHelper.getTFileFormatType(partition.getInputFormat()));
            THiveLocationParams locationParams = new THiveLocationParams();
            String location = partition.getLocation();
            // The write and target path of an existing partition are the same (BE reads it in place).
            locationParams.setWritePath(location);
            locationParams.setTargetPath(location);
            locationParams.setFileType(TFileType.valueOf(
                    context.getBackendFileType(location, Collections.emptyMap())));
            hivePartition.setLocation(locationParams);
            partitions.add(hivePartition);
        }
        return partitions;
    }

    // Port of legacy HiveTableSink.setCompressType: the compression codec is read from the table parameters by
    // format (orc.compress / parquet.compression / text.compression, the text one falling back to the session
    // default), then mapped to the BE compress type.
    private void setCompressType(THiveTableSink tSink, TFileFormatType formatType,
            HmsTableInfo table, ConnectorSession session) {
        Map<String, String> params = table.getParameters();
        String compressType;
        switch (formatType) {
            case FORMAT_ORC:
                compressType = params.get("orc.compress");
                break;
            case FORMAT_PARQUET:
                compressType = params.get("parquet.compression");
                break;
            case FORMAT_CSV_PLAIN:
            case FORMAT_TEXT:
                compressType = params.get("text.compression");
                if (Strings.isNullOrEmpty(compressType)) {
                    compressType = resolveTextCompressionDefault(session);
                }
                break;
            default:
                compressType = "plain";
                break;
        }
        tSink.setCompressionType(HiveSinkHelper.getTFileCompressType(compressType));
    }

    // Re-impl of legacy SessionVariable.hiveTextCompression() (the "uncompressed" alias maps to "plain"); the
    // value rides on the session properties threaded from the engine.
    private static String resolveTextCompressionDefault(ConnectorSession session) {
        String textCompression = session.getSessionProperties()
                .get(HiveConnectorProperties.SESSION_HIVE_TEXT_COMPRESSION);
        if (HiveConnectorProperties.TEXT_COMPRESSION_UNCOMPRESSED.equals(textCompression)) {
            return HiveConnectorProperties.TEXT_COMPRESSION_PLAIN;
        }
        return textCompression;
    }

    // Mirror iceberg buildHadoopConfig: BE-canonical static catalog creds (AWS_* for object stores, dfs/hadoop
    // for HDFS). Hive has no vended overlay.
    private Map<String, String> buildHadoopConfig() {
        Map<String, String> merged = new HashMap<>();
        if (context != null) {
            // Backend properties from the catalog's parsed storage map, the SAME source the hive READ path uses
            // (HiveScanPlanProvider.getBackendStorageProperties) and legacy HiveTableSink emitted. It carries the
            // resolved hadoop./dfs./fs./juicefs.* passthrough, so it is the sole source of fs.<scheme>.impl for
            // schemes whose fe-filesystem plugin has no typed bind() (jfs, oss-hdfs) — without it a jfs write ships
            // the BE writer a config with no fs.jfs.impl and libhdfs fails "No FileSystem for scheme jfs". Emitted
            // first so the typed getStorageProperties() overlay still wins wherever it produces a value (object
            // stores / HDFS), keeping their behavior byte-identical.
            merged.putAll(context.getBackendStorageProperties());
            for (StorageProperties sp : context.getStorageProperties()) {
                sp.toBackendProperties().ifPresent(b -> merged.putAll(b.toMap()));
            }
        }
        return merged;
    }

    // Resolve the broker backend addresses through the neutral SPI; fail loud when none is resolved (the same
    // message legacy BaseExternalTableDataSink.getBrokerAddresses threw), so a broker write never ships BE an
    // empty broker list.
    private List<TNetworkAddress> resolveBrokerAddresses() {
        List<ConnectorBrokerAddress> addresses = context.getBrokerAddresses();
        if (addresses.isEmpty()) {
            throw new DorisConnectorException("No alive broker.");
        }
        List<TNetworkAddress> result = new ArrayList<>(addresses.size());
        for (ConnectorBrokerAddress address : addresses) {
            result.add(new TNetworkAddress(address.getHost(), address.getPort()));
        }
        return result;
    }

    private HmsTableInfo loadTable(HiveTableHandle tableHandle) {
        try {
            return context.executeAuthenticated(
                    () -> hmsClient.getTable(tableHandle.getDbName(), tableHandle.getTableName()));
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to load hive table "
                    + tableHandle.getDbName() + "." + tableHandle.getTableName() + ": " + e.getMessage(), e);
        }
    }

    private HiveConnectorTransaction currentTransaction(ConnectorSession session) {
        Optional<ConnectorTransaction> transaction = session.getCurrentTransaction();
        if (!transaction.isPresent()) {
            throw new DorisConnectorException(
                    "Hive write requires an active connector transaction bound to the session; none is "
                            + "present. The executor must open it via beginTransaction and bind it to the "
                            + "session (wired at the hive cutover).");
        }
        return (HiveConnectorTransaction) transaction.get();
    }
}
