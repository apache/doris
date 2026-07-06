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
// This file ports the write/commit lifecycle of the legacy fe-core
// org.apache.doris.datasource.hive.HMSTransaction (itself derived from Trino's
// SemiTransactionalHiveMetastore) onto the connector SPI, breaking the fe-core couplings per the
// P7.3 design decisions (D1-D12). Control flow is preserved; only the fe-core seams are replaced.

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsCommonStatistics;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsPartitionStatistics;
import org.apache.doris.connector.hms.HmsPartitionWithStatistics;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.hms.HmsTypeMapping;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemUtil;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.filesystem.spi.ObjFileSystem;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TS3MPUPendingUpload;
import org.apache.doris.thrift.TUpdateMode;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.Iterables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Hive non-ACID write transaction ported from the legacy fe-core {@code HMSTransaction} onto the
 * connector {@link ConnectorTransaction} SPI (design P7.3). It accumulates the BE-reported commit
 * fragments ({@link THivePartitionUpdate}, fed via {@link #addCommitData}), classifies each partition
 * update into a table/partition action (APPEND / NEW / OVERWRITE, with a NEW&rarr;APPEND downgrade on an
 * HMS existence probe), and drives the {@link HmsCommitter}: staging&rarr;target renames, object-store
 * multipart-upload complete/abort, {@code addPartitions}, and statistics updates.
 *
 * <p>fe-core couplings broken per design: query profiling dropped (D4); metastore access via the plugin
 * {@link HmsClient} SPI instead of {@code HiveMetadataOps} (D10); object-store multipart uploads via a
 * plugin-side {@code fe-filesystem-spi} {@link ObjFileSystem} built from {@code context.getStorageProperties()}
 * instead of {@code SpiSwitchingFileSystem} (D6); plugin-owned async pool threads each auth-wrapped
 * (D5); full-ACID writes hard-rejected at begin (D7); {@code rollback()} deletes staging + aborts MPUs (D9).
 *
 * <p>Gate-closed / dormant: hive is not in {@code SPI_READY_TYPES}, so nothing routes plugin-driven hive
 * writes through this class until the P7.4/P7.5 cutover. {@link #beginWrite} is wired by INC-4's
 * {@code HiveWritePlanProvider.planWrite}.
 */
public class HiveConnectorTransaction implements ConnectorTransaction {

    private static final Logger LOG = LogManager.getLogger(HiveConnectorTransaction.class);

    private final long transactionId;
    private final HmsClient hmsClient;
    private final ConnectorContext context;

    // Plugin-owned async pool for staging renames + MPU complete/abort (the legacy class had this injected
    // by fe-core). Shut down in close(). authWrappingExecutor wraps each submitted task in the catalog auth
    // context (D5: plugin-owned pool threads do not inherit the caller pin).
    private final ExecutorService fileSystemExecutor;
    private final Executor authWrappingExecutor;

    // Built lazily from the catalog's object-store StorageProperties (D6). Held so staging renames/deletes
    // and MPU complete/abort share one FileSystem; closed in close().
    private volatile FileSystem fs;

    private NameMapping nameMapping;
    private volatile HmsTableInfo hmsTableInfo;
    private String queryId;
    private boolean isOverwrite;
    private TFileType fileType;
    private Optional<String> stagingDirectory = Optional.empty();
    private boolean isMockedPartitionUpdate = false;

    private List<THivePartitionUpdate> hivePartitionUpdates = new ArrayList<>();
    private final Map<NameMapping, Action<TableAndMore>> tableActions = new HashMap<>();
    private final Map<NameMapping, Map<List<String>, Action<PartitionAndMore>>> partitionActions = new HashMap<>();
    private final Set<UncompletedMpuPendingUpload> uncompletedMpuPendingUploads = new HashSet<>();

    private HmsCommitter hmsCommitter;

    public HiveConnectorTransaction(long transactionId, HmsClient hmsClient, ConnectorContext context) {
        this.transactionId = transactionId;
        this.hmsClient = hmsClient;
        this.context = context;
        this.fileSystemExecutor = Executors.newFixedThreadPool(16, namedDaemonThreadFactory("hive-write-fs-%d"));
        this.authWrappingExecutor = command -> fileSystemExecutor.execute(() -> {
            try {
                context.executeAuthenticated(() -> {
                    command.run();
                    return null;
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    // ─────────────────────────────── SPI surface ───────────────────────────────

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public String profileLabel() {
        // D2: maps to the existing fe-core TransactionType.HMS; any other string would fall back to UNKNOWN.
        return "HMS";
    }

    @Override
    public void addCommitData(byte[] commitFragment) {
        THivePartitionUpdate pu = new THivePartitionUpdate();
        try {
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(pu, commitFragment);
        } catch (TException e) {
            throw new DorisConnectorException("failed to deserialize Hive partition update", e);
        }
        synchronized (this) {
            hivePartitionUpdates.add(pu);
        }
    }

    @Override
    public long getUpdateCnt() {
        // D3: preserve legacy behavior — the affected-row count is the sum of the fragment row counts.
        return hivePartitionUpdates.stream().mapToLong(THivePartitionUpdate::getRowCount).sum();
    }

    @Override
    public void close() {
        shutdownExecutorService(fileSystemExecutor);
        FileSystem local = fs;
        if (local != null) {
            try {
                local.close();
            } catch (Exception e) {
                LOG.warn("Failed to close filesystem for hive write transaction {}: {}",
                        transactionId, e.getMessage());
            }
        }
    }

    /**
     * Opens the write for {@code db.tableName} (analogue of iceberg {@code beginWrite}; folds the legacy
     * {@code beginInsertTable} plus the table load and the full-ACID-write guard). Called by INC-4's
     * {@code HiveWritePlanProvider.planWrite}. The table is loaded under the catalog auth context (D5) — the
     * only pre-commit point that has the table — so the full-ACID reject (D7) can run here.
     */
    public void beginWrite(ConnectorSession session, String db, String tableName, HiveWriteContext ctx) {
        this.queryId = ctx.getQueryId();
        this.isOverwrite = ctx.isOverwrite();
        this.fileType = ctx.getFileType();
        this.stagingDirectory = (fileType == TFileType.FILE_S3)
                ? Optional.empty() : Optional.of(ctx.getWritePath());
        this.nameMapping = new NameMapping(context.getCatalogId(), db, tableName, db, tableName);
        try {
            context.executeAuthenticated(() -> {
                HmsTableInfo table = hmsClient.getTable(db, tableName);
                rejectTransactionalWrite(table.getParameters());
                this.hmsTableInfo = table;
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to begin write for hive table " + tableName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void commit() {
        // The classification (finishInsertTable) ran from the executor in the legacy class; the unified SPI
        // exposes only commit(), so it runs here (before the committer) to populate the action maps. If it
        // throws, the committer was never created and the engine's subsequent rollback() cleans up.
        finishInsertTable(nameMapping);
        hmsCommitter = new HmsCommitter();
        try {
            for (Map.Entry<NameMapping, Action<TableAndMore>> entry : tableActions.entrySet()) {
                NameMapping nm = entry.getKey();
                Action<TableAndMore> action = entry.getValue();
                switch (action.getType()) {
                    case INSERT_EXISTING:
                        hmsCommitter.prepareInsertExistingTable(nm, action.getData());
                        break;
                    case ALTER:
                        hmsCommitter.prepareAlterTable(nm, action.getData());
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported table action type: " + action.getType());
                }
            }

            for (Map.Entry<NameMapping, Map<List<String>, Action<PartitionAndMore>>> tableEntry
                    : partitionActions.entrySet()) {
                NameMapping nm = tableEntry.getKey();
                for (Map.Entry<List<String>, Action<PartitionAndMore>> partitionEntry
                        : tableEntry.getValue().entrySet()) {
                    Action<PartitionAndMore> action = partitionEntry.getValue();
                    switch (action.getType()) {
                        case INSERT_EXISTING:
                            hmsCommitter.prepareInsertExistPartition(nm, action.getData());
                            break;
                        case ADD:
                            hmsCommitter.prepareAddPartition(nm, action.getData());
                            break;
                        case ALTER:
                            hmsCommitter.prepareAlterPartition(nm, action.getData());
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unsupported partition action type: " + action.getType());
                    }
                }
            }

            hmsCommitter.doCommit();
        } catch (Throwable t) {
            LOG.warn("Failed to commit for {}, abort it.", queryId);
            try {
                hmsCommitter.abort();
                hmsCommitter.rollback();
            } catch (RuntimeException e) {
                t.addSuppressed(new Exception("Failed to roll back after commit failure", e));
            }
            throw t;
        } finally {
            hmsCommitter.runClearPathsForFinish();
            hmsCommitter.shutdownExecutorService();
        }
    }

    @Override
    public void rollback() {
        if (hmsCommitter == null) {
            collectUncompletedMpuPendingUploads(hivePartitionUpdates);
            if (uncompletedMpuPendingUploads.isEmpty()) {
                return;
            }
            hmsCommitter = new HmsCommitter();
            try {
                hmsCommitter.rollback();
            } finally {
                hmsCommitter.shutdownExecutorService();
            }
            return;
        }
        try {
            hmsCommitter.abort();
            hmsCommitter.rollback();
        } finally {
            hmsCommitter.shutdownExecutorService();
        }
    }

    // ─────────────────────────────── begin-guard (D7) ───────────────────────────────

    // DEC-2: reject ANY transactional Hive table (full-ACID AND insert-only), matching legacy
    // InsertIntoTableCommand's AcidUtils.isTransactionalTable gate. A narrower full-ACID-only reject would let
    // an insert-only ACID table slip through and get plain files (corrupt/invisible data).
    private static void rejectTransactionalWrite(Map<String, String> tableParameters) {
        if (isTransactionalTable(tableParameters)) {
            throw new DorisConnectorException(
                    "Cannot write to a transactional Hive table (only non-ACID INSERT/OVERWRITE is supported)");
        }
    }

    // Mirrors hive AcidUtils.isTablePropertyTransactional: "transactional" (or its upper-cased key) parsed as
    // a boolean. D8: derived plugin-side from the raw HMS parameters (fe-core parses no properties).
    private static boolean isTransactionalTable(Map<String, String> params) {
        if (params == null) {
            return false;
        }
        String value = params.get("transactional");
        if (value == null) {
            value = params.get("transactional".toUpperCase(Locale.ROOT));
        }
        return Boolean.parseBoolean(value);
    }

    // Mirrors hive AcidUtils.isInsertOnlyTable: transactional_properties == "insert_only" (case-insensitive).
    private static boolean isInsertOnlyTable(Map<String, String> params) {
        if (params == null) {
            return false;
        }
        return "insert_only".equalsIgnoreCase(params.get("transactional_properties"));
    }

    // Mirrors hive AcidUtils.isFullAcidTable: transactional AND NOT insert-only.
    private static boolean isFullAcidTable(Map<String, String> params) {
        return isTransactionalTable(params) && !isInsertOnlyTable(params);
    }

    // ─────────────────────────────── classification (legacy finishInsertTable) ───────────────────────────────

    void finishInsertTable(NameMapping nameMapping) {
        HmsTableInfo table = getTable(nameMapping);
        if (hivePartitionUpdates.isEmpty() && isOverwrite && table.getPartitionKeys().isEmpty()) {
            // INSERT OVERWRITE from an empty source: fabricate one empty OVERWRITE update to clean the table.
            isMockedPartitionUpdate = true;
            THivePartitionUpdate emptyUpdate = new THivePartitionUpdate();
            emptyUpdate.setUpdateMode(TUpdateMode.OVERWRITE);
            emptyUpdate.setFileSize(0);
            emptyUpdate.setRowCount(0);
            emptyUpdate.setFileNames(Collections.emptyList());
            if (fileType == TFileType.FILE_S3) {
                emptyUpdate.setS3MpuPendingUploads(new ArrayList<>(Collections.singletonList(
                        new TS3MPUPendingUpload())));
                THiveLocationParams location = new THiveLocationParams();
                location.setWritePath(table.getLocation());
                emptyUpdate.setLocation(location);
            } else if (stagingDirectory.isPresent()) {
                String v = stagingDirectory.get();
                try {
                    getFileSystem().mkdirs(Location.of(v));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create staging directory: " + v, e);
                }
                THiveLocationParams location = new THiveLocationParams();
                location.setWritePath(v);
                emptyUpdate.setLocation(location);
            }
            hivePartitionUpdates = new ArrayList<>(Collections.singletonList(emptyUpdate));
        }

        List<THivePartitionUpdate> mergedPUs = HiveWriteUtils.mergePartitions(hivePartitionUpdates);
        collectUncompletedMpuPendingUploads(mergedPUs);
        List<Map.Entry<THivePartitionUpdate, HmsPartitionStatistics>> insertExistsPartitions = new ArrayList<>();
        for (THivePartitionUpdate pu : mergedPUs) {
            TUpdateMode updateMode = pu.getUpdateMode();
            HmsPartitionStatistics stats = HmsPartitionStatistics.fromCommonStatistics(
                    pu.getRowCount(), pu.getFileNamesSize(), pu.getFileSize());
            String writePath = pu.getLocation().getWritePath();
            if (table.getPartitionKeys().isEmpty()) {
                Preconditions.checkArgument(mergedPUs.size() == 1,
                        "When updating a non-partitioned table, multiple partitions should not be written");
                switch (updateMode) {
                    case APPEND:
                        finishChangingExistingTable(ActionType.INSERT_EXISTING, nameMapping, writePath,
                                pu.getFileNames(), stats, pu);
                        break;
                    case OVERWRITE:
                        dropTable(nameMapping);
                        createTable(nameMapping, table, writePath, pu.getFileNames(), stats, pu);
                        break;
                    default:
                        throw new RuntimeException("Not support mode:[" + updateMode + "] in unPartitioned table");
                }
            } else {
                switch (updateMode) {
                    case APPEND:
                        insertExistsPartitions.add(new AbstractMap.SimpleImmutableEntry<>(pu, stats));
                        break;
                    case NEW:
                        String partitionName = pu.getName();
                        if (partitionName == null || partitionName.isEmpty()) {
                            LOG.warn("Partition name is null/empty for NEW mode in partitioned table, skipping");
                            break;
                        }
                        List<String> partitionValues = HiveWriteUtils.toPartitionValues(partitionName);
                        boolean existsInHms;
                        try {
                            existsInHms = hmsClient.partitionExists(nameMapping.getRemoteDbName(),
                                    nameMapping.getRemoteTblName(), partitionValues);
                        } catch (Exception e) {
                            // Not found (or the probe failed) -> treat as truly new, mirroring the legacy
                            // getPartition()-in-try-catch.
                            existsInHms = false;
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Partition {} existence probe failed, will create it", partitionName);
                            }
                        }
                        if (existsInHms) {
                            LOG.info("Partition {} already exists in HMS (Doris cache miss), treating as APPEND",
                                    partitionName);
                            insertExistsPartitions.add(new AbstractMap.SimpleImmutableEntry<>(pu, stats));
                        } else {
                            createAndAddPartition(nameMapping, table, partitionValues, writePath, pu, stats, false);
                        }
                        break;
                    case OVERWRITE:
                        String overwritePartitionName = pu.getName();
                        if (overwritePartitionName == null || overwritePartitionName.isEmpty()) {
                            LOG.warn("Partition name is null/empty for OVERWRITE mode in partitioned table, "
                                    + "skipping");
                            break;
                        }
                        createAndAddPartition(nameMapping, table,
                                HiveWriteUtils.toPartitionValues(overwritePartitionName),
                                writePath, pu, stats, true);
                        break;
                    default:
                        throw new RuntimeException("Not support mode:[" + updateMode + "] in partitioned table");
                }
            }
        }

        if (!insertExistsPartitions.isEmpty()) {
            convertToInsertExistingPartitionAction(nameMapping, insertExistsPartitions);
        }
    }

    private void collectUncompletedMpuPendingUploads(List<THivePartitionUpdate> hivePartitionUpdates) {
        for (THivePartitionUpdate pu : hivePartitionUpdates) {
            if (pu.getS3MpuPendingUploads() != null) {
                for (TS3MPUPendingUpload s3MpuPendingUpload : pu.getS3MpuPendingUploads()) {
                    uncompletedMpuPendingUploads.add(
                            new UncompletedMpuPendingUpload(s3MpuPendingUpload, pu.getLocation().getWritePath()));
                }
            }
        }
    }

    private void convertToInsertExistingPartitionAction(
            NameMapping nameMapping,
            List<Map.Entry<THivePartitionUpdate, HmsPartitionStatistics>> partitions) {
        Map<List<String>, Action<PartitionAndMore>> partitionActionsForTable =
                partitionActions.computeIfAbsent(nameMapping, k -> new HashMap<>());

        for (List<Map.Entry<THivePartitionUpdate, HmsPartitionStatistics>> partitionBatch
                : Iterables.partition(partitions, 100)) {

            List<String> partitionNames = partitionBatch.stream()
                    .map(pair -> pair.getKey().getName())
                    .collect(Collectors.toList());

            // check in partitionAction
            Action<PartitionAndMore> oldPartitionAction = partitionActionsForTable.get(partitionNames);
            if (oldPartitionAction != null) {
                switch (oldPartitionAction.getType()) {
                    case DROP:
                    case DROP_PRESERVE_DATA:
                        throw new RuntimeException("Not found partition from partition actions"
                                + "for " + nameMapping.getFullLocalName() + ", partitions: " + partitionNames);
                    case ADD:
                    case ALTER:
                    case INSERT_EXISTING:
                    case MERGE:
                        throw new UnsupportedOperationException("Inserting into a partition that were added, altered,"
                                + "or inserted into in the same transaction is not supported");
                    default:
                        throw new IllegalStateException("Unknown action type: " + oldPartitionAction.getType());
                }
            }

            List<HmsPartitionInfo> hmsPartitions = hmsClient.getPartitions(
                    nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(), partitionNames);
            // Mirror HiveUtil.convertToNamePartitionMap's Collectors.toMap: fail loud on a duplicate
            // key (two HMS partitions with identical values, or a repeated requested partition name)
            // instead of silently overwriting. A silent overwrite would shrink partitionsByNamesMap
            // below partitionNames.size(), and the size()-bounded loop below would then drop a
            // partition's INSERT_EXISTING action (silent write loss) rather than abort the txn.
            Map<List<String>, HmsPartitionInfo> partitionsByValues = new HashMap<>();
            for (HmsPartitionInfo p : hmsPartitions) {
                if (partitionsByValues.put(p.getValues(), p) != null) {
                    throw new IllegalStateException("Duplicate key " + p.getValues());
                }
            }
            Map<String, HmsPartitionInfo> partitionsByNamesMap = new HashMap<>();
            for (String name : partitionNames) {
                HmsPartitionInfo p = partitionsByValues.get(HiveWriteUtils.toPartitionValues(name));
                if (p != null && partitionsByNamesMap.put(name, p) != null) {
                    throw new IllegalStateException("Duplicate key " + name);
                }
            }

            for (int i = 0; i < partitionsByNamesMap.size(); i++) {
                String partitionName = partitionNames.get(i);
                HmsPartitionInfo partition = partitionsByNamesMap.get(partitionName);
                if (partition == null) {
                    // Prevent this partition from being deleted by other engines.
                    throw new RuntimeException("Not found partition from hms for " + nameMapping.getFullLocalName()
                            + ", partitions: " + partitionNames);
                }
                THivePartitionUpdate pu = partitionBatch.get(i).getKey();
                HmsPartitionStatistics updateStats = partitionBatch.get(i).getValue();
                List<String> partitionValues = HiveWriteUtils.toPartitionValues(pu.getName());

                partitionActionsForTable.put(
                        partitionValues,
                        new Action<>(ActionType.INSERT_EXISTING,
                                new PartitionAndMore(
                                        partitionValues,
                                        partition.getLocation(),
                                        pu.getLocation().getWritePath(),
                                        pu.getName(),
                                        pu.getFileNames(),
                                        updateStats,
                                        pu)));
            }
        }
    }

    // ─────────────────────────────── state-transition helpers (legacy, synchronized) ───────────────────────────────

    private synchronized HmsTableInfo getTable(NameMapping nameMapping) {
        Action<TableAndMore> tableAction = tableActions.get(nameMapping);
        if (tableAction == null) {
            // Reuse the begin-time table snapshot (loaded in beginWrite for the D7 reject); the transaction
            // targets exactly this one table, so no re-fetch is needed.
            return hmsTableInfo;
        }
        switch (tableAction.getType()) {
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case MERGE:
                return tableAction.getData().getTable();
            case DROP:
            case DROP_PRESERVE_DATA:
                break;
            default:
                throw new IllegalStateException("Unknown action type: " + tableAction.getType());
        }
        throw new RuntimeException("Not Found table: " + nameMapping);
    }

    private synchronized void finishChangingExistingTable(
            ActionType actionType,
            NameMapping nameMapping,
            String location,
            List<String> fileNames,
            HmsPartitionStatistics statisticsUpdate,
            THivePartitionUpdate hivePartitionUpdate) {
        Action<TableAndMore> oldTableAction = tableActions.get(nameMapping);
        if (oldTableAction == null) {
            tableActions.put(nameMapping,
                    new Action<>(actionType,
                            new TableAndMore(hmsTableInfo, location, fileNames, statisticsUpdate,
                                    hivePartitionUpdate)));
            return;
        }
        switch (oldTableAction.getType()) {
            case DROP:
                throw new RuntimeException("Not found table: " + nameMapping.getFullLocalName());
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case MERGE:
                throw new UnsupportedOperationException("Inserting into an unpartitioned table that were added, "
                        + "altered,or inserted into in the same transaction is not supported");
            case DROP_PRESERVE_DATA:
                break;
            default:
                throw new IllegalStateException("Unknown action type: " + oldTableAction.getType());
        }
    }

    private synchronized void createTable(
            NameMapping nameMapping,
            HmsTableInfo table, String location, List<String> fileNames,
            HmsPartitionStatistics statistics,
            THivePartitionUpdate hivePartitionUpdate) {
        // When creating a table, it should never have partition actions. This is just a sanity check.
        checkNoPartitionAction(nameMapping);
        Action<TableAndMore> oldTableAction = tableActions.get(nameMapping);
        TableAndMore tableAndMore = new TableAndMore(table, location, fileNames, statistics, hivePartitionUpdate);
        if (oldTableAction == null) {
            tableActions.put(nameMapping, new Action<>(ActionType.ADD, tableAndMore));
            return;
        }
        switch (oldTableAction.getType()) {
            case DROP:
                tableActions.put(nameMapping, new Action<>(ActionType.ALTER, tableAndMore));
                return;
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case MERGE:
                throw new RuntimeException("Table already exists: " + nameMapping.getFullLocalName());
            case DROP_PRESERVE_DATA:
                break;
            default:
                throw new IllegalStateException("Unknown action type: " + oldTableAction.getType());
        }
    }

    private synchronized void dropTable(NameMapping nameMapping) {
        // Dropping table with partition actions requires cleaning up staging data, which is not implemented yet.
        checkNoPartitionAction(nameMapping);
        Action<TableAndMore> oldTableAction = tableActions.get(nameMapping);
        if (oldTableAction == null || oldTableAction.getType() == ActionType.ALTER) {
            tableActions.put(nameMapping, new Action<>(ActionType.DROP, null));
            return;
        }
        switch (oldTableAction.getType()) {
            case DROP:
                throw new RuntimeException("Not found table: " + nameMapping.getFullLocalName());
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case MERGE:
                throw new RuntimeException("Dropping a table added/modified in the same transaction is not supported");
            case DROP_PRESERVE_DATA:
                break;
            default:
                throw new IllegalStateException("Unknown action type: " + oldTableAction.getType());
        }
    }

    private void checkNoPartitionAction(NameMapping nameMapping) {
        Map<List<String>, Action<PartitionAndMore>> partitionActionsForTable = partitionActions.get(nameMapping);
        if (partitionActionsForTable != null && !partitionActionsForTable.isEmpty()) {
            throw new RuntimeException(
                    "Cannot make schema changes to a table with modified partitions in the same transaction");
        }
    }

    private void createAndAddPartition(
            NameMapping nameMapping,
            HmsTableInfo table,
            List<String> partitionValues,
            String writePath,
            THivePartitionUpdate pu,
            HmsPartitionStatistics statistics,
            boolean dropFirst) {
        String pathForHms = this.fileType == TFileType.FILE_S3
                ? writePath
                : pu.getLocation().getTargetPath();
        if (dropFirst) {
            dropPartition(nameMapping, partitionValues, true);
        }
        addPartition(nameMapping, partitionValues, pathForHms, writePath, pu.getName(), pu.getFileNames(),
                statistics, pu);
    }

    private synchronized void addPartition(
            NameMapping nameMapping,
            List<String> partitionValues,
            String targetPath,
            String currentLocation,
            String partitionName,
            List<String> files,
            HmsPartitionStatistics statistics,
            THivePartitionUpdate hivePartitionUpdate) {
        Map<List<String>, Action<PartitionAndMore>> partitionActionsForTable =
                partitionActions.computeIfAbsent(nameMapping, k -> new HashMap<>());
        Action<PartitionAndMore> oldPartitionAction = partitionActionsForTable.get(partitionValues);
        if (oldPartitionAction == null) {
            partitionActionsForTable.put(partitionValues,
                    new Action<>(ActionType.ADD,
                            new PartitionAndMore(partitionValues, targetPath, currentLocation, partitionName, files,
                                    statistics, hivePartitionUpdate)));
            return;
        }
        switch (oldPartitionAction.getType()) {
            case DROP:
            case DROP_PRESERVE_DATA:
                partitionActionsForTable.put(partitionValues,
                        new Action<>(ActionType.ALTER,
                                new PartitionAndMore(partitionValues, targetPath, currentLocation, partitionName,
                                        files, statistics, hivePartitionUpdate)));
                return;
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case MERGE:
                throw new RuntimeException("Partition already exists for table: "
                        + nameMapping.getFullLocalName() + ", partition values: " + partitionValues);
            default:
                throw new IllegalStateException("Unknown action type: " + oldPartitionAction.getType());
        }
    }

    private synchronized void dropPartition(
            NameMapping nameMapping,
            List<String> partitionValues,
            boolean deleteData) {
        Map<List<String>, Action<PartitionAndMore>> partitionActionsForTable =
                partitionActions.computeIfAbsent(nameMapping, k -> new HashMap<>());
        Action<PartitionAndMore> oldPartitionAction = partitionActionsForTable.get(partitionValues);
        if (oldPartitionAction == null) {
            if (deleteData) {
                partitionActionsForTable.put(partitionValues, new Action<>(ActionType.DROP, null));
            } else {
                partitionActionsForTable.put(partitionValues, new Action<>(ActionType.DROP_PRESERVE_DATA, null));
            }
            return;
        }
        switch (oldPartitionAction.getType()) {
            case DROP:
            case DROP_PRESERVE_DATA:
                throw new RuntimeException("Not found partition from partition actions for "
                        + nameMapping.getFullLocalName() + ", partitions: " + partitionValues);
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case MERGE:
                throw new RuntimeException("Dropping a partition added in the same transaction is not supported: "
                        + nameMapping.getFullLocalName() + ", partition values: " + partitionValues);
            default:
                throw new IllegalStateException("Unknown action type: " + oldPartitionAction.getType());
        }
    }

    // ─────────────────────────────── filesystem (D6) ───────────────────────────────

    private FileSystem getFileSystem() {
        FileSystem local = fs;
        if (local == null) {
            synchronized (this) {
                local = fs;
                if (local == null) {
                    StorageProperties objSp = context.getStorageProperties().stream()
                            .filter(sp -> sp.kind() == StorageKind.OBJECT_STORAGE)
                            .findFirst()
                            .orElse(null);
                    try {
                        local = resolveObjectStoreFileSystem(objSp);
                    } catch (IOException e) {
                        throw new DorisConnectorException(
                                "Failed to build filesystem for hive write: " + e.getMessage(), e);
                    }
                    fs = local;
                }
            }
        }
        return local;
    }

    /**
     * Builds the plugin-side object-store {@link FileSystem} used for staging renames/deletes and MPU
     * complete/abort (D6). Isolated behind this single method so production ServiceLoader discovery — a
     * cutover-time concern (the object-store providers may be directory-loaded in a separate classloader) —
     * is swappable and unit tests can inject a fake FileSystem by overriding it.
     */
    protected FileSystem resolveObjectStoreFileSystem(StorageProperties objSp) throws IOException {
        if (objSp == null) {
            throw new DorisConnectorException("No object-store StorageProperties available for hive write");
        }
        for (FileSystemProvider provider : ServiceLoader.load(FileSystemProvider.class)) {
            if (provider.supports(objSp.rawProperties())) {
                return provider.create(objSp.rawProperties());
            }
        }
        throw new DorisConnectorException("No FileSystemProvider supports the object-store properties for hive write");
    }

    // ─────────────────────────────── commit-time object-store MPU (D6) ───────────────────────────────

    /**
     * Completes the BE-initiated multipart uploads on the object store (FE finalizes what BE staged). Ported
     * from the legacy {@code objCommit}; only the FileSystem source (plugin-built vs SpiSwitchingFileSystem)
     * and the per-task auth wrap (D5) differ. Skipped for a mocked empty overwrite.
     */
    private void objCommit(List<CompletableFuture<?>> asyncFileSystemTaskFutures,
            AtomicBoolean fileSystemTaskCancelled, THivePartitionUpdate hivePartitionUpdate, String path) {
        if (isMockedPartitionUpdate) {
            return;
        }
        FileSystem resolved = getFileSystem();
        if (!(resolved instanceof ObjFileSystem)) {
            throw new RuntimeException("Expected ObjFileSystem for MPU commit at path '" + path + "', got: "
                    + resolved.getClass().getSimpleName() + ". This path does not point to an object-storage "
                    + "backend.");
        }
        ObjFileSystem objFs = (ObjFileSystem) resolved;
        for (TS3MPUPendingUpload s3MpuPendingUpload : hivePartitionUpdate.getS3MpuPendingUploads()) {
            asyncFileSystemTaskFutures.add(CompletableFuture.runAsync(() -> {
                if (fileSystemTaskCancelled.get()) {
                    return;
                }
                String remotePath = "s3://" + s3MpuPendingUpload.getBucket() + "/" + s3MpuPendingUpload.getKey();
                try {
                    context.executeAuthenticated(() -> {
                        objFs.completeMultipartUpload(remotePath, s3MpuPendingUpload.getUploadId(),
                                s3MpuPendingUpload.getEtags());
                        return null;
                    });
                } catch (Exception e) {
                    throw new RuntimeException("Failed to complete MPU for " + remotePath, e);
                }
                uncompletedMpuPendingUploads.remove(new UncompletedMpuPendingUpload(s3MpuPendingUpload, path));
            }, fileSystemExecutor));
        }
    }

    // ─────────────────────────────── filesystem walk helpers (legacy) ───────────────────────────────

    private void recursiveDeleteItems(Path directory, boolean deleteEmptyDir, boolean reverse) {
        DeleteRecursivelyResult deleteResult = recursiveDeleteFiles(directory, deleteEmptyDir, reverse);
        if (!deleteResult.getNotDeletedEligibleItems().isEmpty()) {
            LOG.warn("Failed to delete directory {}. Some eligible items can't be deleted: {}.",
                    directory.toString(), deleteResult.getNotDeletedEligibleItems());
            throw new RuntimeException(
                    "Failed to delete directory for files: " + deleteResult.getNotDeletedEligibleItems());
        } else if (deleteEmptyDir && !deleteResult.dirNotExists()) {
            LOG.warn("Failed to delete directory {} due to dir isn't empty", directory.toString());
            throw new RuntimeException("Failed to delete directory for empty dir: " + directory.toString());
        }
    }

    private DeleteRecursivelyResult recursiveDeleteFiles(Path directory, boolean deleteEmptyDir, boolean reverse) {
        try {
            boolean dirExists = getFileSystem().exists(Location.of(directory.toString()));
            if (!dirExists) {
                return new DeleteRecursivelyResult(true, Collections.emptyList());
            }
        } catch (IOException e) {
            return new DeleteRecursivelyResult(false,
                    new ArrayList<>(Collections.singletonList(directory.toString() + "/*")));
        }
        return doRecursiveDeleteFiles(directory, deleteEmptyDir, queryId, reverse);
    }

    private DeleteRecursivelyResult doRecursiveDeleteFiles(Path directory, boolean deleteEmptyDir,
            String queryId, boolean reverse) {
        List<FileEntry> allFiles;
        Set<String> allDirs;
        try {
            allFiles = getFileSystem().listFilesRecursive(Location.of(directory.toString()));
            allDirs = getFileSystem().listDirectories(Location.of(directory.toString()));
        } catch (IOException e) {
            return new DeleteRecursivelyResult(false,
                    new ArrayList<>(Collections.singletonList(directory + "/*")));
        }

        boolean allDescendentsDeleted = true;
        List<String> notDeletedEligibleItems = new ArrayList<>();
        for (FileEntry file : allFiles) {
            String fileName = new Path(file.location().uri()).getName();
            String filePath = file.location().uri();
            if (reverse ^ fileName.startsWith(queryId)) {
                if (!deleteIfExists(new Path(filePath))) {
                    allDescendentsDeleted = false;
                    notDeletedEligibleItems.add(filePath);
                }
            } else {
                allDescendentsDeleted = false;
            }
        }

        for (String dir : allDirs) {
            DeleteRecursivelyResult subResult = doRecursiveDeleteFiles(new Path(dir), deleteEmptyDir, queryId, reverse);
            if (!subResult.dirNotExists()) {
                allDescendentsDeleted = false;
            }
            if (!subResult.getNotDeletedEligibleItems().isEmpty()) {
                notDeletedEligibleItems.addAll(subResult.getNotDeletedEligibleItems());
            }
        }

        if (allDescendentsDeleted && deleteEmptyDir) {
            Verify.verify(notDeletedEligibleItems.isEmpty());
            if (!deleteDirectoryIfExists(directory)) {
                return new DeleteRecursivelyResult(false,
                        new ArrayList<>(Collections.singletonList(directory + "/")));
            }
            // all items of the location have been deleted.
            return new DeleteRecursivelyResult(true, Collections.emptyList());
        }
        return new DeleteRecursivelyResult(false, notDeletedEligibleItems);
    }

    private boolean deleteIfExists(Path path) {
        deleteFile(path.toString());
        try {
            return !getFileSystem().exists(Location.of(path.toString()));
        } catch (IOException e) {
            return false;
        }
    }

    private boolean deleteDirectoryIfExists(Path path) {
        deleteDir(path.toString());
        try {
            return !getFileSystem().exists(Location.of(path.toString()));
        } catch (IOException e) {
            return false;
        }
    }

    private void deleteFile(String remotePath) {
        try {
            getFileSystem().delete(Location.of(remotePath), false);
        } catch (IOException e) {
            LOG.warn("Failed to delete {}: {}", remotePath, e.getMessage());
        }
    }

    private void deleteDir(String remotePath) {
        try {
            getFileSystem().delete(Location.of(remotePath), true);
        } catch (IOException e) {
            LOG.warn("Failed to delete directory {}: {}", remotePath, e.getMessage());
        }
    }

    private void renameDirectory(String origFilePath, String destFilePath, Runnable runWhenPathNotExist) {
        try {
            getFileSystem().renameDirectory(Location.of(origFilePath), Location.of(destFilePath),
                    runWhenPathNotExist);
        } catch (IOException e) {
            throw new RuntimeException("Failed to rename directory from " + origFilePath
                    + " to " + destFilePath + ": " + e.getMessage(), e);
        }
    }

    private void deleteTargetPathContents(String targetPath, String excludedChildPath) {
        try {
            Set<String> dirs = getFileSystem().listDirectories(Location.of(targetPath));
            for (String dir : dirs) {
                if (excludedChildPath != null && HiveWriteUtils.pathsEqual(dir, excludedChildPath)) {
                    continue;
                }
                deleteDir(dir);
            }
            List<FileEntry> files = getFileSystem().listFiles(Location.of(targetPath));
            for (FileEntry file : files) {
                deleteFile(file.location().uri());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to list/delete contents under " + targetPath, e);
        }
    }

    private void ensureDirectory(String path) {
        try {
            getFileSystem().mkdirs(Location.of(path));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directory " + path + ": " + e.getMessage(), e);
        }
    }

    // ─────────────────────────────── column conversion (GAP-4) ───────────────────────────────

    private static List<FieldSchema> toFieldSchemas(List<ConnectorColumn> columns) {
        List<FieldSchema> result = new ArrayList<>(columns.size());
        for (ConnectorColumn c : columns) {
            result.add(new FieldSchema(c.getName(), HmsTypeMapping.toHiveTypeString(c.getType()), c.getComment()));
        }
        return result;
    }

    // ─────────────────────────────── test seams (package-private) ───────────────────────────────

    NameMapping getNameMapping() {
        return nameMapping;
    }

    Map<NameMapping, Action<TableAndMore>> getTableActions() {
        return tableActions;
    }

    Map<NameMapping, Map<List<String>, Action<PartitionAndMore>>> getPartitionActions() {
        return partitionActions;
    }

    // ─────────────────────────────── static helpers ───────────────────────────────

    private static ThreadFactory namedDaemonThreadFactory(String nameFormat) {
        AtomicInteger counter = new AtomicInteger(0);
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName(String.format(nameFormat, counter.getAndIncrement()));
            thread.setDaemon(true);
            return thread;
        };
    }

    private static Object getFutureValue(CompletableFuture<?> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static void addSuppressedExceptions(List<Throwable> suppressedExceptions, Throwable t,
            List<String> descriptions, String description) {
        descriptions.add(description);
        // A limit is needed to avoid having a huge exception object. 5 was chosen arbitrarily.
        if (suppressedExceptions.size() < 5) {
            suppressedExceptions.add(t);
        }
    }

    private static void shutdownExecutorService(ExecutorService executor) {
        // Disable new tasks from being submitted.
        executor.shutdown();
        try {
            // Wait a while for existing tasks to terminate.
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                // Cancel currently executing tasks.
                executor.shutdownNow();
                // Wait a while for tasks to respond to being cancelled.
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    LOG.warn("Pool did not terminate");
                }
            }
        } catch (InterruptedException e) {
            // (Re-)Cancel if current thread also interrupted.
            executor.shutdownNow();
            // Preserve interrupt status.
            Thread.currentThread().interrupt();
        }
    }

    // ─────────────────────────────── inner: committer ───────────────────────────────

    class HmsCommitter {

        // update statistics for unPartitioned table or existed partition
        private final List<UpdateStatisticsTask> updateStatisticsTasks = new ArrayList<>();
        private final ExecutorService updateStatisticsExecutor = Executors.newFixedThreadPool(16);

        // add new partition
        private final AddPartitionsTask addPartitionsTask = new AddPartitionsTask();

        // for file system rename operation: whether to cancel the file system tasks
        private final AtomicBoolean fileSystemTaskCancelled = new AtomicBoolean(false);
        // file system tasks that are executed asynchronously, including rename_file, rename_dir
        private final List<CompletableFuture<?>> asyncFileSystemTaskFutures = new ArrayList<>();
        // when aborted, we need to delete all files under this path, even the current directory
        private final Queue<DirectoryCleanUpTask> directoryCleanUpTasksForAbort = new ConcurrentLinkedQueue<>();
        // when aborted, we need restore directory
        private final List<RenameDirectoryTask> renameDirectoryTasksForAbort = new ArrayList<>();
        // when finished, we need clear some directories
        private final List<String> clearDirsForFinish = new ArrayList<>();
        private final List<String> s3cleanWhenSuccess = new ArrayList<>();

        void cancelUnStartedAsyncFileSystemTask() {
            fileSystemTaskCancelled.set(true);
        }

        private void undoUpdateStatisticsTasks() {
            List<CompletableFuture<?>> undoUpdateFutures = new ArrayList<>();
            for (UpdateStatisticsTask task : updateStatisticsTasks) {
                undoUpdateFutures.add(CompletableFuture.runAsync(() -> {
                    try {
                        task.undo(hmsClient);
                    } catch (Throwable throwable) {
                        LOG.warn("Failed to rollback: {}", task.getDescription(), throwable);
                    }
                }, updateStatisticsExecutor));
            }
            for (CompletableFuture<?> undoUpdateFuture : undoUpdateFutures) {
                getFutureValue(undoUpdateFuture);
            }
            updateStatisticsTasks.clear();
        }

        private void undoAddPartitionsTask() {
            if (addPartitionsTask.isEmpty()) {
                return;
            }
            List<List<String>> rollbackFailedPartitions = addPartitionsTask.rollback(hmsClient);
            if (!rollbackFailedPartitions.isEmpty()) {
                LOG.warn("Failed to rollback: add_partition for partition values {}", rollbackFailedPartitions);
            }
            addPartitionsTask.clear();
        }

        private void waitForAsyncFileSystemTaskSuppressThrowable() {
            for (CompletableFuture<?> future : asyncFileSystemTaskFutures) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    // ignore
                }
            }
            asyncFileSystemTaskFutures.clear();
        }

        void prepareInsertExistingTable(NameMapping nameMapping, TableAndMore tableAndMore) {
            HmsTableInfo table = tableAndMore.getTable();
            String targetPath = table.getLocation();
            String writePath = tableAndMore.getCurrentLocation();
            // In the BE, all object stores are unified under the "s3" URI scheme, so a rename is only needed
            // when target and write paths differ AFTER ignoring an s3-vs-native scheme difference (GAP-11:
            // NOT HiveWriteUtils.pathsEqual, which treats a scheme mismatch as a different filesystem).
            boolean needRename = !HiveWriteUtils.equalsIgnoreSchemeIfOneIsS3(targetPath, writePath);
            if (needRename) {
                FileSystemUtil.asyncRenameFiles(getFileSystem(), authWrappingExecutor, asyncFileSystemTaskFutures,
                        fileSystemTaskCancelled, writePath, targetPath, tableAndMore.getFileNames());
            } else if (hasPendingUploads(tableAndMore.getHivePartitionUpdate())) {
                objCommit(asyncFileSystemTaskFutures, fileSystemTaskCancelled,
                        tableAndMore.getHivePartitionUpdate(), targetPath);
            }
            directoryCleanUpTasksForAbort.add(new DirectoryCleanUpTask(targetPath, false));
            updateStatisticsTasks.add(new UpdateStatisticsTask(nameMapping, Optional.empty(),
                    tableAndMore.getStatisticsUpdate(), true));
        }

        void prepareAlterTable(NameMapping nameMapping, TableAndMore tableAndMore) {
            HmsTableInfo table = tableAndMore.getTable();
            String targetPath = table.getLocation();
            String writePath = tableAndMore.getCurrentLocation();
            if (!targetPath.equals(writePath)) {
                if (HiveWriteUtils.isSubDirectory(targetPath, writePath)) {
                    String stagingRoot = HiveWriteUtils.getImmediateChildPath(targetPath, writePath);
                    deleteTargetPathContents(targetPath, stagingRoot);
                    ensureDirectory(targetPath);
                    FileSystemUtil.asyncRenameFiles(getFileSystem(), authWrappingExecutor, asyncFileSystemTaskFutures,
                            fileSystemTaskCancelled, writePath, targetPath, tableAndMore.getFileNames());
                } else {
                    Path path = new Path(targetPath);
                    String oldTablePath = new Path(
                            path.getParent(), "_temp_" + queryId + "_" + path.getName()).toString();
                    renameDirectoryTasksForAbort.add(new RenameDirectoryTask(oldTablePath, targetPath));
                    renameDirectory(targetPath, oldTablePath, () -> { });
                    clearDirsForFinish.add(oldTablePath);
                    directoryCleanUpTasksForAbort.add(new DirectoryCleanUpTask(targetPath, true));
                    renameDirectory(writePath, targetPath, () -> { });
                }
            } else if (hasPendingUploads(tableAndMore.getHivePartitionUpdate())) {
                s3cleanWhenSuccess.add(targetPath);
                objCommit(asyncFileSystemTaskFutures, fileSystemTaskCancelled,
                        tableAndMore.getHivePartitionUpdate(), targetPath);
            }
            updateStatisticsTasks.add(new UpdateStatisticsTask(nameMapping, Optional.empty(),
                    tableAndMore.getStatisticsUpdate(), false));
        }

        void prepareAddPartition(NameMapping nameMapping, PartitionAndMore partitionAndMore) {
            String targetPath = partitionAndMore.getTargetPath();
            String writePath = partitionAndMore.getCurrentLocation();
            if (!targetPath.equals(writePath)) {
                directoryCleanUpTasksForAbort.add(new DirectoryCleanUpTask(targetPath, true));
                FileSystemUtil.asyncRenameDir(getFileSystem(), authWrappingExecutor, asyncFileSystemTaskFutures,
                        fileSystemTaskCancelled, writePath, targetPath, () -> { });
            } else if (hasPendingUploads(partitionAndMore.getHivePartitionUpdate())) {
                objCommit(asyncFileSystemTaskFutures, fileSystemTaskCancelled,
                        partitionAndMore.getHivePartitionUpdate(), targetPath);
            }

            // Rebuild the partition storage descriptor from the table at commit time (mirrors the legacy
            // "build partition SD from table SD"): the ADD case is the only one that needs columns/formats.
            HmsTableInfo table = getTable(nameMapping);
            HmsPartitionWithStatistics partitionWithStats = HmsPartitionWithStatistics.builder()
                    .name(partitionAndMore.getPartitionName())
                    .partitionValues(partitionAndMore.getPartitionValues())
                    .location(targetPath)
                    .columns(toFieldSchemas(table.getColumns()))
                    .inputFormat(table.getInputFormat())
                    .outputFormat(table.getOutputFormat())
                    .serde(table.getSerializationLib())
                    .parameters(new HashMap<>())
                    .statistics(partitionAndMore.getStatisticsUpdate())
                    .build();
            addPartitionsTask.addPartition(nameMapping, partitionWithStats);
        }

        void prepareInsertExistPartition(NameMapping nameMapping, PartitionAndMore partitionAndMore) {
            String targetPath = partitionAndMore.getTargetPath();
            String writePath = partitionAndMore.getCurrentLocation();
            directoryCleanUpTasksForAbort.add(new DirectoryCleanUpTask(targetPath, false));
            if (!targetPath.equals(writePath)) {
                FileSystemUtil.asyncRenameFiles(getFileSystem(), authWrappingExecutor, asyncFileSystemTaskFutures,
                        fileSystemTaskCancelled, writePath, targetPath, partitionAndMore.getFileNames());
            } else if (hasPendingUploads(partitionAndMore.getHivePartitionUpdate())) {
                objCommit(asyncFileSystemTaskFutures, fileSystemTaskCancelled,
                        partitionAndMore.getHivePartitionUpdate(), targetPath);
            }
            updateStatisticsTasks.add(new UpdateStatisticsTask(nameMapping,
                    Optional.of(partitionAndMore.getPartitionName()), partitionAndMore.getStatisticsUpdate(), true));
        }

        void prepareAlterPartition(NameMapping nameMapping, PartitionAndMore partitionAndMore) {
            String targetPath = partitionAndMore.getTargetPath();
            String writePath = partitionAndMore.getCurrentLocation();
            if (!targetPath.equals(writePath)) {
                Path path = new Path(targetPath);
                String oldPartitionPath = new Path(
                        path.getParent(), "_temp_" + queryId + "_" + path.getName()).toString();
                renameDirectoryTasksForAbort.add(new RenameDirectoryTask(oldPartitionPath, targetPath));
                renameDirectory(targetPath, oldPartitionPath, () -> { });
                clearDirsForFinish.add(oldPartitionPath);
                directoryCleanUpTasksForAbort.add(new DirectoryCleanUpTask(targetPath, true));
                renameDirectory(writePath, targetPath, () -> { });
            } else if (hasPendingUploads(partitionAndMore.getHivePartitionUpdate())) {
                s3cleanWhenSuccess.add(targetPath);
                objCommit(asyncFileSystemTaskFutures, fileSystemTaskCancelled,
                        partitionAndMore.getHivePartitionUpdate(), targetPath);
            }
            updateStatisticsTasks.add(new UpdateStatisticsTask(nameMapping,
                    Optional.of(partitionAndMore.getPartitionName()), partitionAndMore.getStatisticsUpdate(), false));
        }

        private void runDirectoryClearUpTasksForAbort() {
            for (DirectoryCleanUpTask cleanUpTask : directoryCleanUpTasksForAbort) {
                recursiveDeleteItems(cleanUpTask.getPath(), cleanUpTask.isDeleteEmptyDir(), false);
            }
            directoryCleanUpTasksForAbort.clear();
        }

        private void runRenameDirTasksForAbort() {
            for (RenameDirectoryTask task : renameDirectoryTasksForAbort) {
                try {
                    boolean srcExists = getFileSystem().exists(Location.of(task.getRenameFrom()));
                    if (srcExists) {
                        renameDirectory(task.getRenameFrom(), task.getRenameTo(), () -> { });
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to abort rename dir from {} to {}: {}",
                            task.getRenameFrom(), task.getRenameTo(), e.getMessage());
                }
            }
            renameDirectoryTasksForAbort.clear();
        }

        void runClearPathsForFinish() {
            for (String path : clearDirsForFinish) {
                deleteDir(path);
            }
        }

        private void runS3cleanWhenSuccess() {
            for (String path : s3cleanWhenSuccess) {
                recursiveDeleteItems(new Path(path), false, true);
            }
        }

        private void waitForAsyncFileSystemTasks() {
            for (CompletableFuture<?> future : asyncFileSystemTaskFutures) {
                getFutureValue(future);
            }
        }

        private void doAddPartitionsTask() {
            // No committer-side batching: ThriftHmsClient.addPartitions batches internally (GAP-7), so the
            // whole list is added in one call.
            if (!addPartitionsTask.isEmpty()) {
                addPartitionsTask.run(hmsClient);
            }
        }

        private void doUpdateStatisticsTasks() {
            List<CompletableFuture<?>> updateStatsFutures = new ArrayList<>();
            List<String> failedTaskDescriptions = new ArrayList<>();
            List<Throwable> suppressedExceptions = new ArrayList<>();
            for (UpdateStatisticsTask task : updateStatisticsTasks) {
                updateStatsFutures.add(CompletableFuture.runAsync(() -> {
                    try {
                        task.run(hmsClient);
                    } catch (Throwable t) {
                        synchronized (suppressedExceptions) {
                            addSuppressedExceptions(suppressedExceptions, t, failedTaskDescriptions,
                                    task.getDescription());
                        }
                    }
                }, updateStatisticsExecutor));
            }
            for (CompletableFuture<?> executeUpdateFuture : updateStatsFutures) {
                getFutureValue(executeUpdateFuture);
            }
            if (!suppressedExceptions.isEmpty()) {
                StringBuilder message = new StringBuilder();
                message.append("Failed to execute some updating statistics tasks: ");
                message.append(String.join("; ", failedTaskDescriptions));
                RuntimeException exception = new RuntimeException(message.toString());
                suppressedExceptions.forEach(exception::addSuppressed);
                throw exception;
            }
        }

        private void pruneAndDeleteStagingDirectories() {
            stagingDirectory.ifPresent((v) -> recursiveDeleteItems(new Path(v), true, false));
        }

        private void abortMultiUploads() {
            if (uncompletedMpuPendingUploads.isEmpty()) {
                return;
            }
            FileSystem resolved = getFileSystem();
            for (UncompletedMpuPendingUpload uncompletedMpuPendingUpload : uncompletedMpuPendingUploads) {
                if (!(resolved instanceof ObjFileSystem)) {
                    LOG.warn("FileSystem {} is not object-storage, skipping MPU abort for {}",
                            resolved.getClass().getSimpleName(), uncompletedMpuPendingUpload.path);
                    continue;
                }
                ObjFileSystem objFs = (ObjFileSystem) resolved;
                TS3MPUPendingUpload mpu = uncompletedMpuPendingUpload.s3MPUPendingUpload;
                String remotePath = "s3://" + mpu.getBucket() + "/" + mpu.getKey();
                asyncFileSystemTaskFutures.add(CompletableFuture.runAsync(() -> {
                    try {
                        context.executeAuthenticated(() -> {
                            objFs.getObjStorage().abortMultipartUpload(remotePath, mpu.getUploadId());
                            return null;
                        });
                    } catch (Exception e) {
                        LOG.warn("Failed to abort MPU for {}: {}", remotePath, e.getMessage());
                    }
                }, fileSystemExecutor));
            }
            uncompletedMpuPendingUploads.clear();
        }

        void doCommit() {
            waitForAsyncFileSystemTasks();
            runS3cleanWhenSuccess();
            doAddPartitionsTask();
            doUpdateStatisticsTasks();
            // delete write path
            pruneAndDeleteStagingDirectories();
        }

        void abort() {
            cancelUnStartedAsyncFileSystemTask();
            undoUpdateStatisticsTasks();
            undoAddPartitionsTask();
            waitForAsyncFileSystemTaskSuppressThrowable();
            runDirectoryClearUpTasksForAbort();
            runRenameDirTasksForAbort();
        }

        void rollback() {
            // delete write path
            pruneAndDeleteStagingDirectories();
            // abort the in-progress multipart uploads
            abortMultiUploads();
            for (CompletableFuture<?> future : asyncFileSystemTaskFutures) {
                getFutureValue(future);
            }
            asyncFileSystemTaskFutures.clear();
        }

        void shutdownExecutorService() {
            HiveConnectorTransaction.shutdownExecutorService(updateStatisticsExecutor);
        }

        private boolean hasPendingUploads(THivePartitionUpdate pu) {
            List<TS3MPUPendingUpload> uploads = pu.getS3MpuPendingUploads();
            return uploads != null && !uploads.isEmpty();
        }
    }

    // ─────────────────────────────── inner: tasks ───────────────────────────────

    private static class UpdateStatisticsTask {
        private final NameMapping nameMapping;
        private final Optional<String> partitionName;
        private final HmsPartitionStatistics updatePartitionStat;
        private final boolean merge;
        private boolean done;

        UpdateStatisticsTask(NameMapping nameMapping, Optional<String> partitionName,
                HmsPartitionStatistics statistics, boolean merge) {
            this.nameMapping = Objects.requireNonNull(nameMapping, "nameMapping is null");
            this.partitionName = Objects.requireNonNull(partitionName, "partitionName is null");
            this.updatePartitionStat = Objects.requireNonNull(statistics, "statistics is null");
            this.merge = merge;
        }

        void run(HmsClient client) {
            if (partitionName.isPresent()) {
                client.updatePartitionStatistics(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(),
                        partitionName.get(), this::updateStatistics);
            } else {
                client.updateTableStatistics(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(),
                        this::updateStatistics);
            }
            done = true;
        }

        void undo(HmsClient client) {
            if (!done) {
                return;
            }
            if (partitionName.isPresent()) {
                client.updatePartitionStatistics(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(),
                        partitionName.get(), this::resetStatistics);
            } else {
                client.updateTableStatistics(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(),
                        this::resetStatistics);
            }
        }

        String getDescription() {
            if (partitionName.isPresent()) {
                return "alter partition parameters " + nameMapping.getFullLocalName() + " " + partitionName.get();
            } else {
                return "alter table parameters " + nameMapping.getFullRemoteName();
            }
        }

        private HmsPartitionStatistics updateStatistics(HmsPartitionStatistics currentStats) {
            return merge ? HmsPartitionStatistics.merge(currentStats, updatePartitionStat) : updatePartitionStat;
        }

        private HmsPartitionStatistics resetStatistics(HmsPartitionStatistics currentStatistics) {
            // GAP-2: ReduceOperator lives on HmsCommonStatistics.
            return HmsPartitionStatistics.reduce(currentStatistics, updatePartitionStat,
                    HmsCommonStatistics.ReduceOperator.SUBTRACT);
        }
    }

    private static class AddPartitionsTask {
        private final List<HmsPartitionWithStatistics> partitions = new ArrayList<>();
        private final List<List<String>> createdPartitionValues = new ArrayList<>();
        private NameMapping nameMapping;

        boolean isEmpty() {
            return partitions.isEmpty();
        }

        void clear() {
            partitions.clear();
            createdPartitionValues.clear();
        }

        void addPartition(NameMapping nameMapping, HmsPartitionWithStatistics partition) {
            this.nameMapping = nameMapping;
            partitions.add(partition);
        }

        void run(HmsClient client) {
            client.addPartitions(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(), partitions);
            for (HmsPartitionWithStatistics partition : partitions) {
                createdPartitionValues.add(partition.getPartitionValues());
            }
        }

        List<List<String>> rollback(HmsClient client) {
            List<List<String>> rollbackFailedPartitions = new ArrayList<>();
            for (List<String> createdPartitionValue : createdPartitionValues) {
                try {
                    client.dropPartition(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(),
                            createdPartitionValue, false);
                } catch (Throwable t) {
                    LOG.warn("Failed to drop partition on {} when rollback: {}",
                            nameMapping.getFullLocalName(), createdPartitionValue);
                    rollbackFailedPartitions.add(createdPartitionValue);
                }
            }
            return rollbackFailedPartitions;
        }
    }

    private static class DirectoryCleanUpTask {
        private final Path path;
        private final boolean deleteEmptyDir;

        DirectoryCleanUpTask(String path, boolean deleteEmptyDir) {
            this.path = new Path(path);
            this.deleteEmptyDir = deleteEmptyDir;
        }

        Path getPath() {
            return path;
        }

        boolean isDeleteEmptyDir() {
            return deleteEmptyDir;
        }
    }

    private static class DeleteRecursivelyResult {
        private final boolean dirNoLongerExists;
        private final List<String> notDeletedEligibleItems;

        DeleteRecursivelyResult(boolean dirNoLongerExists, List<String> notDeletedEligibleItems) {
            this.dirNoLongerExists = dirNoLongerExists;
            this.notDeletedEligibleItems = notDeletedEligibleItems;
        }

        boolean dirNotExists() {
            return dirNoLongerExists;
        }

        List<String> getNotDeletedEligibleItems() {
            return notDeletedEligibleItems;
        }
    }

    private static class RenameDirectoryTask {
        private final String renameFrom;
        private final String renameTo;

        RenameDirectoryTask(String renameFrom, String renameTo) {
            this.renameFrom = renameFrom;
            this.renameTo = renameTo;
        }

        String getRenameFrom() {
            return renameFrom;
        }

        String getRenameTo() {
            return renameTo;
        }
    }

    private static class UncompletedMpuPendingUpload {
        private final TS3MPUPendingUpload s3MPUPendingUpload;
        private final String path;

        UncompletedMpuPendingUpload(TS3MPUPendingUpload s3MPUPendingUpload, String path) {
            this.s3MPUPendingUpload = s3MPUPendingUpload;
            this.path = path;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UncompletedMpuPendingUpload that = (UncompletedMpuPendingUpload) o;
            return Objects.equals(s3MPUPendingUpload, that.s3MPUPendingUpload) && Objects.equals(path, that.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(s3MPUPendingUpload, path);
        }
    }

    // ─────────────────────────────── inner: action model ───────────────────────────────

    enum ActionType {
        // drop a table/partition
        DROP,
        // drop a table/partition but will preserve data
        DROP_PRESERVE_DATA,
        // add a table/partition
        ADD,
        // drop then add a table/partition, like overwrite
        ALTER,
        // insert into an existing table/partition
        INSERT_EXISTING,
        // merge into an existing table/partition
        MERGE
    }

    static class Action<T> {
        private final ActionType type;
        private final T data;

        Action(ActionType type, T data) {
            this.type = Objects.requireNonNull(type, "type is null");
            if (type == ActionType.DROP || type == ActionType.DROP_PRESERVE_DATA) {
                Preconditions.checkArgument(data == null, "data is not null");
            } else {
                Objects.requireNonNull(data, "data is null");
            }
            this.data = data;
        }

        ActionType getType() {
            return type;
        }

        T getData() {
            Preconditions.checkState(type != ActionType.DROP);
            return data;
        }
    }

    private static class TableAndMore {
        private final HmsTableInfo table;
        private final String currentLocation;
        private final List<String> fileNames;
        private final HmsPartitionStatistics statisticsUpdate;
        private final THivePartitionUpdate hivePartitionUpdate;

        TableAndMore(HmsTableInfo table, String currentLocation, List<String> fileNames,
                HmsPartitionStatistics statisticsUpdate, THivePartitionUpdate hivePartitionUpdate) {
            this.table = Objects.requireNonNull(table, "table is null");
            this.currentLocation = Objects.requireNonNull(currentLocation, "currentLocation is null");
            this.fileNames = Objects.requireNonNull(fileNames, "fileNames is null");
            this.statisticsUpdate = Objects.requireNonNull(statisticsUpdate, "statisticsUpdate is null");
            this.hivePartitionUpdate = Objects.requireNonNull(hivePartitionUpdate, "hivePartitionUpdate is null");
        }

        HmsTableInfo getTable() {
            return table;
        }

        String getCurrentLocation() {
            return currentLocation;
        }

        List<String> getFileNames() {
            return fileNames;
        }

        HmsPartitionStatistics getStatisticsUpdate() {
            return statisticsUpdate;
        }

        THivePartitionUpdate getHivePartitionUpdate() {
            return hivePartitionUpdate;
        }
    }

    private static class PartitionAndMore {
        private final List<String> partitionValues;
        private final String targetPath;
        private final String currentLocation;
        private final String partitionName;
        private final List<String> fileNames;
        private final HmsPartitionStatistics statisticsUpdate;
        private final THivePartitionUpdate hivePartitionUpdate;

        PartitionAndMore(List<String> partitionValues, String targetPath, String currentLocation,
                String partitionName, List<String> fileNames, HmsPartitionStatistics statisticsUpdate,
                THivePartitionUpdate hivePartitionUpdate) {
            this.partitionValues = Objects.requireNonNull(partitionValues, "partitionValues is null");
            this.targetPath = Objects.requireNonNull(targetPath, "targetPath is null");
            this.currentLocation = Objects.requireNonNull(currentLocation, "currentLocation is null");
            this.partitionName = Objects.requireNonNull(partitionName, "partitionName is null");
            this.fileNames = Objects.requireNonNull(fileNames, "fileNames is null");
            this.statisticsUpdate = Objects.requireNonNull(statisticsUpdate, "statisticsUpdate is null");
            this.hivePartitionUpdate = Objects.requireNonNull(hivePartitionUpdate, "hivePartitionUpdate is null");
        }

        List<String> getPartitionValues() {
            return partitionValues;
        }

        String getTargetPath() {
            return targetPath;
        }

        String getCurrentLocation() {
            return currentLocation;
        }

        String getPartitionName() {
            return partitionName;
        }

        List<String> getFileNames() {
            return fileNames;
        }

        HmsPartitionStatistics getStatisticsUpdate() {
            return statisticsUpdate;
        }

        THivePartitionUpdate getHivePartitionUpdate() {
            return hivePartitionUpdate;
        }
    }
}
