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

package org.apache.doris.cloud.backup;

import org.apache.doris.analysis.RestoreStmt;
import org.apache.doris.backup.BackupJobInfo;
import org.apache.doris.backup.RestoreFileMapping.IdChain;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.backup.SnapshotInfo;
import org.apache.doris.backup.Status;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.common.util.CopyUtil;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CloudRestoreJob extends RestoreJob {

    private static final Logger LOG = LogManager.getLogger(CloudRestoreJob.class);

    private static final String PROP_STORAGE_VAULT_NAME = RestoreStmt.PROP_STORAGE_VAULT_NAME;

    @SerializedName("storageVaultName")
    private String storageVaultName = null;

    @SerializedName("cloudClusterName")
    private String cloudClusterName = null;

    private String storageVaultId = "";

    private String cloudClusterId = null;

    private Map<OlapTable, Cloud.CreateTabletsRequest.Builder> tabletsPerTable = new HashMap<>();

    public enum MetaSeriviceOperation {
        PREPARE,
        COMMIT,
        DROP
    }

    public CloudRestoreJob() {
        super();
    }

    public CloudRestoreJob(JobType jobType) {
        super(jobType);
    }

    public CloudRestoreJob(String label, String backupTs, long dbId, String dbName, BackupJobInfo jobInfo,
            boolean allowLoad, ReplicaAllocation replicaAlloc, long timeoutMs, int metaVersion, boolean reserveReplica,
            boolean reserveDynamicPartitionEnable, boolean isBeingSynced, boolean isCleanTables,
            boolean isCleanPartitions, boolean isAtomicRestore, boolean isForceReplace, Env env, long repoId,
            String storageVaultName) {
        super(label, backupTs, dbId, dbName, jobInfo, allowLoad, replicaAlloc, timeoutMs, metaVersion, reserveReplica,
                reserveDynamicPartitionEnable, isBeingSynced, isCleanTables, isCleanPartitions, isAtomicRestore,
                isForceReplace, env, repoId);
        if (((CloudEnv) env).getEnableStorageVault()) {
            this.storageVaultName = storageVaultName;
            properties.put(PROP_STORAGE_VAULT_NAME, storageVaultName);
        }
        ConnectContext context = ConnectContext.get();
        if (context != null) {
            String clusterName = "";
            try {
                clusterName = context.getCloudCluster();
            } catch (ComputeGroupException e) {
                LOG.warn("failed to get compute group name", e);
            }
            if (!Strings.isNullOrEmpty(clusterName)) {
                this.cloudClusterName = clusterName;
                this.cloudClusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterIdByName(
                        cloudClusterName);
            }
        }
    }

    private AutoCloseConnectContext buildConnectContext() throws UserException {
        if (Strings.isNullOrEmpty(cloudClusterName)) {
            throw new UserException("compute group name is not set.");
        }
        if (ConnectContext.get() == null) {
            ConnectContext ctx = new ConnectContext();
            ctx.setCloudCluster(cloudClusterName);
            return new AutoCloseConnectContext(ctx);
        } else {
            ConnectContext.get().setCloudCluster(cloudClusterName);
            return null;
        }
    }

    @Override
    public synchronized void run() {
        if (state == RestoreJobState.FINISHED || state == RestoreJobState.CANCELLED) {
            return;
        }
        try (AutoCloseConnectContext r = buildConnectContext()) {
            super.run();
        } catch (UserException e) {
            LOG.error("failed to run cloud restore job", e);
        }
    }

    @Override
    public void checkIfNeedCancel() {
        super.checkIfNeedCancel();
        if ((cloudClusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterIdByName(
                cloudClusterName)) == null) {
            status = new Status(Status.ErrCode.NOT_FOUND, "compute group " + cloudClusterName
                    + " has been removed");
        }
    }

    @Override
    public void checkStorageVault(OlapTable localTable) {
        if (((CloudEnv) Env.getCurrentEnv()).getEnableStorageVault()) {
            Preconditions.checkNotNull(storageVaultName);
            if (localTable.getStorageVaultId().isEmpty()) {
                status = new Status(Status.ErrCode.COMMON_ERROR, "local table " + localTable.getName()
                        + " has no storage vault.");
                return;
            }
            String localStorageVaultName = localTable.getStorageVaultName();
            if (!localStorageVaultName.equals(storageVaultName)) {
                // currently we only support unique storage vault name in one restore job
                status = new Status(Status.ErrCode.COMMON_ERROR,
                        "local table " + localTable.getName() + " storage vault is " + localStorageVaultName
                                + ", but restore job storage vault is " + storageVaultName);
            }
        }
    }

    @Override
    public void doCreateReplicas() {
        try {
            handleMetaObject(MetaSeriviceOperation.PREPARE);
            // send create tablets requests
            boolean needSetStorageVault = ((CloudEnv) Env.getCurrentEnv()).getEnableStorageVault();
            for (Map.Entry<OlapTable, Cloud.CreateTabletsRequest.Builder> entry : tabletsPerTable.entrySet()) {
                OlapTable table = entry.getKey();
                Cloud.CreateTabletsRequest.Builder requestBuilder = entry.getValue();
                Cloud.CreateTabletsResponse resp = sendCreateTabletsRequests(requestBuilder, table,
                        needSetStorageVault);
                if (resp.hasStorageVaultId()) {
                    storageVaultId = resp.getStorageVaultId();
                    needSetStorageVault = false;
                }
            }
            // set storage vault for new restoring table
            if (((CloudEnv) Env.getCurrentEnv()).getEnableStorageVault()) {
                for (Table table : restoredTbls) {
                    if (table.getType() == TableIf.TableType.OLAP) {
                        OlapTable olapTable = (OlapTable) table;
                        if (olapTable.getStorageVaultId().isEmpty() && storageVaultId != null) {
                            olapTable.setStorageVaultId(storageVaultId);
                        }
                    }
                }
            }
        } catch (Exception e) {
            status = new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        } finally {
            tabletsPerTable.clear();
        }
    }

    @Override
    public void waitingAllReplicasCreated() {
        try {
            handleMetaObject(MetaSeriviceOperation.COMMIT);
        } catch (Exception e) {
            status = new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("finished to create all restored replicas. {}", this);
        }
        allReplicasCreated();
    }

    public void waitingAllSnapshotsFinished() {
        snapshotFinishedTime = System.currentTimeMillis();
        state = RestoreJobState.DOWNLOAD;
        env.getEditLog().logRestoreJob(this);
        LOG.info("finished making snapshots. {}", this);
    }

    @Override
    protected void prepareAndSendSnapshotTaskForOlapTable(Database db) {
        LOG.info("begin to make snapshot. {} when restore content is ALL", this);
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        db.readLock();
        try {
            for (Map.Entry<IdChain, IdChain> entry : fileMapping.getMapping()
                    .entrySet()) {
                IdChain idChain = entry.getKey();
                OlapTable tbl = (OlapTable) db.getTableNullable(idChain.getTblId());
                tbl.readLock();
                try {
                    Partition part = tbl.getPartition(idChain.getPartId());
                    MaterializedIndex index = part.getIndex(idChain.getIdxId());
                    CloudTablet tablet = (CloudTablet) index.getTablet(idChain.getTabletId());
                    Preconditions.checkState(tablet.getReplicas().size() == 1);
                    CloudReplica replica = (CloudReplica) tablet.getReplicaById(idChain.getReplicaId());
                    // Hash snapshot info to be(s) in cluster
                    long backendId = replica.hashReplicaToBe(cloudClusterId, false);
                    // cloud restore job does not need to send snapshot task to be
                    SnapshotInfo info = new SnapshotInfo(db.getId(), tbl.getId(), part.getId(), index.getId(),
                            tablet.getId(), backendId, tbl.getSchemaHashByIndexId(index.getId()), storageVaultId);
                    snapshotInfos.put(tablet.getId(), backendId, info);
                } finally {
                    tbl.readUnlock();
                }
            }
        } catch (Exception e) {
            LOG.error("failed to make snapshot for {}", this, e);
            status = new Status(Status.ErrCode.COMMON_ERROR, "failed to make snapshot, errMsg:"
                    + e.getMessage());
        } finally {
            db.readUnlock();
        }
        LOG.info("finished to send snapshot tasks, num: {}. {}", 0, this);
    }

    @Override
    protected DownloadTask createDownloadTask(long beId, long signature, long jobId, long dbId,
                                              Map<String, String> srcToDest, FsBroker brokerAddr) {
        if (Strings.isNullOrEmpty(storageVaultId)) {
            storageVaultId = snapshotInfos.values().iterator().next().getStorageVaultId();
        }
        Preconditions.checkState(storageVaultId != null, "Storage vault ID cannot be null");
        return new DownloadTask(null, beId, signature, jobId, dbId, srcToDest,
            brokerAddr, repo.getRemoteFileSystem().getStorageProperties().getBackendConfigProperties(),
            repo.getRemoteFileSystem().getStorageType(), repo.getLocation(), storageVaultId);
    }

    public void downloadLocalSnapshots() {
        status = new Status(Status.ErrCode.COMMON_ERROR, "currently not support cloud mode");
    }

    @Override
    protected void cleanMetaObjects(boolean isReplay) {
        super.cleanMetaObjects(isReplay);
        try {
            handleMetaObject(MetaSeriviceOperation.DROP);
        } catch (Exception e) {
            status = new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
    }

    @Override
    public Partition resetPartitionForRestore(OlapTable localTbl, OlapTable remoteTbl, String partName,
                                                 ReplicaAllocation replicaAlloc) {
        Partition restoredPart = super.resetPartitionForRestore(localTbl, remoteTbl, partName, replicaAlloc);
        // convert partition to cloud partition
        CloudPartition cloudPartition = CopyUtil.copyToChild(restoredPart, CloudPartition.class);
        if (cloudPartition != null) {
            cloudPartition.setTableId(localTbl.getId());
            cloudPartition.setDbId(localTbl.getDatabase().getId());
        }
        return cloudPartition;
    }

    @Override
    public Partition resetTabletForRestore(OlapTable localTbl, OlapTable remoteTbl, Partition remotePart,
                                              ReplicaAllocation replicaAlloc) {
        // tablets
        long partitionId = remotePart.getId();
        long visibleVersion = remotePart.getVisibleVersion();
        for (MaterializedIndex remoteIdx : remotePart.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
            int schemaHash = remoteTbl.getSchemaHashByIndexId(remoteIdx.getId());
            int remotetabletSize = remoteIdx.getTablets().size();
            remoteIdx.clearTabletsForRestore();
            for (int i = 0; i < remotetabletSize; i++) {
                // generate new tablet id
                long newTabletId = env.getNextId();
                Tablet newTablet = EnvFactory.getInstance().createTablet(newTabletId);
                // add tablet to index, but not add to TabletInvertedIndex
                remoteIdx.addTablet(newTablet, null /* tablet meta */, true /* is restore */);
                // replicas
                long newReplicaId = Env.getCurrentEnv().getNextId();
                Replica replica = new CloudReplica(newReplicaId, null, Replica.ReplicaState.NORMAL,
                        visibleVersion, schemaHash, dbId, localTbl.getId(), partitionId, remoteIdx.getId(), i);
                newTablet.addReplica(replica, true /* is restore */);
            }
        }
        return remotePart;
    }

    @Override
    public void createReplicas(Database db, OlapTable localTbl, Partition restorePart) {
        createReplicas(db, localTbl, restorePart, null);
    }

    @Override
    public void createReplicas(Database db, OlapTable localTbl, Partition restorePart,
                               Map<Long, TabletRef> tabletBases) {
        List<String> rowStoreColumns = localTbl.getTableProperty().getCopiedRowStoreColumns();
        Cloud.CreateTabletsRequest.Builder requestBuilder = tabletsPerTable.computeIfAbsent(localTbl,
                r -> Cloud.CreateTabletsRequest.newBuilder());

        for (MaterializedIndex restoredIdx : restorePart.getMaterializedIndices(MaterializedIndex.IndexExtState
                .VISIBLE)) {
            MaterializedIndexMeta indexMeta = localTbl.getIndexMetaByIndexId(restoredIdx.getId());
            List<Index> indexes = restoredIdx.getId() == localTbl.getBaseIndexId()
                    ? localTbl.getCopiedIndexes() : null;
            for (Tablet restoreTablet : restoredIdx.getTablets()) {
                try {
                    requestBuilder.addTabletMetas(((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                            .createTabletMetaBuilder(localTbl.getId(), restoredIdx.getId(),
                                restorePart.getId(), restoreTablet,
                                localTbl.getPartitionInfo().getTabletType(restorePart.getId()),
                                indexMeta.getSchemaHash(), indexMeta.getKeysType(),
                                indexMeta.getShortKeyColumnCount(), localTbl.getCopiedBfColumns(),
                                localTbl.getBfFpp(), indexes, indexMeta.getSchema(), localTbl.getDataSortInfo(),
                                localTbl.getCompressionType(), localTbl.getStoragePolicy(),
                                localTbl.isInMemory(), false, localTbl.getName(), localTbl.getTTLSeconds(),
                                localTbl.getEnableUniqueKeyMergeOnWrite(), localTbl.storeRowColumn(),
                                localTbl.getBaseSchemaVersion(), localTbl.getCompactionPolicy(),
                                localTbl.getTimeSeriesCompactionGoalSizeMbytes(),
                                localTbl.getTimeSeriesCompactionFileCountThreshold(),
                                localTbl.getTimeSeriesCompactionTimeThresholdSeconds(),
                                localTbl.getTimeSeriesCompactionEmptyRowsetsThreshold(),
                                localTbl.getTimeSeriesCompactionLevelThreshold(), localTbl.disableAutoCompaction(),
                                localTbl.getRowStoreColumnsUniqueIds(rowStoreColumns),
                                localTbl.getEnableMowLightDelete(),
                                localTbl.getInvertedIndexFileStorageFormat(),
                                localTbl.rowStorePageSize(),
                                localTbl.variantEnableFlattenNested(),
                                localTbl.storagePageSize(),
                                localTbl.storageDictPageSize(), false));
                    // In cloud mode all storage medium will be saved to HDD.
                    TabletMeta tabletMeta = new TabletMeta(db.getId(), localTbl.getId(), restorePart.getId(),
                            restoredIdx.getId(), indexMeta.getSchemaHash(), TStorageMedium.HDD);
                    Env.getCurrentInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
                    Env.getCurrentInvertedIndex().addReplica(restoreTablet.getId(),
                            restoreTablet.getReplicaByBackendId(-1));
                } catch (Exception e) {
                    String errMsg = String.format("create tablet meta builder failed, errMsg:%s, local table:%d, "
                            + "restore partition=%d, restore index=%d, restore tablet=%d", e.getMessage(),
                            localTbl.getId(), restorePart.getId(), restoredIdx.getId(), restoreTablet.getId());
                    status = new Status(Status.ErrCode.COMMON_ERROR, errMsg);
                }
            }
        }
    }

    private void handleMetaObject(MetaSeriviceOperation operation) throws DdlException {
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            throw new DdlException("database " + dbId + " does not exist");
        }
        // 1. for restoring tables for remote tables
        for (Table table : restoredTbls) {
            if (table.getType() == TableIf.TableType.OLAP) {
                OlapTable olapTable = (OlapTable) table;
                handleOlapTableMeta(operation, olapTable, olapTable.getAllPartitions());
            }
        }
        // 2. for restoring partitions for local tables
        // group partition ids by local table
        Map<String, Collection<Partition>> localTableToPartitions = new HashMap<>();
        for (Pair<String, Partition> entry : restoredPartitions) {
            localTableToPartitions.computeIfAbsent(entry.first, k -> Lists.newArrayList())
                    .add(entry.second);
        }
        for (Map.Entry<String, Collection<Partition>> entry : localTableToPartitions.entrySet()) {
            OlapTable localTbl = (OlapTable) db.getTableOrDdlException(entry.getKey());
            handleOlapTableMeta(operation, localTbl, entry.getValue());
        }
    }

    private void handleOlapTableMeta(MetaSeriviceOperation operation, OlapTable olapTable,
                                     Collection<Partition> partitions) throws DdlException {
        List<Long> partitionIds = new ArrayList<>();
        switch (operation) {
            case PREPARE: {
                List<Long> visibleVersions = new ArrayList<>();
                partitions.forEach(partition -> {
                    visibleVersions.add(partition.getCachedVisibleVersion());
                    partitionIds.add(partition.getId());
                });
                preparePartitions(olapTable, partitionIds, visibleVersions);
                break;
            }
            case COMMIT: {
                partitions.forEach(partition -> {
                    partitionIds.add(partition.getId());
                });
                commitPartitions(olapTable, partitionIds);
                break;
            }
            case DROP: {
                partitions.forEach(partition -> {
                    partitionIds.add(partition.getId());
                });
                dropPartitions(olapTable, partitionIds);
                break;
            }
            default:
                throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
    }

    private void preparePartitions(OlapTable olapTable, List<Long> partitionIds, List<Long> visibleVersions)
            throws DdlException {
        Preconditions.checkState(partitionIds.size() == visibleVersions.size(),
                "partitionIds and visibleVersions size not equal");
        try {
            ((CloudInternalCatalog) Env.getCurrentInternalCatalog()).preparePartition(
                    dbId, olapTable.getId(), partitionIds, olapTable.getIndexIdList(), visibleVersions);
        } catch (Exception e) {
            String errMsg = String.format("cloud restore job failed to prepare partitions, table=%s, "
                        + "partitions=%s, errMsg: %s", olapTable.getName(), partitionIds, e.getMessage());
            throw new DdlException(errMsg);
        }
        LOG.info("cloud restore job prepare partitions, dbId: {}, tableName: {}, partitions: {},"
                + " vault name: {}", dbId, olapTable.getName(), partitionIds, storageVaultName);
    }

    private void commitPartitions(OlapTable olapTable, List<Long> partitionIds) throws DdlException {
        try {
            ((CloudInternalCatalog) Env.getCurrentInternalCatalog()).commitPartition(
                    dbId, olapTable.getId(), partitionIds, olapTable.getIndexIdList());
        } catch (Exception e) {
            String errMsg = String.format("cloud restore job failed to commit partitions, table=%s, "
                    + "partitions=%s, errMsg: %s", olapTable.getName(), partitionIds, e.getMessage());
            throw new DdlException(errMsg);
        }
        LOG.info("cloud restore job commit partitions, dbId: {}, tableName: {}, partitions: {},"
                + " vault name: {}", dbId, olapTable.getName(), partitionIds, storageVaultName);
    }

    private void dropPartitions(OlapTable olapTable, List<Long> partitionIds) throws DdlException {
        try {
            ((CloudInternalCatalog) Env.getCurrentInternalCatalog()).dropCloudPartition(
                    dbId, olapTable.getId(), partitionIds, olapTable.getIndexIdList(), false);
        } catch (Exception e) {
            String errMsg = String.format("cloud restore job failed to drop partitions, table=%s, "
                    + "partitions=%s, errMsg: %s", olapTable.getName(), partitionIds, e.getMessage());
            throw new DdlException(errMsg);
        }
        LOG.info("cloud restore job drop partitions, dbId: {}, tableName: {}, partitions: {},"
                + " vault name: {}", dbId, olapTable.getName(), partitionIds, storageVaultName);
    }

    private Cloud.CreateTabletsResponse sendCreateTabletsRequests(Cloud.CreateTabletsRequest.Builder requestBuilder,
                                                                  OlapTable olapTable, boolean needSetStorageVault)
            throws DdlException {
        if (needSetStorageVault && ((CloudEnv) Env.getCurrentEnv()).getEnableStorageVault()) {
            requestBuilder.setStorageVaultName(storageVaultName);
        }
        requestBuilder.setDbId(dbId);
        Cloud.CreateTabletsResponse resp;
        try {
            resp = ((CloudInternalCatalog) Env.getCurrentInternalCatalog()).sendCreateTabletsRpc(requestBuilder);
        } catch (Exception e) {
            String errMsg = String.format("cloud restore job restore tablets failed, dbId=%d, tableName=%s, "
                    + "vault name=%s, errMsg=%s", dbId, olapTable.getName(), storageVaultName, e.getMessage());
            throw new DdlException(errMsg);
        }
        LOG.info("cloud restore job restore tablets, dbId: {}, tableName: {}, vault name: {}", dbId,
                olapTable.getName(), storageVaultName);
        return resp;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        storageVaultName = properties.get(PROP_STORAGE_VAULT_NAME);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(", storage vault name: ").append(storageVaultName);
        sb.append(", compute cluster: ").append(cloudClusterName);
        return sb.toString();
    }
}
