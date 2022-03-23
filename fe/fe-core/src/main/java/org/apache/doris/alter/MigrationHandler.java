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

package org.apache.doris.alter;

import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.CancelStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.RemoveAlterJobV2OperationLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MigrationHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(MigrationHandler.class);

    // all shadow indexes should have this prefix in name
    public static final String SHADOW_NAME_PRFIX = "__doris_shadow_";

    public static final int MAX_ACTIVE_MIGRATION_JOB_SIZE = 10;

    public static final int CYCLE_COUNT_TO_CHECK_EXPIRE_MIGRATION_JOB = 20;

    public final ThreadPoolExecutor migrationThreadPool = ThreadPoolManager.newDaemonCacheThreadPool(MAX_ACTIVE_MIGRATION_JOB_SIZE, "migration-pool", true);

    public final Map<Long, AlterJobV2> activeMigrationJob = Maps.newConcurrentMap();

    public final Map<Long, AlterJobV2> runnableMigrationJob = Maps.newConcurrentMap();

    public int cycle_count = 0;

    public MigrationHandler() {
        super("migration", Config.default_schema_change_scheduler_interval_millisecond);
    }

    private void createJob(long dbId, OlapTable olapTable, Collection<Long> partitionIds,
                           TStorageMedium destStorageMedium) throws UserException {
        if (olapTable.getState() == OlapTableState.ROLLUP) {
            throw new DdlException("Table[" + olapTable.getName() + "]'s is doing ROLLUP job");
        }

        if (olapTable.getState() == OlapTableState.SCHEMA_CHANGE) {
            throw new DdlException("Table[" + olapTable.getName() + "]'s is doing SCHEMA_CHANGE job");
        }

        // for now table's state can only be NORMAL
        Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL, olapTable.getState().name());

        // create job
        Catalog catalog = Catalog.getCurrentCatalog();
        long jobId = catalog.getNextId();
        long tableId = olapTable.getId();
        SchemaChangeJobV2 schemaChangeJob = new SchemaChangeJobV2(
                jobId, dbId, tableId, olapTable.getName(), Config.migration_timeout_second * 1000);

        schemaChangeJob.setStorageFormat(olapTable.getStorageFormat());
        schemaChangeJob.setJobType(AlterJobV2.JobType.MIGRATION);

        for (Map.Entry<Long, List<Column>> entry : olapTable.getIndexIdToSchema(true).entrySet()) {
            long originIndexId = entry.getKey();
            MaterializedIndexMeta currentIndexMeta = olapTable.getIndexMetaByIndexId(originIndexId);
            // 1. get new schema version/schema version hash, short key column count
            int currentSchemaVersion = currentIndexMeta.getSchemaVersion();
            int newSchemaVersion = currentSchemaVersion + 1;
            // generate schema hash for new index has to generate a new schema hash not equal to current schema hash
            int currentSchemaHash = currentIndexMeta.getSchemaHash();
            int newSchemaHash = Util.generateSchemaHash();
            while (currentSchemaHash == newSchemaHash) {
                newSchemaHash = Util.generateSchemaHash();
            }
            String newIndexName = SHADOW_NAME_PRFIX + olapTable.getIndexNameById(originIndexId);
            short shortKeyColumnCount = Catalog.calcShortKeyColumnCount(entry.getValue(), null);
            long shadowIndexId = catalog.getNextId();

            // create SHADOW index for each partition
            List<Tablet> addedTablets = Lists.newArrayList();
            for (Partition partition : olapTable.getPartitions()) {
                long partitionId = partition.getId();
                if (!partitionIds.contains(partitionId)) {
                    continue;
                }
                DataProperty dataProperty = olapTable.getPartitionInfo().getDataProperty(partitionId);
                if (dataProperty.getMigrationState() != DataProperty.MigrationState.RUNNING) {
                    throw new DdlException("partition " + partitionId + " migration state is invalid: " +
                            dataProperty.getMigrationState().name());
                }
                // index state is SHADOW
                MaterializedIndex shadowIndex = new MaterializedIndex(shadowIndexId, IndexState.SHADOW);
                MaterializedIndex originIndex = partition.getIndex(originIndexId);
                TabletMeta shadowTabletMeta = new TabletMeta(
                        dbId, tableId, partitionId, shadowIndexId, newSchemaHash, destStorageMedium);
                ReplicaAllocation replicaAlloc = olapTable.getPartitionInfo().getReplicaAllocation(partitionId);
                Short totalReplicaNum = replicaAlloc.getTotalReplicaNum();
                for (Tablet originTablet : originIndex.getTablets()) {
                    long originTabletId = originTablet.getId();
                    long shadowTabletId = catalog.getNextId();

                    Tablet shadowTablet = new Tablet(shadowTabletId);
                    shadowIndex.addTablet(shadowTablet, shadowTabletMeta);
                    addedTablets.add(shadowTablet);

                    schemaChangeJob.addTabletIdMap(partitionId, shadowIndexId, shadowTabletId, originTabletId);
                    List<Replica> originReplicas = originTablet.getReplicas();

                    int healthyReplicaNum = 0;
                    for (Replica originReplica : originReplicas) {
                        long shadowReplicaId = catalog.getNextId();
                        long backendId = originReplica.getBackendId();

                        if (originReplica.getState() == ReplicaState.CLONE
                                || originReplica.getState() == ReplicaState.DECOMMISSION
                                || originReplica.getLastFailedVersion() > 0) {
                            LOG.info("origin replica {} of tablet {} state is {}, and last failed version is {}, skip creating shadow replica",
                                    originReplica.getId(), originReplica, originReplica.getState(), originReplica.getLastFailedVersion());
                            continue;
                        }
                        Preconditions.checkState(originReplica.getState() == ReplicaState.NORMAL, originReplica.getState());
                        // replica's init state is ALTER, so that tablet report process will ignore its report
                        Replica shadowReplica = new Replica(shadowReplicaId, backendId, ReplicaState.ALTER,
                                Partition.PARTITION_INIT_VERSION, Partition.PARTITION_INIT_VERSION_HASH,
                                newSchemaHash);
                        shadowTablet.addReplica(shadowReplica);
                        healthyReplicaNum++;
                    }

                    if (healthyReplicaNum < totalReplicaNum / 2 + 1) {
                        /*
                         * TODO(cmy): This is a bad design.
                         * Because in the schema change job, we will only send tasks to the shadow replicas that have been created,
                         * without checking whether the quorum of replica number are satisfied.
                         * This will cause the job to fail until we find that the quorum of replica number
                         * is not satisfied until the entire job is done.
                         * So here we check the replica number strictly and do not allow to submit the job
                         * if the quorum of replica number is not satisfied.
                         */
                        for (Tablet tablet : addedTablets) {
                            Catalog.getCurrentInvertedIndex().deleteTablet(tablet.getId());
                        }
                        throw new DdlException(
                                "tablet " + originTabletId + " has few healthy replica: " + healthyReplicaNum);
                    }
                }

                schemaChangeJob.addPartitionShadowIndex(partitionId, shadowIndexId, shadowIndex);
            } // end for partition
            schemaChangeJob.addIndexSchema(shadowIndexId, originIndexId, newIndexName, newSchemaVersion,
                    newSchemaHash, shortKeyColumnCount, new LinkedList<>(entry.getValue()));
        } // end for index

        // set table state
        olapTable.setState(OlapTableState.SCHEMA_CHANGE);

        // 2. add schemaChangeJob
        addMigrationJob(schemaChangeJob);

        // 3. write edit log
        Catalog.getCurrentCatalog().getEditLog().logAlterJob(schemaChangeJob);
        LOG.info("finished to create migration job: {}", schemaChangeJob.getJobId());
    }

    @Override
    protected void runAfterCatalogReady() {
        if (cycle_count >= CYCLE_COUNT_TO_CHECK_EXPIRE_MIGRATION_JOB) {
            clearFinishedOrCancelledMigrationJob();
            super.runAfterCatalogReady();
            cycle_count = 0;
        }
        createMigrationJobs();
        runMigrationJob();
        cycle_count++;
    }

    private void runMigrationJob() {
        runnableMigrationJob.values().forEach(
                migrationJob -> {
                    if (!migrationJob.isDone() && !activeMigrationJob.containsKey(migrationJob.getJobId()) &&
                            activeMigrationJob.size() < MAX_ACTIVE_MIGRATION_JOB_SIZE) {
                        if (FeConstants.runningUnitTest) {
                            migrationJob.run();
                        } else {
                            migrationThreadPool.submit(() -> {
                                if (activeMigrationJob.putIfAbsent(migrationJob.getJobId(), migrationJob) == null) {
                                    try {
                                        migrationJob.run();
                                    } finally {
                                        activeMigrationJob.remove(migrationJob.getJobId());
                                    }
                                }
                            });
                        }
                    }
                });
    }

    public void createMigrationJobs() {
        HashMap<Long, HashMap<Long, Multimap<TStorageMedium, Long>>> changedPartitionsMap = new HashMap<>();
        long currentTimeMs = System.currentTimeMillis();
        List<Long> dbIds = Catalog.getCurrentCatalog().getDbIds();

        for (long dbId : dbIds) {
            Database db = Catalog.getCurrentCatalog().getDbNullable(dbId);
            if (db == null) {
                LOG.warn("db {} does not exist while doing backend report", dbId);
                continue;
            }
            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (table.getType() != Table.TableType.OLAP) {
                    continue;
                }

                long tableId = table.getId();
                OlapTable olapTable = (OlapTable) table;
                olapTable.readLock();
                try {
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                    for (Partition partition : olapTable.getAllPartitions()) {
                        long partitionId = partition.getId();
                        DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
                        Preconditions.checkNotNull(dataProperty, partition.getName() + ", pId:"
                                + partitionId + ", db: " + dbId + ", tbl: " + tableId);
                        if (dataProperty.getStorageMedium() != dataProperty.getStorageColdMedium()
                                && dataProperty.getCooldownTimeMs() < currentTimeMs
                                && dataProperty.getMigrationState() == DataProperty.MigrationState.NONE) {
                            if (!changedPartitionsMap.containsKey(dbId)) {
                                changedPartitionsMap.put(dbId, new HashMap<>());
                            }
                            HashMap<Long, Multimap<TStorageMedium, Long>> tableMultiMap = changedPartitionsMap.get(dbId);
                            if (!tableMultiMap.containsKey(tableId)) {
                                tableMultiMap.put(tableId, HashMultimap.create());
                            }
                            Multimap<TStorageMedium, Long> multimap = tableMultiMap.get(tableId);
                            multimap.put(dataProperty.getStorageColdMedium(), partitionId);
                        }
                    } // end for partitions
                } finally {
                    olapTable.readUnlock();
                }
            } // end for tables
        } // end for dbs

        // handle data property changed
        for (Long dbId : changedPartitionsMap.keySet()) {
            Database db = Catalog.getCurrentCatalog().getDbNullable(dbId);
            if (db == null) {
                LOG.warn("db {} does not exist while checking backend storage medium", dbId);
                continue;
            }
            HashMap<Long, Multimap<TStorageMedium, Long>> tableIdToStorageMedium = changedPartitionsMap.get(dbId);

            for (Long tableId : tableIdToStorageMedium.keySet()) {
                Table table = db.getTableNullable(tableId);
                if (table == null) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                olapTable.writeLock();
                try {
                    Multimap<TStorageMedium, Long> storageMediumToPartitionIds = tableIdToStorageMedium.get(tableId);
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                    for (TStorageMedium storageMedium : storageMediumToPartitionIds.keySet()) {
                        Collection<Long> partitionIds = storageMediumToPartitionIds.get(storageMedium);
                        try {
                            for (Long partitionId : partitionIds) {
                                Partition partition = olapTable.getPartition(partitionId);
                                DataProperty dataProperty = partitionInfo.getDataProperty(partitionId);
                                if (partition == null || dataProperty == null) {
                                    continue;
                                }
                                dataProperty.setMigrationState(DataProperty.MigrationState.RUNNING);
                                LOG.info("partition[{}-{}-{}] storage medium changed from {} to {}",
                                        dbId, tableId, partitionId, dataProperty.getStorageMedium(),
                                        dataProperty.getStorageColdMedium());
                            } // end for partitions
                            createJob(db.getId(), olapTable, partitionIds, storageMedium);
                        } catch (Exception e) {
                            for (Long partitionId : partitionIds) {
                                Partition partition = olapTable.getPartition(partitionId);
                                DataProperty dataProperty = partitionInfo.getDataProperty(partitionId);
                                if (partition == null || dataProperty == null) {
                                    continue;
                                }
                                dataProperty.setMigrationState(DataProperty.MigrationState.NONE);
                            }
                            LOG.error("create migration job failed. tableName: {}", olapTable.getName(), e);
                        }
                    }
                } finally {
                    olapTable.writeUnlock();
                }
            } // end for tables
        } // end for dbs
    }

    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        List<List<Comparable>> schemaChangeJobInfos = new LinkedList<>();
        getAlterJobV2Infos(db, schemaChangeJobInfos);

        // sort by "JobId", "PartitionName", "CreateTime", "FinishTime", "IndexName", "IndexState"
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2, 3, 4, 5);
        schemaChangeJobInfos.sort(comparator);
        return schemaChangeJobInfos;
    }

    private void getAlterJobV2Infos(Database db, List<AlterJobV2> alterJobsV2, List<List<Comparable>> schemaChangeJobInfos) {
        ConnectContext ctx = ConnectContext.get();
        for (AlterJobV2 alterJob : alterJobsV2) {
            if (alterJob.getDbId() != db.getId()) {
                continue;
            }
            if (ctx != null) {
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ctx, db.getFullName(), alterJob.getTableName(), PrivPredicate.ALTER)) {
                    continue;
                }
            }
            alterJob.getInfo(schemaChangeJobInfos);
        }
    }

    private void getAlterJobV2Infos(Database db, List<List<Comparable>> schemaChangeJobInfos) {
        getAlterJobV2Infos(db, ImmutableList.copyOf(alterJobsV2.values()), schemaChangeJobInfos);
    }

    @Override
    public void process(List<AlterClause> alterClauses, String clusterName, Database db, OlapTable olapTable) throws UserException {
        throw new DdlException("Table[" + olapTable.getName() + "]'s migration process is not implement");
    }

    @Override
    public void cancel(CancelStmt stmt) throws DdlException {
        throw new DdlException("migration cancel is not implement");
    }

    protected void addMigrationJob(AlterJobV2 alterJob) {
        // super.addAlterJobV2(alterJob);
        runnableMigrationJob.put(alterJob.getJobId(), alterJob);
    }

    private void clearFinishedOrCancelledMigrationJob() {
        Iterator<Map.Entry<Long, AlterJobV2>> iterator = runnableMigrationJob.entrySet().iterator();
        while (iterator.hasNext()) {
            AlterJobV2 alterJobV2 = iterator.next().getValue();
            if (alterJobV2.isDone()) {
                iterator.remove();
            }
        }
    }

    @Override
    public void replayRemoveAlterJobV2(RemoveAlterJobV2OperationLog log) {
        if (runnableMigrationJob.containsKey(log.getJobId())) {
            runnableMigrationJob.remove(log.getJobId());
        }
        super.replayRemoveAlterJobV2(log);
    }

    @Override
    public void replayAlterJobV2(AlterJobV2 alterJob) {
        if (!alterJob.isDone() && !runnableMigrationJob.containsKey(alterJob.getJobId())) {
            runnableMigrationJob.put(alterJob.getJobId(), alterJob);
        }
        super.replayAlterJobV2(alterJob);
    }
}
