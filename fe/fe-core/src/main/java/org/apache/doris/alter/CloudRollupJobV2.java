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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TTabletType;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CloudRollupJobV2 extends RollupJobV2 {
    private static final Logger LOG = LogManager.getLogger(CloudRollupJobV2.class);

    public static AlterJobV2 buildCloudRollupJobV2(RollupJobV2 job) throws IllegalAccessException, AnalysisException {
        CloudRollupJobV2 ret = new CloudRollupJobV2();
        List<Field> allFields = new ArrayList<>();
        Class tmpClass = RollupJobV2.class;
        while (tmpClass != null) {
            allFields.addAll(Arrays.asList(tmpClass.getDeclaredFields()));
            tmpClass = tmpClass.getSuperclass();
        }
        for (Field field : allFields) {
            field.setAccessible(true);
            Annotation annotation = field.getAnnotation(SerializedName.class);
            if (annotation != null) {
                field.set(ret, field.get(job));
            }
        }
        ret.initAnalyzer();
        return ret;
    }

    private CloudRollupJobV2() {}

    // Don't call it directly, use AlterJobV2Factory to replace
    public CloudRollupJobV2(String rawSql, long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                       long baseIndexId,
                       long rollupIndexId, String baseIndexName, String rollupIndexName, List<Column> rollupSchema,
                       Column whereColumn,
                       int baseSchemaHash, int rollupSchemaHash, KeysType rollupKeysType,
                       short rollupShortKeyColumnCount,
                       OriginStatement origStmt) throws AnalysisException {
        super(rawSql, jobId, dbId, tableId, tableName, timeoutMs, baseIndexId,
                rollupIndexId, baseIndexName, rollupIndexName, rollupSchema, whereColumn,
                baseSchemaHash, rollupSchemaHash, rollupKeysType, rollupShortKeyColumnCount, origStmt);
        ConnectContext context = ConnectContext.get();
        if (context != null) {
            String clusterName = context.getCloudCluster();
            LOG.debug("rollup job add cloud cluster, context not null, cluster: {}", clusterName);
            if (!Strings.isNullOrEmpty(clusterName)) {
                setCloudClusterName(clusterName);
            }
        }
        LOG.debug("rollup job add cloud cluster, context {}", context);
    }

    @Override
    protected void onCreateRollupReplicaDone() throws AlterCancelException {
        List<Long> rollupIndexList = new ArrayList<Long>();
        rollupIndexList.add(rollupIndexId);
        try {
            ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                .commitMaterializedIndex(dbId, tableId, rollupIndexList, false);
        } catch (Exception e) {
            LOG.warn("commitMaterializedIndex Exception:{}", e);
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("onCreateRollupReplicaDone finished, dbId:{}, tableId:{}, jobId:{}, rollupIndexList:{}",
                dbId, tableId, jobId, rollupIndexList);
    }

    @Override
    protected void onCancel() {
        List<Long> rollupIndexList = new ArrayList<Long>();
        rollupIndexList.add(rollupIndexId);
        long tryTimes = 1;
        while (true) {
            try {
                ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                    .dropMaterializedIndex(tableId, rollupIndexList, false);
                for (Map.Entry<Long, Map<Long, Long>> partitionEntry : partitionIdToBaseRollupTabletIdMap.entrySet()) {
                    Long partitionId = partitionEntry.getKey();
                    Map<Long, Long> rollupTabletIdToBaseTabletId = partitionEntry.getValue();
                    for (Map.Entry<Long, Long> tabletEntry : rollupTabletIdToBaseTabletId.entrySet()) {
                        Long rollupTabletId = tabletEntry.getKey();
                        Long baseTabletId = tabletEntry.getValue();
                        ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                                .removeSchemaChangeJob(dbId, tableId, baseIndexId, rollupIndexId,
                                    partitionId, baseTabletId, rollupTabletId);
                    }
                    LOG.info("Cancel RollupJob. Remove SchemaChangeJob in ms."
                            + "dbId:{}, tableId:{}, rollupIndexId: {} partitionId:{}. tabletSize:{}",
                            dbId, tableId, rollupIndexId, partitionId, rollupTabletIdToBaseTabletId.size());
                }
                break;
            } catch (Exception e) {
                LOG.warn("tryTimes:{}, onCancel exception:", tryTimes, e);
            }
            sleepSeveralSeconds();
            tryTimes++;
        }

        LOG.info("onCancel finished, dbId:{}, tableId:{}, jobId:{}, rollupIndexList:{}",
                dbId, tableId, jobId, rollupIndexList);
    }

    @Override
    protected void createRollupReplica() throws AlterCancelException {
        Database db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbId, s -> new AlterCancelException("Database " + s + " does not exist"));

        // 1. create rollup replicas
        OlapTable tbl;
        try {
            tbl = (OlapTable) db.getTableOrMetaException(tableId, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new AlterCancelException(e.getMessage());
        }

        long expiration = (createTimeMs + timeoutMs) / 1000;
        tbl.readLock();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
            try {
                List<Long> rollupIndexList = new ArrayList<Long>();
                rollupIndexList.add(rollupIndexId);
                ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                    .prepareMaterializedIndex(tbl.getId(), rollupIndexList, expiration);
                createRollupReplicaForPartition(tbl);
            } catch (Exception e) {
                LOG.warn("createCloudShadowIndexReplica Exception:{}", e);
                throw new AlterCancelException(e.getMessage());
            }
        } finally {
            tbl.readUnlock();
        }

        // create all rollup replicas success.
        // add rollup index to catalog
        tbl.writeLockOrAlterCancelException();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
            addRollupIndexToCatalog(tbl);
        } finally {
            tbl.writeUnlock();
        }
    }

    private void createRollupReplicaForPartition(OlapTable tbl) throws Exception {
        for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
            long partitionId = entry.getKey();
            Partition partition = tbl.getPartition(partitionId);
            if (partition == null) {
                continue;
            }
            TTabletType tabletType = tbl.getPartitionInfo().getTabletType(partitionId);
            MaterializedIndex rollupIndex = entry.getValue();
            Cloud.CreateTabletsRequest.Builder requestBuilder =
                    Cloud.CreateTabletsRequest.newBuilder();
            List<String> rowStoreColumns =
                                        tbl.getTableProperty().getCopiedRowStoreColumns();
            for (Tablet rollupTablet : rollupIndex.getTablets()) {
                OlapFile.TabletMetaCloudPB.Builder builder =
                        ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                            .createTabletMetaBuilder(tableId, rollupIndexId,
                            partitionId, rollupTablet, tabletType, rollupSchemaHash,
                                    rollupKeysType, rollupShortKeyColumnCount, tbl.getCopiedBfColumns(),
                                    tbl.getBfFpp(), null, rollupSchema,
                                    tbl.getDataSortInfo(), tbl.getCompressionType(), tbl.getStoragePolicy(),
                                    tbl.isInMemory(), true,
                                    tbl.getName(), tbl.getTTLSeconds(),
                                    tbl.getEnableUniqueKeyMergeOnWrite(), tbl.storeRowColumn(),
                                    tbl.getBaseSchemaVersion(), tbl.getCompactionPolicy(),
                                    tbl.getTimeSeriesCompactionGoalSizeMbytes(),
                                    tbl.getTimeSeriesCompactionFileCountThreshold(),
                                    tbl.getTimeSeriesCompactionTimeThresholdSeconds(),
                                    tbl.getTimeSeriesCompactionEmptyRowsetsThreshold(),
                                    tbl.getTimeSeriesCompactionLevelThreshold(),
                                    tbl.disableAutoCompaction(),
                                    tbl.getRowStoreColumnsUniqueIds(rowStoreColumns),
                                    tbl.getEnableMowLightDelete(), null,
                                    tbl.rowStorePageSize(),
                                    tbl.variantEnableFlattenNested());
                requestBuilder.addTabletMetas(builder);
            } // end for rollupTablets
            requestBuilder.setDbId(dbId);
            ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                    .sendCreateTabletsRpc(requestBuilder);
        }
    }

    @Override
    protected void ensureCloudClusterExist(List<AgentTask> tasks) throws AlterCancelException {
        if (((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getCloudClusterIdByName(cloudClusterName) == null) {
            for (AgentTask task : tasks) {
                task.setFinished(true);
                AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.ALTER, task.getSignature());
            }
            StringBuilder sb = new StringBuilder("cloud cluster(");
            sb.append(cloudClusterName);
            sb.append(") has been removed, jobId=");
            sb.append(jobId);
            String msg = sb.toString();
            LOG.warn(msg);
            throw new AlterCancelException(msg);
        }
    }

    @Override
    protected boolean checkTableStable(Database db) throws AlterCancelException {
        return true;
    }
}
