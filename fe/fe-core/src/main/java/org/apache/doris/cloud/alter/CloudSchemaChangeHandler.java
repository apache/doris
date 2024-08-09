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

package org.apache.doris.cloud.alter;

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CloudSchemaChangeHandler extends SchemaChangeHandler {
    private static final Logger LOG = LogManager.getLogger(CloudSchemaChangeHandler.class);

    @Override
    public void updatePartitionsProperties(Database db, String tableName, List<String> partitionNames,
                                           Map<String, String> properties) throws DdlException, MetaNotFoundException {
        Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS));

        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);
        if (properties.size() != 1) {
            throw new DdlException("Can only set one partition property at a time");
        }

        UpdatePartitionMetaParam param = new UpdatePartitionMetaParam();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS)) {
            long ttlSeconds = Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS));
            olapTable.readLock();
            try {
                if (ttlSeconds == olapTable.getTTLSeconds()) {
                    LOG.info("ttlSeconds:{} is equal with olapTable.getTTLSeconds():{}", ttlSeconds,
                            olapTable.getTTLSeconds());
                    return;
                }
            } finally {
                olapTable.readUnlock();
            }
            param.ttlSeconds = ttlSeconds;
            param.type = UpdatePartitionMetaParam.TabletMetaType.TTL_SECONDS;
        } else {
            LOG.warn("invalid properties:{}", properties);
            throw new DdlException("invalid properties");
        }

        for (String partitionName : partitionNames) {
            try {
                updateCloudPartitionMeta(db, olapTable.getName(), partitionName, param);
            } catch (Exception e) {
                LOG.warn("tableName:{}, partitionNames:{} updateCloudPartitionsProperties exception:",
                        tableName, partitionNames, e);
                throw new DdlException(e.getMessage());
            }
        }
    }

    @Override
    public void updateTableProperties(Database db, String tableName, Map<String, String> properties)
            throws UserException {
        Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_INTERVAL_MS)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_DATA_BYTES)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_LEVEL_THRESHOLD)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_DISABLE_AUTO_COMPACTION)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_MOW_LIGHT_DELETE));

        if (properties.size() != 1) {
            throw new UserException("Can only set one table property at a time");
        }

        List<Partition> partitions = Lists.newArrayList();
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);
        UpdatePartitionMetaParam param = new UpdatePartitionMetaParam();

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS)) {
            long ttlSeconds = PropertyAnalyzer.analyzeTTL(properties);
            olapTable.readLock();
            try {
                if (ttlSeconds == olapTable.getTTLSeconds()) {
                    LOG.info("ttlSeconds:{} is equal with olapTable.getTTLSeconds():{}", ttlSeconds,
                            olapTable.getTTLSeconds());
                    return;
                }
                partitions.addAll(olapTable.getPartitions());
            } finally {
                olapTable.readUnlock();
            }
            param.ttlSeconds = ttlSeconds;
            param.type = UpdatePartitionMetaParam.TabletMetaType.TTL_SECONDS;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_INTERVAL_MS)) {
            long groupCommitIntervalMs = Long.parseLong(properties.get(PropertyAnalyzer
                    .PROPERTIES_GROUP_COMMIT_INTERVAL_MS));
            olapTable.readLock();
            try {
                if (groupCommitIntervalMs == olapTable.getGroupCommitIntervalMs()) {
                    LOG.info("groupCommitIntervalMs:{} is equal with olapTable.getGroupCommitIntervalMs():{}",
                            groupCommitIntervalMs, olapTable.getGroupCommitIntervalMs());
                    return;
                }
                partitions.addAll(olapTable.getPartitions());
            } finally {
                olapTable.readUnlock();
            }
            param.groupCommitIntervalMs = groupCommitIntervalMs;
            param.type = UpdatePartitionMetaParam.TabletMetaType.GROUP_COMMIT_INTERVAL_MS;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_DATA_BYTES)) {
            long groupCommitDataBytes = Long.parseLong(properties.get(PropertyAnalyzer
                    .PROPERTIES_GROUP_COMMIT_DATA_BYTES));
            olapTable.readLock();
            try {
                if (groupCommitDataBytes == olapTable.getGroupCommitDataBytes()) {
                    LOG.info("groupCommitDataBytes:{} is equal with olapTable.getGroupCommitDataBytes():{}",
                            groupCommitDataBytes, olapTable.getGroupCommitDataBytes());
                    return;
                }
                partitions.addAll(olapTable.getPartitions());
            } finally {
                olapTable.readUnlock();
            }
            param.groupCommitDataBytes = groupCommitDataBytes;
            param.type = UpdatePartitionMetaParam.TabletMetaType.GROUP_COMMIT_DATA_BYTES;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY)) {
            String compactionPolicy = properties.get(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY);
            if (compactionPolicy != null
                    && !compactionPolicy.equals(PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY)
                    && !compactionPolicy.equals(PropertyAnalyzer.SIZE_BASED_COMPACTION_POLICY)) {
                throw new UserException("Table compaction policy only support for "
                        + PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY
                        + " or " + PropertyAnalyzer.SIZE_BASED_COMPACTION_POLICY);
            }
            olapTable.readLock();
            try {
                if (compactionPolicy == olapTable.getCompactionPolicy()) {
                    LOG.info("compactionPolicy:{} is equal with olapTable.getCompactionPolicy():{}",
                            compactionPolicy, olapTable.getCompactionPolicy());
                    return;
                }
                partitions.addAll(olapTable.getPartitions());
            } finally {
                olapTable.readUnlock();
            }
            param.compactionPolicy = compactionPolicy;
            param.type = UpdatePartitionMetaParam.TabletMetaType.COMPACTION_POLICY;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES)) {
            long timeSeriesCompactionGoalSizeMbytes = Long.parseLong(properties.get(PropertyAnalyzer
                    .PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES));
            olapTable.readLock();
            try {
                if (timeSeriesCompactionGoalSizeMbytes
                        == olapTable.getTimeSeriesCompactionGoalSizeMbytes()) {
                    LOG.info("timeSeriesCompactionGoalSizeMbytes:{} is equal with"
                                    + " olapTable.timeSeriesCompactionGoalSizeMbytes():{}",
                            timeSeriesCompactionGoalSizeMbytes,
                            olapTable.getTimeSeriesCompactionGoalSizeMbytes());
                    return;
                }
                partitions.addAll(olapTable.getPartitions());
            } finally {
                olapTable.readUnlock();
            }
            param.timeSeriesCompactionGoalSizeMbytes = timeSeriesCompactionGoalSizeMbytes;
            param.type = UpdatePartitionMetaParam.TabletMetaType.TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD)) {
            long timeSeriesCompactionFileCountThreshold = Long.parseLong(properties.get(PropertyAnalyzer
                    .PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD));
            olapTable.readLock();
            try {
                if (timeSeriesCompactionFileCountThreshold
                        == olapTable.getTimeSeriesCompactionFileCountThreshold()) {
                    LOG.info("timeSeriesCompactionFileCountThreshold:{} is equal with"
                                    + " olapTable.getTimeSeriesCompactionFileCountThreshold():{}",
                            timeSeriesCompactionFileCountThreshold,
                            olapTable.getTimeSeriesCompactionFileCountThreshold());
                    return;
                }
                partitions.addAll(olapTable.getPartitions());
            } finally {
                olapTable.readUnlock();
            }
            param.timeSeriesCompactionFileCountThreshold = timeSeriesCompactionFileCountThreshold;
            param.type = UpdatePartitionMetaParam.TabletMetaType.TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS)) {
            long timeSeriesCompactionTimeThresholdSeconds = Long.parseLong(properties.get(PropertyAnalyzer
                    .PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS));
            olapTable.readLock();
            try {
                if (timeSeriesCompactionTimeThresholdSeconds
                        == olapTable.getTimeSeriesCompactionTimeThresholdSeconds()) {
                    LOG.info("timeSeriesCompactionTimeThresholdSeconds:{} is equal with"
                                    + " olapTable.getTimeSeriesCompactionTimeThresholdSeconds():{}",
                            timeSeriesCompactionTimeThresholdSeconds,
                            olapTable.getTimeSeriesCompactionTimeThresholdSeconds());
                    return;
                }
                partitions.addAll(olapTable.getPartitions());
            } finally {
                olapTable.readUnlock();
            }
            param.timeSeriesCompactionTimeThresholdSeconds = timeSeriesCompactionTimeThresholdSeconds;
            param.type = UpdatePartitionMetaParam.TabletMetaType.TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD)) {
            long timeSeriesCompactionEmptyRowsetsThreshold = Long.parseLong(properties.get(PropertyAnalyzer
                    .PROPERTIES_TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD));
            olapTable.readLock();
            try {
                if (timeSeriesCompactionEmptyRowsetsThreshold
                        == olapTable.getTimeSeriesCompactionEmptyRowsetsThreshold()) {
                    LOG.info("timeSeriesCompactionEmptyRowsetsThreshold:{} is equal with"
                                    + " olapTable.getTimeSeriesCompactionEmptyRowsetsThreshold():{}",
                            timeSeriesCompactionEmptyRowsetsThreshold,
                            olapTable.getTimeSeriesCompactionEmptyRowsetsThreshold());
                    return;
                }
                partitions.addAll(olapTable.getPartitions());
            } finally {
                olapTable.readUnlock();
            }
            param.timeSeriesCompactionEmptyRowsetsThreshold = timeSeriesCompactionEmptyRowsetsThreshold;
            param.type = UpdatePartitionMetaParam.TabletMetaType.TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_LEVEL_THRESHOLD)) {
            long timeSeriesCompactionLevelThreshold = Long.parseLong(properties.get(PropertyAnalyzer
                    .PROPERTIES_TIME_SERIES_COMPACTION_LEVEL_THRESHOLD));
            olapTable.readLock();
            try {
                if (timeSeriesCompactionLevelThreshold
                        == olapTable.getTimeSeriesCompactionLevelThreshold()) {
                    LOG.info("timeSeriesCompactionLevelThreshold:{} is equal with"
                                    + " olapTable.getTimeSeriesCompactionLevelThreshold():{}",
                            timeSeriesCompactionLevelThreshold,
                            olapTable.getTimeSeriesCompactionLevelThreshold());
                    return;
                }
                partitions.addAll(olapTable.getPartitions());
            } finally {
                olapTable.readUnlock();
            }
            param.timeSeriesCompactionLevelThreshold = timeSeriesCompactionLevelThreshold;
            param.type = UpdatePartitionMetaParam.TabletMetaType.TIME_SERIES_COMPACTION_LEVEL_THRESHOLD;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DISABLE_AUTO_COMPACTION)) {
            boolean disableAutoCompaction = Boolean.parseBoolean(properties.get(PropertyAnalyzer
                    .PROPERTIES_DISABLE_AUTO_COMPACTION));
            olapTable.readLock();
            try {
                if (disableAutoCompaction
                        == olapTable.disableAutoCompaction()) {
                    LOG.info("disableAutoCompaction:{} is equal with"
                                    + " olapTable.disableAutoCompaction():{}",
                            disableAutoCompaction,
                            olapTable.disableAutoCompaction());
                    return;
                }
                partitions.addAll(olapTable.getPartitions());
            } finally {
                olapTable.readUnlock();
            }
            param.disableAutoCompaction = disableAutoCompaction;
            param.type = UpdatePartitionMetaParam.TabletMetaType.DISABLE_AUTO_COMPACTION;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_MOW_LIGHT_DELETE)) {
            boolean enableMowLightDelete = Boolean.parseBoolean(properties.get(PropertyAnalyzer
                    .PROPERTIES_ENABLE_MOW_LIGHT_DELETE));
            olapTable.readLock();
            try {
                if (enableMowLightDelete
                        == olapTable.getEnableMowLightDelete()) {
                    LOG.info("enableMowLightDelete:{} is equal with"
                                    + " olapTable.getEnableMowLightDelete():{}",
                            enableMowLightDelete,
                            olapTable.getEnableMowLightDelete());
                    return;
                }
                if (!olapTable.getEnableUniqueKeyMergeOnWrite()) {
                    throw new UserException("enable_mow_light_delete property is "
                            + "not supported for unique merge-on-read table");
                }
                partitions.addAll(olapTable.getPartitions());
            } finally {
                olapTable.readUnlock();
            }
            param.enableMowLightDelete = enableMowLightDelete;
            param.type = UpdatePartitionMetaParam.TabletMetaType.ENABLE_MOW_LIGHT_DELETE;
        } else {
            LOG.warn("invalid properties:{}", properties);
            throw new UserException("invalid properties");
        }

        for (Partition partition : partitions) {
            updateCloudPartitionMeta(db, olapTable.getName(), partition.getName(), param);
        }

        olapTable.writeLockOrDdlException();
        try {
            Env.getCurrentEnv().modifyTableProperties(db, olapTable, properties);
        } finally {
            olapTable.writeUnlock();
        }
    }

    private static class UpdatePartitionMetaParam {
        public enum TabletMetaType {
            INMEMORY,
            PERSISTENT,
            TTL_SECONDS,
            GROUP_COMMIT_INTERVAL_MS,
            GROUP_COMMIT_DATA_BYTES,
            COMPACTION_POLICY,
            TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES,
            TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD,
            TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS,
            TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD,
            TIME_SERIES_COMPACTION_LEVEL_THRESHOLD,
            DISABLE_AUTO_COMPACTION,
            ENABLE_MOW_LIGHT_DELETE,
        }

        TabletMetaType type;
        boolean isPersistent = false;
        boolean isInMemory = false;
        long ttlSeconds = 0;
        long groupCommitIntervalMs = 0;
        long groupCommitDataBytes = 0;
        String compactionPolicy;
        long timeSeriesCompactionGoalSizeMbytes = 0;
        long timeSeriesCompactionFileCountThreshold = 0;
        long timeSeriesCompactionTimeThresholdSeconds = 0;
        long timeSeriesCompactionEmptyRowsetsThreshold = 0;
        long timeSeriesCompactionLevelThreshold = 0;
        boolean disableAutoCompaction = false;
        boolean enableMowLightDelete = false;
    }

    public void updateCloudPartitionMeta(Database db,
            String tableName,
            String partitionName,
            UpdatePartitionMetaParam param) throws UserException {
        List<Long> tabletIds = new ArrayList<>();
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);
        olapTable.readLock();
        try {
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException(
                        "Partition[" + partitionName + "] does not exist in table[" + olapTable.getName() + "]");
            }
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    tabletIds.add(tablet.getId());
                }
            }
        } finally {
            olapTable.readUnlock();
        }
        for (int index = 0; index < tabletIds.size();) {
            int nextIndex = tabletIds.size() - index > Config.cloud_txn_tablet_batch_size
                    ? index + Config.cloud_txn_tablet_batch_size
                    : tabletIds.size();
            Cloud.UpdateTabletRequest.Builder requestBuilder = Cloud.UpdateTabletRequest.newBuilder();
            while (index < nextIndex) {
                Cloud.TabletMetaInfoPB.Builder infoBuilder = Cloud.TabletMetaInfoPB.newBuilder();
                infoBuilder.setTabletId(tabletIds.get(index));
                switch (param.type) {
                    case PERSISTENT:
                        infoBuilder.setIsPersistent(param.isPersistent);
                        break;
                    case INMEMORY:
                        infoBuilder.setIsInMemory(param.isInMemory);
                        break;
                    case TTL_SECONDS:
                        infoBuilder.setTtlSeconds(param.ttlSeconds);
                        break;
                    case GROUP_COMMIT_INTERVAL_MS:
                        infoBuilder.setGroupCommitIntervalMs(param.groupCommitIntervalMs);
                        break;
                    case GROUP_COMMIT_DATA_BYTES:
                        infoBuilder.setGroupCommitDataBytes(param.groupCommitDataBytes);
                        break;
                    case COMPACTION_POLICY:
                        infoBuilder.setCompactionPolicy(param.compactionPolicy);
                        break;
                    case TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES:
                        infoBuilder.setTimeSeriesCompactionGoalSizeMbytes(
                                param.timeSeriesCompactionGoalSizeMbytes);
                        break;
                    case TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD:
                        infoBuilder.setTimeSeriesCompactionFileCountThreshold(
                                param.timeSeriesCompactionFileCountThreshold);
                        break;
                    case TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS:
                        infoBuilder.setTimeSeriesCompactionTimeThresholdSeconds(
                                param.timeSeriesCompactionTimeThresholdSeconds);
                        break;
                    case TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD:
                        infoBuilder.setTimeSeriesCompactionEmptyRowsetsThreshold(
                                param.timeSeriesCompactionEmptyRowsetsThreshold);
                        break;
                    case TIME_SERIES_COMPACTION_LEVEL_THRESHOLD:
                        infoBuilder.setTimeSeriesCompactionLevelThreshold(
                                param.timeSeriesCompactionLevelThreshold);
                        break;
                    case DISABLE_AUTO_COMPACTION:
                        infoBuilder.setDisableAutoCompaction(
                                param.disableAutoCompaction);
                        break;
                    case ENABLE_MOW_LIGHT_DELETE:
                        infoBuilder.setEnableMowLightDelete(
                                param.enableMowLightDelete
                        );
                        break;
                    default:
                        throw new UserException("Unknown TabletMetaType");
                }
                Cloud.TabletMetaInfoPB tabletMetaInfo = infoBuilder.build();
                requestBuilder.addTabletMetaInfos(tabletMetaInfo);
                index++;
            }
            requestBuilder.setCloudUniqueId(Config.cloud_unique_id);
            Cloud.UpdateTabletRequest updateTabletReq = requestBuilder.build();
            LOG.info("UpdateTabletRequest: {} ", updateTabletReq);

            Cloud.UpdateTabletResponse response;
            try {
                response = MetaServiceProxy.getInstance().updateTablet(updateTabletReq);
            } catch (Exception e) {
                LOG.warn("updateTablet Exception:", e);
                throw new UserException(e.getMessage());
            }
            LOG.info("response: {} ", response);

            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                throw new UserException(response.getStatus().getMsg());
            }
        }
    }
}
