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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.AllPartitionDesc;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MTMVPartitionUtil {
    private static final Logger LOG = LogManager.getLogger(MTMVPartitionUtil.class);
    private static final Pattern PARTITION_NAME_PATTERN = Pattern.compile("[^a-zA-Z0-9,]");
    private static final String PARTITION_NAME_PREFIX = "p_";

    /**
     * Determine whether the partition is sync with retated partition and other baseTables
     *
     * @param mtmv
     * @param partitionId
     * @param relatedPartitionIds
     * @param tables
     * @param excludedTriggerTables
     * @return
     * @throws AnalysisException
     */
    public static boolean isMTMVPartitionSync(MTMV mtmv, Long partitionId, Set<Long> relatedPartitionIds,
            Set<BaseTableInfo> tables,
            Set<String> excludedTriggerTables) throws AnalysisException {
        boolean isSyncWithPartition = true;
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE) {
            MTMVRelatedTableIf relatedTable = mtmv.getMvPartitionInfo().getRelatedTable();
            // if follow base table, not need compare with related table, only should compare with related partition
            excludedTriggerTables.add(relatedTable.getName());
            if (CollectionUtils.isEmpty(relatedPartitionIds)) {
                LOG.warn("can not found related partition, partitionId: {}, mtmvName: {}, relatedTableName: {}",
                        partitionId, mtmv.getName(), relatedTable.getName());
                return false;
            }
            isSyncWithPartition = isSyncWithPartitions(mtmv, partitionId, relatedTable, relatedPartitionIds);
        }
        return isSyncWithPartition && isSyncWithAllBaseTables(mtmv, partitionId, tables, excludedTriggerTables);

    }

    /**
     * Align the partitions of mtmv and related tables, delete more and add less
     *
     * @param mtmv
     * @throws DdlException
     * @throws AnalysisException
     */
    public static void alignMvPartition(MTMV mtmv)
            throws DdlException, AnalysisException {
        Map<Long, PartitionKeyDesc> mtmvPartitionDescs = mtmv.generateMvPartitionDescs();
        Set<PartitionKeyDesc> relatedPartitionDescs = mtmv.generateRelatedPartitionDescs().keySet();
        // drop partition of mtmv
        for (Entry<Long, PartitionKeyDesc> entry : mtmvPartitionDescs.entrySet()) {
            if (!relatedPartitionDescs.contains(entry.getValue())) {
                dropPartition(mtmv, entry.getKey());
            }
        }
        // add partition for mtmv
        HashSet<PartitionKeyDesc> mtmvPartitionDescsSet = Sets.newHashSet(mtmvPartitionDescs.values());
        for (PartitionKeyDesc desc : relatedPartitionDescs) {
            if (!mtmvPartitionDescsSet.contains(desc)) {
                addPartition(mtmv, desc);
            }
        }
    }

    /**
     * getPartitionDescsByRelatedTable when create MTMV
     *
     * @param relatedTable
     * @param tableProperties
     * @param relatedCol
     * @return
     * @throws AnalysisException
     */
    public static List<AllPartitionDesc> getPartitionDescsByRelatedTable(MTMVRelatedTableIf relatedTable,
            Map<String, String> tableProperties, String relatedCol, Map<String, String> mvProperties)
            throws AnalysisException {
        HashMap<String, String> partitionProperties = Maps.newHashMap();
        List<AllPartitionDesc> res = Lists.newArrayList();
        Set<PartitionKeyDesc> relatedPartitionDescs = getRelatedPartitionDescs(relatedTable, relatedCol,
                MTMVUtil.generateMTMVPartitionSyncConfigByProperties(mvProperties));
        for (PartitionKeyDesc partitionKeyDesc : relatedPartitionDescs) {
            SinglePartitionDesc singlePartitionDesc = new SinglePartitionDesc(true,
                    generatePartitionName(partitionKeyDesc),
                    partitionKeyDesc, partitionProperties);
            // mtmv can only has one partition col
            singlePartitionDesc.analyze(1, tableProperties);
            res.add(singlePartitionDesc);
        }
        return res;
    }

    private static Set<PartitionKeyDesc> getRelatedPartitionDescs(MTMVRelatedTableIf relatedTable, String relatedCol,
            MTMVPartitionSyncConfig config)
            throws AnalysisException {
        int pos = getPos(relatedTable, relatedCol);
        Set<PartitionKeyDesc> res = Sets.newHashSet();
        for (Entry<Long, PartitionItem> entry : relatedTable.getPartitionItemsByTimeFilter(pos, config).entrySet()) {
            PartitionKeyDesc partitionKeyDesc = entry.getValue().toPartitionKeyDesc(pos);
            res.add(partitionKeyDesc);
        }
        return res;
    }

    public static int getPos(MTMVRelatedTableIf relatedTable, String relatedCol) throws AnalysisException {
        List<Column> partitionColumns = relatedTable.getPartitionColumns();
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (partitionColumns.get(i).getName().equalsIgnoreCase(relatedCol)) {
                return i;
            }
        }
        throw new AnalysisException(
                String.format("getRelatedColPos error, relatedCol: %s, partitionColumns: %s", relatedCol,
                        partitionColumns));
    }

    public static List<String> getPartitionNamesByIds(MTMV mtmv, Collection<Long> ids) throws AnalysisException {
        List<String> res = Lists.newArrayList();
        for (Long partitionId : ids) {
            res.add(mtmv.getPartitionName(partitionId));
        }
        return res;
    }

    public static List<Long> getPartitionsIdsByNames(MTMV mtmv, List<String> partitions) throws AnalysisException {
        mtmv.readLock();
        try {
            List<Long> res = Lists.newArrayList();
            for (String partitionName : partitions) {
                Partition partition = mtmv.getPartitionOrAnalysisException(partitionName);
                res.add(partition.getId());
            }
            return res;
        } finally {
            mtmv.readUnlock();
        }

    }

    /**
     * check if table is sync with all baseTables
     *
     * @param mtmv
     * @return
     */
    public static boolean isMTMVSync(MTMV mtmv) {
        MTMVRelation mtmvRelation = mtmv.getRelation();
        if (mtmvRelation == null) {
            return false;
        }
        try {
            return isMTMVSync(mtmv, mtmvRelation.getBaseTables(), Sets.newHashSet(), mtmv.calculatePartitionMappings());
        } catch (AnalysisException e) {
            LOG.warn("isMTMVSync failed: ", e);
            return false;
        }
    }

    /**
     * Determine whether the mtmv is sync with tables
     *
     * @param mtmv
     * @param tables
     * @param excludeTables
     * @param partitionMappings
     * @return
     * @throws AnalysisException
     */
    public static boolean isMTMVSync(MTMV mtmv, Set<BaseTableInfo> tables, Set<String> excludeTables,
            Map<Long, Set<Long>> partitionMappings)
            throws AnalysisException {
        List<Long> partitionIds = mtmv.getPartitionIds();
        for (Long partitionId : partitionIds) {
            if (!isMTMVPartitionSync(mtmv, partitionId, partitionMappings.get(partitionId), tables,
                    excludeTables)) {
                return false;
            }
        }
        return true;
    }

    /**
     * getPartitionsUnSyncTables
     *
     * @param mtmv
     * @param partitionIds
     * @return partitionId ==> UnSyncTableNames
     * @throws AnalysisException
     */
    public static Map<Long, List<String>> getPartitionsUnSyncTables(MTMV mtmv, List<Long> partitionIds)
            throws AnalysisException {
        Map<Long, List<String>> res = Maps.newHashMap();
        Map<Long, Set<Long>> partitionMappings = mtmv.calculatePartitionMappings();
        for (Long partitionId : partitionIds) {
            res.put(partitionId, getPartitionUnSyncTables(mtmv, partitionId, partitionMappings.get(partitionId)));
        }
        return res;
    }

    private static List<String> getPartitionUnSyncTables(MTMV mtmv, Long partitionId, Set<Long> relatedPartitionIds)
            throws AnalysisException {
        List<String> res = Lists.newArrayList();
        for (BaseTableInfo baseTableInfo : mtmv.getRelation().getBaseTables()) {
            TableIf table = MTMVUtil.getTable(baseTableInfo);
            if (!(table instanceof MTMVRelatedTableIf)) {
                continue;
            }
            MTMVRelatedTableIf mtmvRelatedTableIf = (MTMVRelatedTableIf) table;
            if (!mtmvRelatedTableIf.needAutoRefresh()) {
                continue;
            }
            if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE && mtmv
                    .getMvPartitionInfo().getRelatedTableInfo().equals(baseTableInfo)) {
                if (CollectionUtils.isEmpty(relatedPartitionIds)) {
                    throw new AnalysisException("can not found related partition");
                }
                boolean isSyncWithPartition = isSyncWithPartitions(mtmv, partitionId, mtmvRelatedTableIf,
                        relatedPartitionIds);
                if (!isSyncWithPartition) {
                    res.add(mtmvRelatedTableIf.getName());
                }
            } else {
                if (!isSyncWithBaseTable(mtmv, partitionId, baseTableInfo)) {
                    res.add(table.getName());
                }
            }
        }
        return res;
    }

    /**
     * Get the partitions that need to be refreshed
     *
     * @param mtmv
     * @param baseTables
     * @return
     */
    public static List<Long> getMTMVNeedRefreshPartitions(MTMV mtmv, Set<BaseTableInfo> baseTables,
            Map<Long, Set<Long>> partitionMappings) {
        List<Long> partitionIds = mtmv.getPartitionIds();
        List<Long> res = Lists.newArrayList();
        for (Long partitionId : partitionIds) {
            try {
                if (!isMTMVPartitionSync(mtmv, partitionId, partitionMappings.get(partitionId), baseTables,
                        mtmv.getExcludedTriggerTables())) {
                    res.add(partitionId);
                }
            } catch (AnalysisException e) {
                res.add(partitionId);
                LOG.warn("check isMTMVPartitionSync failed", e);
            }
        }
        return res;
    }

    /**
     * Compare the current and last updated partition (or table) snapshot of the associated partition (or table)
     *
     * @param mtmv
     * @param mtmvPartitionId
     * @param relatedTable
     * @param relatedPartitionIds
     * @return
     * @throws AnalysisException
     */
    public static boolean isSyncWithPartitions(MTMV mtmv, Long mtmvPartitionId,
            MTMVRelatedTableIf relatedTable,
            Set<Long> relatedPartitionIds) throws AnalysisException {
        if (!relatedTable.needAutoRefresh()) {
            return true;
        }
        String mtmvPartitionName = mtmv.getPartitionName(mtmvPartitionId);
        for (Long relatedPartitionId : relatedPartitionIds) {
            MTMVSnapshotIf relatedPartitionCurrentSnapshot = relatedTable
                    .getPartitionSnapshot(relatedPartitionId);
            String relatedPartitionName = relatedTable.getPartitionName(relatedPartitionId);
            if (!mtmv.getRefreshSnapshot()
                    .equalsWithRelatedPartition(mtmvPartitionName, relatedPartitionName,
                            relatedPartitionCurrentSnapshot)) {
                return false;
            }
        }
        return true;
    }

    /**
     * like p_00000101_20170201
     *
     * @param desc
     * @return
     */
    public static String generatePartitionName(PartitionKeyDesc desc) {
        Matcher matcher = PARTITION_NAME_PATTERN.matcher(desc.toSql());
        String partitionName = PARTITION_NAME_PREFIX + matcher.replaceAll("").replaceAll("\\,", "_");
        if (partitionName.length() > 50) {
            partitionName = partitionName.substring(0, 30) + Math.abs(Objects.hash(partitionName))
                    + "_" + System.currentTimeMillis();
        }
        return partitionName;
    }

    /**
     * drop partition of mtmv
     *
     * @param mtmv
     * @param partitionId
     */
    private static void dropPartition(MTMV mtmv, Long partitionId) throws AnalysisException, DdlException {
        if (!mtmv.writeLockIfExist()) {
            return;
        }
        try {
            Partition partition = mtmv.getPartitionOrAnalysisException(partitionId);
            DropPartitionClause dropPartitionClause = new DropPartitionClause(false, partition.getName(), false, false);
            Env.getCurrentEnv().dropPartition((Database) mtmv.getDatabase(), mtmv, dropPartitionClause);
        } finally {
            mtmv.writeUnlock();
        }

    }

    /**
     * add partition for mtmv like relatedPartitionId of relatedTable
     * `Env.getCurrentEnv().addPartition` has obtained the lock internally, but we do not obtain the lock here
     *
     * @param mtmv
     * @param oldPartitionKeyDesc
     * @throws DdlException
     */
    private static void addPartition(MTMV mtmv, PartitionKeyDesc oldPartitionKeyDesc)
            throws DdlException {
        Map<String, String> partitionProperties = Maps.newHashMap();
        SinglePartitionDesc singlePartitionDesc = new SinglePartitionDesc(true,
                generatePartitionName(oldPartitionKeyDesc),
                oldPartitionKeyDesc, partitionProperties);

        AddPartitionClause addPartitionClause = new AddPartitionClause(singlePartitionDesc,
                mtmv.getDefaultDistributionInfo().toDistributionDesc(), partitionProperties, false);
        Env.getCurrentEnv().addPartition((Database) mtmv.getDatabase(), mtmv.getName(), addPartitionClause);
    }

    /**
     * Determine is sync, ignoring excludedTriggerTables and non OlapTanle
     *
     * @param mtmvPartitionId
     * @param tables
     * @param excludedTriggerTables
     * @return
     */
    private static boolean isSyncWithAllBaseTables(MTMV mtmv, long mtmvPartitionId, Set<BaseTableInfo> tables,
            Set<String> excludedTriggerTables) throws AnalysisException {
        for (BaseTableInfo baseTableInfo : tables) {
            TableIf table = null;
            try {
                table = MTMVUtil.getTable(baseTableInfo);
            } catch (AnalysisException e) {
                LOG.warn("get table failed, {}", baseTableInfo, e);
                return false;
            }
            if (excludedTriggerTables.contains(table.getName())) {
                continue;
            }
            boolean syncWithBaseTable = isSyncWithBaseTable(mtmv, mtmvPartitionId, baseTableInfo);
            if (!syncWithBaseTable) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSyncWithBaseTable(MTMV mtmv, long mtmvPartitionId, BaseTableInfo baseTableInfo)
            throws AnalysisException {
        TableIf table = null;
        try {
            table = MTMVUtil.getTable(baseTableInfo);
        } catch (AnalysisException e) {
            LOG.warn("get table failed, {}", baseTableInfo, e);
            return false;
        }

        if (!(table instanceof MTMVRelatedTableIf)) {
            // if not MTMVRelatedTableIf, we can not get snapshot from it,
            // Currently, it is believed to be synchronous
            return true;
        }
        MTMVRelatedTableIf baseTable = (MTMVRelatedTableIf) table;
        if (!baseTable.needAutoRefresh()) {
            return true;
        }
        MTMVSnapshotIf baseTableCurrentSnapshot = baseTable.getTableSnapshot();
        String mtmvPartitionName = mtmv.getPartitionName(mtmvPartitionId);
        return mtmv.getRefreshSnapshot()
                .equalsWithBaseTable(mtmvPartitionName, baseTable.getId(), baseTableCurrentSnapshot);
    }

    /**
     * Generate updated snapshots of partitions to determine if they are synchronized
     *
     * @param mtmv
     * @param baseTables
     * @param partitionIds
     * @param partitionMappings
     * @return
     * @throws AnalysisException
     */
    public static Map<String, MTMVRefreshPartitionSnapshot> generatePartitionSnapshots(MTMV mtmv,
            Set<BaseTableInfo> baseTables, Set<Long> partitionIds,
            Map<Long, Set<Long>> partitionMappings)
            throws AnalysisException {
        Map<String, MTMVRefreshPartitionSnapshot> res = Maps.newHashMap();
        for (Long partitionId : partitionIds) {
            res.put(mtmv.getPartitionName(partitionId),
                    generatePartitionSnapshot(mtmv, baseTables, partitionMappings.get(partitionId)));
        }
        return res;
    }


    private static MTMVRefreshPartitionSnapshot generatePartitionSnapshot(MTMV mtmv,
            Set<BaseTableInfo> baseTables, Set<Long> relatedPartitionIds)
            throws AnalysisException {
        MTMVRefreshPartitionSnapshot refreshPartitionSnapshot = new MTMVRefreshPartitionSnapshot();
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE) {
            MTMVRelatedTableIf relatedTable = mtmv.getMvPartitionInfo().getRelatedTable();
            for (Long relatedPartitionId : relatedPartitionIds) {
                MTMVSnapshotIf partitionSnapshot = relatedTable
                        .getPartitionSnapshot(relatedPartitionId);
                refreshPartitionSnapshot.getPartitions()
                        .put(relatedTable.getPartitionName(relatedPartitionId), partitionSnapshot);
            }
        }
        for (BaseTableInfo baseTableInfo : baseTables) {
            if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE && mtmv
                    .getMvPartitionInfo().getRelatedTableInfo().equals(baseTableInfo)) {
                continue;
            }
            TableIf table = MTMVUtil.getTable(baseTableInfo);
            if (!(table instanceof MTMVRelatedTableIf)) {
                continue;
            }
            refreshPartitionSnapshot.getTables().put(table.getId(), ((MTMVRelatedTableIf) table).getTableSnapshot());
        }
        return refreshPartitionSnapshot;
    }
}
