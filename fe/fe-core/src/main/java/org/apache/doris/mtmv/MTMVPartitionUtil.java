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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

public class MTMVPartitionUtil {
    private static final Logger LOG = LogManager.getLogger(MTMVPartitionUtil.class);

    /**
     * Determine whether the partition is sync with retated partition and other baseTables
     *
     * @param mtmv
     * @param partitionId
     * @param tables
     * @param excludedTriggerTables
     * @return
     * @throws AnalysisException
     */
    public static boolean isMTMVPartitionSync(MTMV mtmv, Long partitionId, Set<BaseTableInfo> tables,
            Set<String> excludedTriggerTables) throws AnalysisException {
        boolean isSyncWithPartition = true;
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE) {
            MTMVRelatedTableIf relatedTable = mtmv.getMvPartitionInfo().getRelatedTable();
            // if follow base table, not need compare with related table, only should compare with related partition
            excludedTriggerTables.add(relatedTable.getName());
            PartitionItem item = mtmv.getPartitionInfo().getItemOrAnalysisException(partitionId);
            Map<Long, PartitionItem> relatedPartitionItems = relatedTable.getPartitionItems();
            long relatedPartitionId = getExistPartitionId(item,
                    relatedPartitionItems);
            if (relatedPartitionId == -1L) {
                LOG.warn("can not found related partition: " + partitionId);
                return false;
            }
            isSyncWithPartition = isSyncWithPartition(mtmv, partitionId, relatedTable, relatedPartitionId);
        }
        return isSyncWithPartition && isSyncWithAllBaseTables(mtmv, partitionId, tables, excludedTriggerTables);

    }

    /**
     * Align the partitions of mtmv and related tables, delete more and add less
     *
     * @param mtmv
     * @param relatedTable
     * @throws DdlException
     * @throws AnalysisException
     */
    public static void alignMvPartition(MTMV mtmv, MTMVRelatedTableIf relatedTable)
            throws DdlException, AnalysisException {
        Map<Long, PartitionItem> relatedTableItems = Maps.newHashMap(relatedTable.getPartitionItems());
        Map<Long, PartitionItem> mtmvItems = Maps.newHashMap(mtmv.getPartitionItems());
        // drop partition of mtmv
        for (Entry<Long, PartitionItem> entry : mtmvItems.entrySet()) {
            long partitionId = getExistPartitionId(entry.getValue(), relatedTableItems);
            if (partitionId == -1L) {
                dropPartition(mtmv, entry.getKey());
            }
        }
        // add partition for mtmv
        for (Entry<Long, PartitionItem> entry : relatedTableItems.entrySet()) {
            long partitionId = getExistPartitionId(entry.getValue(), mtmvItems);
            if (partitionId == -1L) {
                addPartition(mtmv, entry.getValue());
            }
        }
    }

    /**
     * getPartitionDescsByRelatedTable when create MTMV
     *
     * @param relatedTable
     * @param tableProperties
     * @return
     * @throws AnalysisException
     */
    public static List<AllPartitionDesc> getPartitionDescsByRelatedTable(MTMVRelatedTableIf relatedTable,
            Map<String, String> tableProperties) throws AnalysisException {
        HashMap<String, String> partitionProperties = Maps.newHashMap();
        List<AllPartitionDesc> res = Lists.newArrayList();
        Map<Long, PartitionItem> relatedTableItems = relatedTable.getPartitionItems();
        for (Entry<Long, PartitionItem> entry : relatedTableItems.entrySet()) {
            PartitionKeyDesc oldPartitionKeyDesc = entry.getValue().toPartitionKeyDesc();
            SinglePartitionDesc singlePartitionDesc = new SinglePartitionDesc(true,
                    generatePartitionName(oldPartitionKeyDesc),
                    oldPartitionKeyDesc, partitionProperties);
            // mtmv can only has one partition col
            singlePartitionDesc.analyze(1, tableProperties);
            res.add(singlePartitionDesc);
        }
        return res;
    }

    public static List<String> getPartitionNamesByIds(MTMV mtmv, Collection<Long> ids) throws AnalysisException {
        List<String> res = Lists.newArrayList();
        for (Long partitionId : ids) {
            res.add(mtmv.getPartitionOrAnalysisException(partitionId).getName());
        }
        return res;
    }

    public static List<Long> getPartitionsIdsByNames(MTMV mtmv, List<String> partitions) throws AnalysisException {
        List<Long> res = Lists.newArrayList();
        for (String partitionName : partitions) {
            Partition partition = mtmv.getPartitionOrAnalysisException(partitionName);
            res.add(partition.getId());
        }
        return res;
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
            return isMTMVSync(mtmv, mtmvRelation.getBaseTables(), Sets.newHashSet());
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
     * @return
     * @throws AnalysisException
     */
    public static boolean isMTMVSync(MTMV mtmv, Set<BaseTableInfo> tables, Set<String> excludeTables)
            throws AnalysisException {
        Collection<Partition> partitions = mtmv.getPartitions();
        for (Partition partition : partitions) {
            if (!isMTMVPartitionSync(mtmv, partition.getId(), tables, excludeTables)) {
                return false;
            }
        }
        return true;
    }

    /**
     * get not sync tables
     *
     * @param mtmv
     * @param partitionId
     * @return
     * @throws AnalysisException
     */
    public static List<String> getPartitionUnSyncTables(MTMV mtmv, Long partitionId) throws AnalysisException {
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
                PartitionItem item = mtmv.getPartitionInfo().getItemOrAnalysisException(partitionId);
                Map<Long, PartitionItem> relatedPartitionItems = mtmvRelatedTableIf.getPartitionItems();
                long relatedPartitionId = getExistPartitionId(item,
                        relatedPartitionItems);
                if (relatedPartitionId == -1L) {
                    throw new AnalysisException("can not found related partition");
                }
                boolean isSyncWithPartition = isSyncWithPartition(mtmv, partitionId, mtmvRelatedTableIf,
                        relatedPartitionId);
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
    public static List<Long> getMTMVNeedRefreshPartitions(MTMV mtmv, Set<BaseTableInfo> baseTables) {
        Collection<Partition> allPartitions = mtmv.getPartitions();
        List<Long> res = Lists.newArrayList();
        for (Partition partition : allPartitions) {
            try {
                if (!isMTMVPartitionSync(mtmv, partition.getId(), baseTables,
                        mtmv.getExcludedTriggerTables())) {
                    res.add(partition.getId());
                }
            } catch (AnalysisException e) {
                res.add(partition.getId());
                LOG.warn("check isMTMVPartitionSync failed", e);
            }
        }
        return res;
    }

    /**
     * compare last update time of mtmvPartition and tablePartition
     *
     * @param mtmv
     * @param mtmvPartitionId
     * @param relatedTable
     * @param relatedPartitionId
     * @return
     * @throws AnalysisException
     */
    public static boolean isSyncWithPartition(MTMV mtmv, Long mtmvPartitionId,
            MTMVRelatedTableIf relatedTable,
            Long relatedPartitionId) throws AnalysisException {
        if (!relatedTable.needAutoRefresh()) {
            return true;
        }
        MTMVSnapshotIf relatedPartitionCurrentSnapshot = relatedTable
                .getPartitionSnapshot(relatedPartitionId);
        String relatedPartitionName = relatedTable.getPartitionName(relatedPartitionId);
        String mtmvPartitionName = mtmv.getPartitionName(mtmvPartitionId);
        return mtmv.getRefreshSnapshot()
                .equalsWithRelatedPartition(mtmvPartitionName, relatedPartitionName, relatedPartitionCurrentSnapshot);
    }

    /**
     * like p_00000101_20170201
     *
     * @param desc
     * @return
     */
    public static String generatePartitionName(PartitionKeyDesc desc) {
        String partitionName = "p_";
        partitionName += desc.toSql().trim().replaceAll("\\(|\\)|\\-|\\[|\\]|'|\\s+", "")
                .replaceAll("\\(|\\)|\\,|\\[|\\]", "_");
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
     *
     * @param mtmv
     * @param partitionItem
     * @throws DdlException
     */
    private static void addPartition(MTMV mtmv, PartitionItem partitionItem)
            throws DdlException {
        PartitionKeyDesc oldPartitionKeyDesc = partitionItem.toPartitionKeyDesc();
        Map<String, String> partitionProperties = Maps.newHashMap();
        SinglePartitionDesc singlePartitionDesc = new SinglePartitionDesc(true,
                generatePartitionName(oldPartitionKeyDesc),
                oldPartitionKeyDesc, partitionProperties);

        AddPartitionClause addPartitionClause = new AddPartitionClause(singlePartitionDesc,
                mtmv.getDefaultDistributionInfo().toDistributionDesc(), partitionProperties, false);
        Env.getCurrentEnv().addPartition((Database) mtmv.getDatabase(), mtmv.getName(), addPartitionClause);
    }

    /**
     * compare PartitionItem and return equals partitionId
     * if not found, return -1L
     *
     * @param target
     * @param sources
     * @return
     */
    private static long getExistPartitionId(PartitionItem target, Map<Long, PartitionItem> sources) {
        for (Entry<Long, PartitionItem> entry : sources.entrySet()) {
            if (target.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return -1L;
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

    public static Map<String, MTMVRefreshPartitionSnapshot> generatePartitionSnapshots(MTMV mtmv,
            Set<BaseTableInfo> baseTables, Set<Long> partitionIds)
            throws AnalysisException {
        Map<String, MTMVRefreshPartitionSnapshot> res = Maps.newHashMap();
        for (Long partitionId : partitionIds) {
            res.put(mtmv.getPartition(partitionId).getName(), generatePartitionSnapshot(mtmv, baseTables, partitionId));
        }
        return res;
    }


    private static MTMVRefreshPartitionSnapshot generatePartitionSnapshot(MTMV mtmv,
            Set<BaseTableInfo> baseTables, Long partitionId)
            throws AnalysisException {
        MTMVRefreshPartitionSnapshot refreshPartitionSnapshot = new MTMVRefreshPartitionSnapshot();
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE) {
            MTMVRelatedTableIf relatedTable = mtmv.getMvPartitionInfo().getRelatedTable();
            List<Long> relatedPartitionIds = getMTMVPartitionRelatedPartitions(
                    mtmv.getPartitionItems().get(partitionId),
                    relatedTable);

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

    private static List<Long> getMTMVPartitionRelatedPartitions(PartitionItem mtmvPartitionItem,
            MTMVRelatedTableIf relatedTable) {
        List<Long> res = Lists.newArrayList();
        Map<Long, PartitionItem> relatedPartitionItems = relatedTable.getPartitionItems();
        for (Entry<Long, PartitionItem> entry : relatedPartitionItems.entrySet()) {
            if (mtmvPartitionItem.equals(entry.getValue())) {
                res.add(entry.getKey());
                // current, the partitioning of MTMV corresponds one-to-one with the partitioning of related table
                return res;
            }
        }
        return res;
    }
}
