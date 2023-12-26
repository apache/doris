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
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.qe.ConnectContext;

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

public class MTMVUtil {
    private static final Logger LOG = LogManager.getLogger(MTMVUtil.class);

    /**
     * get Table by BaseTableInfo
     *
     * @param baseTableInfo
     * @return
     * @throws AnalysisException
     */
    public static TableIf getTable(BaseTableInfo baseTableInfo) throws AnalysisException {
        TableIf table = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(baseTableInfo.getCtlId())
                .getDbOrAnalysisException(baseTableInfo.getDbId())
                .getTableOrAnalysisException(baseTableInfo.getTableId());
        return table;
    }

    /**
     * Determine whether the mtmv is sync with tables
     *
     * @param mtmv
     * @param tables
     * @param excludedTriggerTables
     * @param gracePeriod
     * @return
     */
    public static boolean isMTMVSync(MTMV mtmv, Set<BaseTableInfo> tables,
            Set<String> excludedTriggerTables, Long gracePeriod) {
        return isSync(getTableMinVisibleVersionTime(mtmv), tables, excludedTriggerTables, gracePeriod);
    }

    /**
     * Determine whether the partition is sync with retated partition and other baseTables
     *
     * @param mtmv
     * @param partitionId
     * @param tables
     * @param excludedTriggerTables
     * @param gracePeriod
     * @return
     * @throws AnalysisException
     */
    public static boolean isMTMVPartitionSync(MTMV mtmv, Long partitionId, Set<BaseTableInfo> tables,
            Set<String> excludedTriggerTables, Long gracePeriod) throws AnalysisException {
        boolean isSyncWithPartition = true;
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE) {
            OlapTable relatedTable = (OlapTable) getTable(mtmv.getMvPartitionInfo().getRelatedTable());
            // if follow base table, not need compare with related table, only should compare with related partition
            excludedTriggerTables.add(relatedTable.getName());
            PartitionItem item = mtmv.getPartitionInfo().getItemOrAnalysisException(partitionId);
            long relatedPartitionId = getExistPartitionId(item,
                    relatedTable.getPartitionInfo().getIdToItem(false));
            if (relatedPartitionId == -1L) {
                LOG.warn("can not found related partition: " + partitionId);
                return false;
            }
            isSyncWithPartition = isSyncWithPartition(mtmv, partitionId, relatedTable, relatedPartitionId);
        }
        return isSyncWithPartition && isSync(
                mtmv.getPartitionOrAnalysisException(partitionId).getVisibleVersionTimeIgnoreInit(), tables,
                excludedTriggerTables, gracePeriod);

    }

    /**
     * Align the partitions of mtmv and related tables, delete more and add less
     *
     * @param mtmv
     * @param relatedTable
     * @throws DdlException
     * @throws AnalysisException
     */
    public static void alignMvPartition(MTMV mtmv, OlapTable relatedTable)
            throws DdlException, AnalysisException {
        Map<Long, PartitionItem> relatedTableItems = relatedTable.getPartitionInfo().getIdToItem(false);
        Map<Long, PartitionItem> mtmvItems = mtmv.getPartitionInfo().getIdToItem(false);
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
                addPartition(mtmv, relatedTable, entry.getKey());
            }
        }
    }

    /**
     * get mv.partitions which not sync with relatedTable
     * <p>
     * Comparing the time of mtmv and relatedTable partitioning,
     * if the visibleVersionTime of the base table is later,
     * then the partitioning of this mtmv is considered stale
     *
     * @param mtmv
     * @param relatedTable
     * @return partitionIds
     * @throws DdlException when partition can not found
     */
    public static Set<Long> getMTMVStalePartitions(MTMV mtmv, OlapTable relatedTable)
            throws AnalysisException {
        Set<Long> ids = Sets.newHashSet();
        Map<Long, Set<Long>> mvToBasePartitions = getMvToBasePartitions(mtmv, relatedTable);
        for (Entry<Long, Set<Long>> entry : mvToBasePartitions.entrySet()) {
            for (Long relatedPartitionId : entry.getValue()) {
                boolean syncWithRelatedPartition = isSyncWithPartition(mtmv, entry.getKey(), relatedTable,
                        relatedPartitionId);
                if (!syncWithRelatedPartition) {
                    ids.add(entry.getKey());
                    break;
                }
            }
        }
        return ids;
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
        return isMTMVSync(mtmv, mtmv.getRelation().getBaseTables(), Sets.newHashSet(), 0L);
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
        long maxAvailableTime = mtmv.getPartitionOrAnalysisException(partitionId).getVisibleVersionTimeIgnoreInit();
        for (BaseTableInfo baseTableInfo : mtmv.getRelation().getBaseTables()) {
            TableIf table = getTable(baseTableInfo);
            if (!(table instanceof OlapTable)) {
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE && mtmv
                    .getMvPartitionInfo().getRelatedTable().equals(baseTableInfo)) {
                PartitionItem item = mtmv.getPartitionInfo().getItemOrAnalysisException(partitionId);
                long relatedPartitionId = getExistPartitionId(item,
                        olapTable.getPartitionInfo().getIdToItem(false));
                if (relatedPartitionId == -1L) {
                    throw new AnalysisException("can not found related partition");
                }
                boolean isSyncWithPartition = isSyncWithPartition(mtmv, partitionId, olapTable, relatedPartitionId);
                if (!isSyncWithPartition) {
                    res.add(olapTable.getName());
                }
            } else {
                long tableLastVisibleVersionTime = getTableMaxVisibleVersionTime((OlapTable) table);
                if (tableLastVisibleVersionTime > maxAvailableTime) {
                    res.add(table.getName());
                }
            }
        }
        return res;
    }

    /**
     * Determine which partition of mtmv can be rewritten
     *
     * @param mtmv
     * @param ctx
     * @return
     */
    public static Collection<Partition> getMTMVCanRewritePartitions(MTMV mtmv, ConnectContext ctx) {
        List<Partition> res = Lists.newArrayList();
        Collection<Partition> allPartitions = mtmv.getPartitions();
        // check session variable if enable rewrite
        if (!ctx.getSessionVariable().isEnableMaterializedViewRewrite()) {
            return res;
        }
        if (mtmvContainsExternalTable(mtmv) && !ctx.getSessionVariable()
                .isMaterializedViewRewriteEnableContainForeignTable()) {
            return res;
        }
        MTMVRelation mtmvRelation = mtmv.getRelation();
        if (mtmvRelation == null) {
            return res;
        }
        // check mv is normal
        if (!(mtmv.getStatus().getState() == MTMVState.NORMAL
                && mtmv.getStatus().getRefreshState() == MTMVRefreshState.SUCCESS)) {
            return res;
        }
        // check gracePeriod
        Long gracePeriod = mtmv.getGracePeriod();
        // do not care data is delayed
        if (gracePeriod < 0) {
            return allPartitions;
        }

        for (Partition partition : allPartitions) {
            try {
                if (isMTMVPartitionSync(mtmv, partition.getId(), mtmvRelation.getBaseTables(), Sets.newHashSet(),
                        gracePeriod)) {
                    res.add(partition);
                }
            } catch (AnalysisException e) {
                // ignore it
                LOG.warn("check isMTMVPartitionSync failed", e);
            }
        }
        return res;
    }

    public static List<Long> getMTMVNeedRefreshPartitions(MTMV mtmv) {
        Collection<Partition> allPartitions = mtmv.getPartitions();
        List<Long> res = Lists.newArrayList();
        for (Partition partition : allPartitions) {
            try {
                if (!isMTMVPartitionSync(mtmv, partition.getId(), mtmv.getRelation().getBaseTables(),
                        mtmv.getExcludedTriggerTables(),
                        0L)) {
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
     * @param relatedTablePartitionId
     * @return
     * @throws AnalysisException
     */
    private static boolean isSyncWithPartition(MTMV mtmv, Long mtmvPartitionId, OlapTable relatedTable,
            Long relatedTablePartitionId) throws AnalysisException {
        return mtmv.getPartitionOrAnalysisException(mtmvPartitionId).getVisibleVersionTimeIgnoreInit() >= relatedTable
                .getPartitionOrAnalysisException(relatedTablePartitionId).getVisibleVersionTimeIgnoreInit();
    }

    /**
     * like p_00000101_20170201
     *
     * @param desc
     * @return
     */
    private static String generatePartitionName(PartitionKeyDesc desc) {
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
        Partition partition = mtmv.getPartitionOrAnalysisException(partitionId);
        DropPartitionClause dropPartitionClause = new DropPartitionClause(false, partition.getName(), false, false);
        Env.getCurrentEnv().dropPartition((Database) mtmv.getDatabase(), mtmv, dropPartitionClause);
    }

    /**
     * add partition for mtmv like relatedPartitionId of relatedTable
     *
     * @param mtmv
     * @param relatedTable
     * @param relatedPartitionId
     * @throws AnalysisException
     * @throws DdlException
     */
    private static void addPartition(MTMV mtmv, OlapTable relatedTable, Long relatedPartitionId)
            throws AnalysisException, DdlException {
        PartitionDesc partitionDesc = relatedTable.getPartitionInfo().toPartitionDesc(relatedTable);
        Partition partition = relatedTable.getPartitionOrAnalysisException(relatedPartitionId);
        SinglePartitionDesc oldPartitionDesc = partitionDesc.getSinglePartitionDescByName(partition.getName());

        Map<String, String> partitionProperties = Maps.newHashMap();
        SinglePartitionDesc singleRangePartitionDesc = new SinglePartitionDesc(true,
                generatePartitionName(oldPartitionDesc.getPartitionKeyDesc()),
                oldPartitionDesc.getPartitionKeyDesc(), partitionProperties);

        AddPartitionClause addPartitionClause = new AddPartitionClause(singleRangePartitionDesc,
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
     * Get the maximum update time among all partitions
     *
     * @param table
     * @return
     */
    private static long getTableMaxVisibleVersionTime(OlapTable table) {
        long result = 0L;
        long visibleVersionTime;
        for (Partition partition : table.getAllPartitions()) {
            visibleVersionTime = partition.getVisibleVersionTimeIgnoreInit();
            if (visibleVersionTime > result) {
                result = visibleVersionTime;
            }
        }
        return result;
    }

    /**
     * Get the minimum update time among all partitions
     *
     * @param table
     * @return
     */
    private static long getTableMinVisibleVersionTime(OlapTable table) {
        long result = Long.MAX_VALUE;
        long visibleVersionTime;
        for (Partition partition : table.getAllPartitions()) {
            visibleVersionTime = partition.getVisibleVersionTimeIgnoreInit();
            if (visibleVersionTime < result) {
                result = visibleVersionTime;
            }
        }
        return result;
    }

    /**
     * Obtain the partition correspondence between materialized views and base tables
     * Currently, there is a one-to-one correspondence between the partitions of materialized views and base tables,
     * but for scalability reasons, Set is used
     * <p>
     * before use this method,should call `alignMvPartition`
     *
     * @param mtmv
     * @param relatedTable
     * @return mv.partitionId ==> relatedTable.partitionId
     */
    public static Map<Long, Set<Long>> getMvToBasePartitions(MTMV mtmv, OlapTable relatedTable)
            throws AnalysisException {
        HashMap<Long, Set<Long>> res = Maps.newHashMap();
        Map<Long, PartitionItem> relatedTableItems = relatedTable.getPartitionInfo().getIdToItem(false);
        Map<Long, PartitionItem> mtmvItems = mtmv.getPartitionInfo().getIdToItem(false);
        for (Entry<Long, PartitionItem> entry : mtmvItems.entrySet()) {
            long partitionId = getExistPartitionId(entry.getValue(), relatedTableItems);
            if (partitionId == -1L) {
                throw new AnalysisException("partition not found: " + entry.getValue().toString());
            }
            res.put(entry.getKey(), Sets.newHashSet(partitionId));
        }
        return res;
    }

    /**
     * Determine is sync, ignoring excludedTriggerTables and non OlapTanle
     *
     * @param visibleVersionTime
     * @param tables
     * @param excludedTriggerTables
     * @param gracePeriod
     * @return
     */
    private static boolean isSync(long visibleVersionTime, Set<BaseTableInfo> tables,
            Set<String> excludedTriggerTables, Long gracePeriod) {
        long maxAvailableTime = visibleVersionTime + gracePeriod;
        for (BaseTableInfo baseTableInfo : tables) {
            TableIf table = null;
            try {
                table = getTable(baseTableInfo);
            } catch (AnalysisException e) {
                LOG.warn("get table failed, {}", baseTableInfo, e);
                return false;
            }
            if (excludedTriggerTables.contains(table.getName())) {
                continue;
            }
            if (!(table instanceof OlapTable)) {
                continue;
            }
            long tableLastVisibleVersionTime = getTableMaxVisibleVersionTime((OlapTable) table);
            if (tableLastVisibleVersionTime > maxAvailableTime) {
                return false;
            }
        }
        return true;
    }

    private static boolean mtmvContainsExternalTable(MTMV mtmv) {
        Set<BaseTableInfo> baseTables = mtmv.getRelation().getBaseTables();
        for (BaseTableInfo baseTableInfo : baseTables) {
            if (baseTableInfo.getCtlId() != InternalCatalog.INTERNAL_CATALOG_ID) {
                return true;
            }
        }
        return false;
    }
}
