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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.collect.ImmutableList;
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

    private static final List<MTMVRelatedPartitionDescGeneratorService> partitionDescGenerators = ImmutableList
            .of(
                    // It is necessary to maintain this order,
                    // because some impl deal `PartitionItem`, and some impl deal `PartitionDesc`
                    // for example: if `MTMVRelatedPartitionDescOnePartitionColGenerator` not generate `PartitionDesc`,
                    // `MTMVRelatedPartitionDescRollUpGenerator` will not have parameter
                    new MTMVRelatedPartitionDescInitGenerator(),
                    new MTMVRelatedPartitionDescSyncLimitGenerator(),
                    new MTMVRelatedPartitionDescOnePartitionColGenerator(),
                    new MTMVRelatedPartitionDescRollUpGenerator()
            );

    /**
     * Determine whether the partition is sync with retated partition and other baseTables
     *
     * @param mtmv
     * @param partitionName
     * @param relatedPartitionNames
     * @param tables
     * @param excludedTriggerTables
     * @return
     * @throws AnalysisException
     */
    public static boolean isMTMVPartitionSync(MTMV mtmv, String partitionName, Set<String> relatedPartitionNames,
            Set<BaseTableInfo> tables,
            Set<String> excludedTriggerTables) throws AnalysisException {
        boolean isSyncWithPartition = true;
        if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE) {
            MTMVRelatedTableIf relatedTable = mtmv.getMvPartitionInfo().getRelatedTable();
            // if follow base table, not need compare with related table, only should compare with related partition
            excludedTriggerTables.add(relatedTable.getName());
            if (CollectionUtils.isEmpty(relatedPartitionNames)) {
                LOG.warn("can not found related partition, partitionId: {}, mtmvName: {}, relatedTableName: {}",
                        partitionName, mtmv.getName(), relatedTable.getName());
                return false;
            }
            isSyncWithPartition = isSyncWithPartitions(mtmv, partitionName, relatedTable, relatedPartitionNames);
        }
        return isSyncWithPartition && isSyncWithAllBaseTables(mtmv, partitionName, tables, excludedTriggerTables);

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
        Map<String, PartitionKeyDesc> mtmvPartitionDescs = mtmv.generateMvPartitionDescs();
        Set<PartitionKeyDesc> relatedPartitionDescs = generateRelatedPartitionDescs(mtmv.getMvPartitionInfo(),
                mtmv.getMvProperties()).keySet();
        // drop partition of mtmv
        for (Entry<String, PartitionKeyDesc> entry : mtmvPartitionDescs.entrySet()) {
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
     * @param tableProperties
     * @param mvPartitionInfo
     * @return
     * @throws AnalysisException
     */
    public static List<AllPartitionDesc> getPartitionDescsByRelatedTable(
            Map<String, String> tableProperties, MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties)
            throws AnalysisException {
        List<AllPartitionDesc> res = Lists.newArrayList();
        HashMap<String, String> partitionProperties = Maps.newHashMap();
        Set<PartitionKeyDesc> relatedPartitionDescs = generateRelatedPartitionDescs(mvPartitionInfo, mvProperties)
                .keySet();
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

    public static Map<PartitionKeyDesc, Set<String>> generateRelatedPartitionDescs(MTMVPartitionInfo mvPartitionInfo,
            Map<String, String> mvProperties) throws AnalysisException {
        long start = System.currentTimeMillis();
        RelatedPartitionDescResult result = new RelatedPartitionDescResult();
        for (MTMVRelatedPartitionDescGeneratorService service : partitionDescGenerators) {
            service.apply(mvPartitionInfo, mvProperties, result);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("generateRelatedPartitionDescs use [{}] mills, mvPartitionInfo is [{}]",
                    System.currentTimeMillis() - start, mvPartitionInfo);
        }
        return result.getDescs();
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
            Map<String, Set<String>> partitionMappings)
            throws AnalysisException {
        Set<String> partitionNames = mtmv.getPartitionNames();
        for (String partitionName : partitionNames) {
            if (!isMTMVPartitionSync(mtmv, partitionName, partitionMappings.get(partitionName), tables,
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
     * @return partitionName ==> UnSyncTableNames
     * @throws AnalysisException
     */
    public static Map<Long, List<String>> getPartitionsUnSyncTables(MTMV mtmv, List<Long> partitionIds)
            throws AnalysisException {
        Map<Long, List<String>> res = Maps.newHashMap();
        Map<String, Set<String>> partitionMappings = mtmv.calculatePartitionMappings();
        for (Long partitionId : partitionIds) {
            String partitionName = mtmv.getPartitionOrAnalysisException(partitionId).getName();
            res.put(partitionId, getPartitionUnSyncTables(mtmv, partitionName, partitionMappings.get(partitionName)));
        }
        return res;
    }

    private static List<String> getPartitionUnSyncTables(MTMV mtmv, String partitionName,
            Set<String> relatedPartitionNames)
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
            if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE && mtmv
                    .getMvPartitionInfo().getRelatedTableInfo().equals(baseTableInfo)) {
                if (CollectionUtils.isEmpty(relatedPartitionNames)) {
                    // can not found related partition
                    res.add(mtmvRelatedTableIf.getName());
                    continue;
                }
                boolean isSyncWithPartition = isSyncWithPartitions(mtmv, partitionName, mtmvRelatedTableIf,
                        relatedPartitionNames);
                if (!isSyncWithPartition) {
                    res.add(mtmvRelatedTableIf.getName());
                }
            } else {
                if (!isSyncWithBaseTable(mtmv, partitionName, baseTableInfo)) {
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
    public static List<String> getMTMVNeedRefreshPartitions(MTMV mtmv, Set<BaseTableInfo> baseTables,
            Map<String, Set<String>> partitionMappings) {
        Set<String> partitionNames = mtmv.getPartitionNames();
        List<String> res = Lists.newArrayList();
        for (String partitionName : partitionNames) {
            try {
                if (!isMTMVPartitionSync(mtmv, partitionName, partitionMappings.get(partitionName), baseTables,
                        mtmv.getExcludedTriggerTables())) {
                    res.add(partitionName);
                }
            } catch (AnalysisException e) {
                res.add(partitionName);
                LOG.warn("check isMTMVPartitionSync failed", e);
            }
        }
        return res;
    }

    /**
     * Compare the current and last updated partition (or table) snapshot of the associated partition (or table)
     *
     * @param mtmv
     * @param mtmvPartitionName
     * @param relatedTable
     * @param relatedPartitionNames
     * @return
     * @throws AnalysisException
     */
    public static boolean isSyncWithPartitions(MTMV mtmv, String mtmvPartitionName,
            MTMVRelatedTableIf relatedTable,
            Set<String> relatedPartitionNames) throws AnalysisException {
        if (!relatedTable.needAutoRefresh()) {
            return true;
        }
        // check if partitions of related table if changed
        Set<String> snapshotPartitions = mtmv.getRefreshSnapshot().getSnapshotPartitions(mtmvPartitionName);
        if (!Objects.equals(relatedPartitionNames, snapshotPartitions)) {
            return false;
        }
        for (String relatedPartitionName : relatedPartitionNames) {
            MTMVSnapshotIf relatedPartitionCurrentSnapshot = relatedTable
                    .getPartitionSnapshot(relatedPartitionName);
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
     * @param partitionName
     */
    private static void dropPartition(MTMV mtmv, String partitionName) throws DdlException {
        if (!mtmv.writeLockIfExist()) {
            return;
        }
        try {
            DropPartitionClause dropPartitionClause = new DropPartitionClause(false, partitionName, false, false);
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
        Env.getCurrentEnv().addPartition((Database) mtmv.getDatabase(), mtmv.getName(), addPartitionClause,
                false, 0, true);
    }

    /**
     * Determine is sync, ignoring excludedTriggerTables and non OlapTanle
     *
     * @param mtmvPartitionName
     * @param tables
     * @param excludedTriggerTables
     * @return
     */
    private static boolean isSyncWithAllBaseTables(MTMV mtmv, String mtmvPartitionName, Set<BaseTableInfo> tables,
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
            boolean syncWithBaseTable = isSyncWithBaseTable(mtmv, mtmvPartitionName, baseTableInfo);
            if (!syncWithBaseTable) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSyncWithBaseTable(MTMV mtmv, String mtmvPartitionName, BaseTableInfo baseTableInfo)
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
        return mtmv.getRefreshSnapshot()
                .equalsWithBaseTable(mtmvPartitionName, baseTable.getId(), baseTableCurrentSnapshot);
    }

    /**
     * Generate updated snapshots of partitions to determine if they are synchronized
     *
     * @param mtmv
     * @param baseTables
     * @param partitionNames
     * @param partitionMappings
     * @return
     * @throws AnalysisException
     */
    public static Map<String, MTMVRefreshPartitionSnapshot> generatePartitionSnapshots(MTMV mtmv,
            Set<BaseTableInfo> baseTables, Set<String> partitionNames,
            Map<String, Set<String>> partitionMappings)
            throws AnalysisException {
        Map<String, MTMVRefreshPartitionSnapshot> res = Maps.newHashMap();
        for (String partitionName : partitionNames) {
            res.put(partitionName,
                    generatePartitionSnapshot(mtmv, baseTables, partitionMappings.get(partitionName)));
        }
        return res;
    }


    private static MTMVRefreshPartitionSnapshot generatePartitionSnapshot(MTMV mtmv,
            Set<BaseTableInfo> baseTables, Set<String> relatedPartitionNames)
            throws AnalysisException {
        MTMVRefreshPartitionSnapshot refreshPartitionSnapshot = new MTMVRefreshPartitionSnapshot();
        if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE) {
            MTMVRelatedTableIf relatedTable = mtmv.getMvPartitionInfo().getRelatedTable();
            for (String relatedPartitionName : relatedPartitionNames) {
                MTMVSnapshotIf partitionSnapshot = relatedTable
                        .getPartitionSnapshot(relatedPartitionName);
                refreshPartitionSnapshot.getPartitions()
                        .put(relatedPartitionName, partitionSnapshot);
            }
        }
        for (BaseTableInfo baseTableInfo : baseTables) {
            if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE && mtmv
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

    public static Type getPartitionColumnType(MTMVRelatedTableIf relatedTable, String col) throws AnalysisException {
        List<Column> partitionColumns = relatedTable.getPartitionColumns();
        for (Column column : partitionColumns) {
            if (column.getName().equals(col)) {
                return column.getType();
            }
        }
        throw new AnalysisException("can not getPartitionColumnType by:" + col);
    }
}
