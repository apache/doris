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
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.rpc.RpcException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
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
     * @param refreshContext
     * @param partitionName
     * @param tables
     * @param excludedTriggerTables
     * @return
     * @throws AnalysisException
     */
    public static boolean isMTMVPartitionSync(MTMVRefreshContext refreshContext, String partitionName,
            Set<BaseTableInfo> tables,
            Set<TableName> excludedTriggerTables) throws AnalysisException {
        MTMV mtmv = refreshContext.getMtmv();
        Set<String> relatedPartitionNames = refreshContext.getPartitionMappings().get(partitionName);
        boolean isSyncWithPartition = true;
        if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE) {
            MTMVRelatedTableIf relatedTable = mtmv.getMvPartitionInfo().getRelatedTable();
            // if follow base table, not need compare with related table, only should compare with related partition
            excludedTriggerTables.add(new TableName(relatedTable));
            if (CollectionUtils.isEmpty(relatedPartitionNames)) {
                LOG.warn("can not found related partition, partitionId: {}, mtmvName: {}, relatedTableName: {}",
                        partitionName, mtmv.getName(), relatedTable.getName());
                return false;
            }
            isSyncWithPartition = isSyncWithPartitions(refreshContext, partitionName, relatedPartitionNames);
        }
        return isSyncWithPartition && isSyncWithAllBaseTables(refreshContext, partitionName, tables,
                excludedTriggerTables);

    }

    /**
     * Align the partitions of mtmv and related tables, delete more and add less
     *
     * @param mtmv
     * @throws DdlException
     * @throws AnalysisException
     */
    public static Pair<List<String>, List<PartitionKeyDesc>> alignMvPartition(MTMV mtmv) throws AnalysisException {
        Map<String, PartitionKeyDesc> mtmvPartitionDescs = mtmv.generateMvPartitionDescs();
        Set<PartitionKeyDesc> relatedPartitionDescs = generateRelatedPartitionDescs(mtmv.getMvPartitionInfo(),
                mtmv.getMvProperties()).keySet();
        List<String> partitionsToDrop = new ArrayList<>();
        List<PartitionKeyDesc> partitionsToAdd = new ArrayList<>();
        // drop partition of mtmv
        for (Entry<String, PartitionKeyDesc> entry : mtmvPartitionDescs.entrySet()) {
            if (!relatedPartitionDescs.contains(entry.getValue())) {
                partitionsToDrop.add(entry.getKey());
            }
        }
        // add partition for mtmv
        HashSet<PartitionKeyDesc> mtmvPartitionDescsSet = Sets.newHashSet(mtmvPartitionDescs.values());
        for (PartitionKeyDesc desc : relatedPartitionDescs) {
            if (!mtmvPartitionDescsSet.contains(desc)) {
                partitionsToAdd.add(desc);
            }
        }
        return Pair.of(partitionsToDrop, partitionsToAdd);
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
            return isMTMVSync(MTMVRefreshContext.buildContext(mtmv), mtmvRelation.getBaseTablesOneLevel(),
                    Sets.newHashSet());
        } catch (AnalysisException e) {
            LOG.warn("isMTMVSync failed: ", e);
            return false;
        }
    }

    /**
     * Determine whether the mtmv is sync with tables
     *
     * @param context
     * @param tables
     * @param excludeTables
     * @return
     * @throws AnalysisException
     */
    public static boolean isMTMVSync(MTMVRefreshContext context, Set<BaseTableInfo> tables,
            Set<TableName> excludeTables)
            throws AnalysisException {
        MTMV mtmv = context.getMtmv();
        Set<String> partitionNames = mtmv.getPartitionNames();
        for (String partitionName : partitionNames) {
            if (!isMTMVPartitionSync(context, partitionName, tables,
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
     * @return partitionName ==> UnSyncTableNames
     * @throws AnalysisException
     */
    public static Map<Long, List<String>> getPartitionsUnSyncTables(MTMV mtmv)
            throws AnalysisException {
        List<Long> partitionIds = mtmv.getPartitionIds();
        Map<Long, List<String>> res = Maps.newHashMap();
        MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv);
        for (Long partitionId : partitionIds) {
            String partitionName = mtmv.getPartitionOrAnalysisException(partitionId).getName();
            res.put(partitionId, getPartitionUnSyncTables(context, partitionName));
        }
        return res;
    }

    private static List<String> getPartitionUnSyncTables(MTMVRefreshContext context, String partitionName)
            throws AnalysisException {
        MTMV mtmv = context.getMtmv();
        Set<String> relatedPartitionNames = context.getPartitionMappings().get(partitionName);
        List<String> res = Lists.newArrayList();
        for (BaseTableInfo baseTableInfo : mtmv.getRelation().getBaseTablesOneLevel()) {
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
                boolean isSyncWithPartition = isSyncWithPartitions(context, partitionName,
                        relatedPartitionNames);
                if (!isSyncWithPartition) {
                    res.add(mtmvRelatedTableIf.getName());
                }
            } else {
                if (!isSyncWithBaseTable(context, partitionName, baseTableInfo)) {
                    res.add(table.getName());
                }
            }
        }
        return res;
    }

    /**
     * Get the partitions that need to be refreshed
     *
     * @param context
     * @param baseTables
     * @return
     */
    public static List<String> getMTMVNeedRefreshPartitions(MTMVRefreshContext context, Set<BaseTableInfo> baseTables) {
        MTMV mtmv = context.getMtmv();
        Set<String> partitionNames = mtmv.getPartitionNames();
        List<String> res = Lists.newArrayList();
        for (String partitionName : partitionNames) {
            try {
                if (!isMTMVPartitionSync(context, partitionName, baseTables,
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
     * @param context
     * @param mtmvPartitionName
     * @param relatedPartitionNames
     * @return
     * @throws AnalysisException
     */
    public static boolean isSyncWithPartitions(MTMVRefreshContext context, String mtmvPartitionName,
            Set<String> relatedPartitionNames) throws AnalysisException {
        MTMV mtmv = context.getMtmv();
        MTMVRelatedTableIf relatedTable = mtmv.getMvPartitionInfo().getRelatedTable();
        if (!relatedTable.needAutoRefresh()) {
            return true;
        }
        // check if partitions of related table is changed
        Set<String> snapshotPartitions = mtmv.getRefreshSnapshot().getSnapshotPartitions(mtmvPartitionName);
        if (!Objects.equals(relatedPartitionNames, snapshotPartitions)) {
            return false;
        }
        for (String relatedPartitionName : relatedPartitionNames) {
            MTMVSnapshotIf relatedPartitionCurrentSnapshot = relatedTable
                    .getPartitionSnapshot(relatedPartitionName, context, MvccUtil.getSnapshotFromContext(relatedTable));
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("isSyncWithPartitions mvName is %s\n, mtmvPartitionName is %s\n, "
                                + "mtmv refreshSnapshot is %s\n, relatedPartitionName is %s\n, "
                                + "relatedPartitionCurrentSnapshot is %s", mtmv.getName(), mtmvPartitionName,
                        mtmv.getRefreshSnapshot(), relatedPartitionName, relatedPartitionCurrentSnapshot));
            }
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
    public static void dropPartition(MTMV mtmv, String partitionName) throws DdlException {
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
    public static void addPartition(MTMV mtmv, PartitionKeyDesc oldPartitionKeyDesc)
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
    private static boolean isSyncWithAllBaseTables(MTMVRefreshContext context, String mtmvPartitionName,
            Set<BaseTableInfo> tables,
            Set<TableName> excludedTriggerTables) throws AnalysisException {
        for (BaseTableInfo baseTableInfo : tables) {
            TableIf table = null;
            try {
                table = MTMVUtil.getTable(baseTableInfo);
            } catch (AnalysisException e) {
                LOG.warn("get table failed, {}", baseTableInfo, e);
                return false;
            }
            if (isTableExcluded(excludedTriggerTables, new TableName(table))) {
                continue;
            }
            boolean syncWithBaseTable = isSyncWithBaseTable(context, mtmvPartitionName, baseTableInfo);
            if (!syncWithBaseTable) {
                return false;
            }
        }
        return true;
    }

    public static boolean isTableExcluded(Set<TableName> excludedTriggerTables, TableName tableNameToCheck) {
        for (TableName tableName : excludedTriggerTables) {
            if (isTableNamelike(tableName, tableNameToCheck)) {
                return true;
            }
        }
        return false;
    }

    /**
     * if excludedTriggerTable.field is empty, we think they are like,otherwise they must equal to tableNameToCheck's
     *
     * @param excludedTriggerTable User-configured tables to excluded,
     *         where dbName and ctlName are not mandatory fields and may therefore be empty.
     * @param tableNameToCheck The table used to create an MTMV, must have non-empty tableName, dbName, and ctlName.
     * @return
     */
    public static boolean isTableNamelike(TableName excludedTriggerTable, TableName tableNameToCheck) {
        Objects.requireNonNull(excludedTriggerTable, "excludedTriggerTable can not be null");
        Objects.requireNonNull(tableNameToCheck, "tableNameToCheck can not be null");

        String excludedCtl = excludedTriggerTable.getCtl();
        String excludedDb = excludedTriggerTable.getDb();
        String excludedTbl = excludedTriggerTable.getTbl();
        String checkCtl = tableNameToCheck.getCtl();
        String checkDb = tableNameToCheck.getDb();
        String checkTbl = tableNameToCheck.getTbl();

        Objects.requireNonNull(excludedTbl, "excludedTbl can not be null");
        Objects.requireNonNull(checkCtl, "checkCtl can not be null");
        Objects.requireNonNull(checkDb, "checkDb can not be null");
        Objects.requireNonNull(checkTbl, "checkTbl can not be null");

        return (excludedTbl.equals(checkTbl))
                && (StringUtils.isEmpty(excludedDb) || excludedDb.equals(checkDb))
                && (StringUtils.isEmpty(excludedCtl) || excludedCtl.equals(checkCtl));
    }

    private static boolean isSyncWithBaseTable(MTMVRefreshContext context, String mtmvPartitionName,
            BaseTableInfo baseTableInfo)
            throws AnalysisException {
        MTMV mtmv = context.getMtmv();
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
        MTMVSnapshotIf baseTableCurrentSnapshot = getTableSnapshotFromContext(baseTable, context);
        return mtmv.getRefreshSnapshot()
                .equalsWithBaseTable(mtmvPartitionName, new BaseTableInfo(baseTable), baseTableCurrentSnapshot);
    }

    /**
     * Try context first, then load via getTableSnapshot and cache
     *
     * @param mtmvRelatedTableIf Base table of materialized views
     * @param context The context data persists for the duration of either a refresh task
     *         or a transparent rewrite operation
     * @return The snapshot information of the MTMV
     * @throws AnalysisException
     */
    public static MTMVSnapshotIf getTableSnapshotFromContext(MTMVRelatedTableIf mtmvRelatedTableIf,
            MTMVRefreshContext context)
            throws AnalysisException {
        BaseTableInfo baseTableInfo = new BaseTableInfo(mtmvRelatedTableIf);
        Map<BaseTableInfo, MTMVSnapshotIf> baseTableSnapshotCache = context.getBaseTableSnapshotCache();
        if (baseTableSnapshotCache.containsKey(baseTableInfo)) {
            return baseTableSnapshotCache.get(baseTableInfo);
        }
        MTMVSnapshotIf baseTableCurrentSnapshot = mtmvRelatedTableIf.getTableSnapshot(context,
                MvccUtil.getSnapshotFromContext(mtmvRelatedTableIf));
        baseTableSnapshotCache.put(baseTableInfo, baseTableCurrentSnapshot);
        return baseTableCurrentSnapshot;
    }

    /**
     * Generate updated snapshots of partitions to determine if they are synchronized
     *
     * @param context
     * @param baseTables
     * @param partitionNames
     * @return
     * @throws AnalysisException
     */
    public static Map<String, MTMVRefreshPartitionSnapshot> generatePartitionSnapshots(MTMVRefreshContext context,
            Set<BaseTableInfo> baseTables, Set<String> partitionNames)
            throws AnalysisException {
        Map<String, MTMVRefreshPartitionSnapshot> res = Maps.newHashMap();
        for (String partitionName : partitionNames) {
            res.put(partitionName,
                    generatePartitionSnapshot(context, baseTables,
                            context.getPartitionMappings().get(partitionName), false));
        }
        return res;
    }

    public static MTMVRefreshPartitionSnapshot generateIncrementalPartitionSnapshotsBySnapshotId(
            MvccSnapshot mvccSnapshot, BaseTableInfo baseTableInfo, MTMVRelatedTableIf mtmvRelatedTable)
            throws AnalysisException {
        MTMVSnapshotIf tableSnapshot = mtmvRelatedTable.getTableSnapshot(Optional.of(mvccSnapshot));
        MTMVRefreshPartitionSnapshot refreshPartitionSnapshot = new MTMVRefreshPartitionSnapshot();
        refreshPartitionSnapshot.addTableSnapshot(baseTableInfo, tableSnapshot);
        return refreshPartitionSnapshot;
    }

    public static MTMVRefreshPartitionSnapshot generatePartitionSnapshot(MTMVRefreshContext context,
            Set<BaseTableInfo> baseTables, Set<String> relatedPartitionNames, boolean incremental)
            throws AnalysisException {
        MTMV mtmv = context.getMtmv();
        MTMVRefreshPartitionSnapshot refreshPartitionSnapshot = new MTMVRefreshPartitionSnapshot();
        if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE && !incremental) {
            MTMVRelatedTableIf relatedTable = mtmv.getMvPartitionInfo().getRelatedTable();
            for (String relatedPartitionName : relatedPartitionNames) {
                MTMVSnapshotIf partitionSnapshot = relatedTable.getPartitionSnapshot(relatedPartitionName, context,
                        MvccUtil.getSnapshotFromContext(relatedTable));
                refreshPartitionSnapshot.getPartitions().put(relatedPartitionName, partitionSnapshot);
            }
        }
        for (BaseTableInfo baseTableInfo : baseTables) {
            if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE && mtmv
                    .getMvPartitionInfo().getRelatedTableInfo().equals(baseTableInfo) && !incremental) {
                continue;
            }
            TableIf table = MTMVUtil.getTable(baseTableInfo);
            if (!(table instanceof MTMVRelatedTableIf)) {
                continue;
            }
            refreshPartitionSnapshot.addTableSnapshot(baseTableInfo,
                    getTableSnapshotFromContext((MTMVRelatedTableIf) table, context));
        }
        return refreshPartitionSnapshot;
    }

    public static Type getPartitionColumnType(MTMVRelatedTableIf relatedTable, String col) throws AnalysisException {
        List<Column> partitionColumns = relatedTable.getPartitionColumns(MvccUtil.getSnapshotFromContext(relatedTable));
        for (Column column : partitionColumns) {
            if (column.getName().equals(col)) {
                return column.getType();
            }
        }
        throw new AnalysisException("can not getPartitionColumnType by:" + col);
    }

    public static MTMVBaseVersions getBaseVersions(MTMV mtmv) throws AnalysisException {
        return new MTMVBaseVersions(getTableVersions(mtmv), getPartitionVersions(mtmv));
    }

    private static Map<String, Long> getPartitionVersions(MTMV mtmv) throws AnalysisException {
        Map<String, Long> res = Maps.newHashMap();
        if (mtmv.getMvPartitionInfo().getPartitionType().equals(MTMVPartitionType.SELF_MANAGE)) {
            return res;
        }
        MTMVRelatedTableIf relatedTable = mtmv.getMvPartitionInfo().getRelatedTable();
        if (!(relatedTable instanceof OlapTable)) {
            return res;
        }
        List<Partition> partitions = Lists.newArrayList(((OlapTable) relatedTable).getPartitions());
        List<Long> versions = null;
        try {
            versions = Partition.getVisibleVersions(partitions);
        } catch (RpcException e) {
            throw new AnalysisException("getVisibleVersions failed.", e);
        }
        Preconditions.checkState(partitions.size() == versions.size());
        for (int i = 0; i < partitions.size(); i++) {
            res.put(partitions.get(i).getName(), versions.get(i));
        }
        return res;
    }

    private static Map<Long, Long> getTableVersions(MTMV mtmv) {
        Map<Long, Long> res = Maps.newHashMap();
        MTMVRelation relation = mtmv.getRelation();
        if (relation == null || relation.getBaseTablesOneLevel() == null) {
            return res;
        }
        List<OlapTable> olapTables = Lists.newArrayList();
        for (BaseTableInfo baseTableInfo : relation.getBaseTablesOneLevel()) {
            TableIf table = null;
            try {
                table = MTMVUtil.getTable(baseTableInfo);
            } catch (AnalysisException e) {
                LOG.info(e);
                continue;
            }
            if (table instanceof OlapTable) {
                olapTables.add((OlapTable) table);
            }
        }
        List<Long> versions = OlapTable.getVisibleVersionInBatch(olapTables);
        Preconditions.checkState(olapTables.size() == versions.size());
        for (int i = 0; i < olapTables.size(); i++) {
            res.put(olapTables.get(i).getId(), versions.get(i));
        }
        return res;
    }
}
