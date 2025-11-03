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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.rpc.RpcException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
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
                    new MTMVRelatedPartitionDescRollUpGenerator(),
                    new MTMVRelatedPartitionDescTransferGenerator()
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
            Set<TableNameInfo> excludedTriggerTables) throws AnalysisException {
        MTMV mtmv = refreshContext.getMtmv();
        Map<MTMVRelatedTableIf, Set<String>> partitionMappings = refreshContext.getByPartitionName(partitionName);
        if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE) {
            if (MapUtils.isEmpty(partitionMappings)) {
                LOG.warn("can not found pct partition, partitionName: {}, mtmvName: {}",
                        partitionName, mtmv.getName());
                return false;
            }
            Set<MTMVRelatedTableIf> pctTables = mtmv.getMvPartitionInfo().getPctTables();
            for (MTMVRelatedTableIf pctTable : pctTables) {
                Set<String> relatedPartitionNames = partitionMappings.getOrDefault(pctTable, Sets.newHashSet());
                // if follow base table, not need compare with related table, only should compare with related partition
                excludedTriggerTables.add(new TableNameInfo(pctTable));
                if (!isSyncWithPartitions(refreshContext, partitionName, relatedPartitionNames, pctTable)) {
                    return false;
                }
            }

        }
        return isSyncWithAllBaseTables(refreshContext, partitionName, tables,
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
                mtmv.getMvProperties(), mtmv.getPartitionColumns()).keySet();
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
     * @param partitionColumns
     * @return
     * @throws AnalysisException
     */
    public static List<AllPartitionDesc> getPartitionDescsByRelatedTable(
            Map<String, String> tableProperties, MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties,
            List<Column> partitionColumns)
            throws AnalysisException {
        List<AllPartitionDesc> res = Lists.newArrayList();
        HashMap<String, String> partitionProperties = Maps.newHashMap();
        Set<PartitionKeyDesc> relatedPartitionDescs = generateRelatedPartitionDescs(mvPartitionInfo, mvProperties,
                partitionColumns)
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

    public static Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> generateRelatedPartitionDescs(
            MTMVPartitionInfo mvPartitionInfo,
            Map<String, String> mvProperties, List<Column> partitionColumns) throws AnalysisException {
        long start = System.currentTimeMillis();
        RelatedPartitionDescResult result = new RelatedPartitionDescResult();
        for (MTMVRelatedPartitionDescGeneratorService service : partitionDescGenerators) {
            service.apply(mvPartitionInfo, mvProperties, result, partitionColumns);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("generateRelatedPartitionDescs use [{}] mills, mvPartitionInfo is [{}]",
                    System.currentTimeMillis() - start, mvPartitionInfo);
        }
        return result.getRes();
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
            return isMTMVSync(MTMVRefreshContext.buildContext(mtmv), mtmvRelation.getBaseTablesOneLevelAndFromView(),
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
            Set<TableNameInfo> excludeTables)
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
        Map<MTMVRelatedTableIf, Set<String>> mappings = context.getByPartitionName(partitionName);
        Set<MTMVRelatedTableIf> pctTables = mtmv.getMvPartitionInfo().getPctTables();
        List<String> res = Lists.newArrayList();
        for (BaseTableInfo baseTableInfo : mtmv.getRelation().getBaseTablesOneLevelAndFromView()) {
            TableIf table = MTMVUtil.getTable(baseTableInfo);
            if (!(table instanceof MTMVRelatedTableIf)) {
                continue;
            }
            MTMVRelatedTableIf pctTable = (MTMVRelatedTableIf) table;
            if (!pctTable.needAutoRefresh()) {
                continue;
            }
            if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE && pctTables.contains(
                    pctTable)) {
                Set<String> pctPartitions = mappings.getOrDefault(pctTable, Sets.newHashSet());
                boolean isSyncWithPartition = isSyncWithPartitions(context, partitionName,
                        pctPartitions, pctTable);
                if (!isSyncWithPartition) {
                    res.add(pctTable.getName());
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
     * @param pctPartitionNames
     * @return
     * @throws AnalysisException
     */
    public static boolean isSyncWithPartitions(MTMVRefreshContext context, String mtmvPartitionName,
            Set<String> pctPartitionNames, MTMVRelatedTableIf pctTable) throws AnalysisException {
        MTMV mtmv = context.getMtmv();
        if (!pctTable.needAutoRefresh()) {
            return true;
        }
        // check if partitions of related table is changed
        BaseTableInfo pctTableInfo = new BaseTableInfo(pctTable);
        // check if partitions of related table is changed
        Set<String> snapshotPartitions = mtmv.getRefreshSnapshot()
                .getPctSnapshots(mtmvPartitionName, pctTableInfo);
        if (!Objects.equals(pctPartitionNames, snapshotPartitions)) {
            return false;
        }
        if (CollectionUtils.isEmpty(pctPartitionNames)) {
            return true;
        }
        for (String pctPartitionName : pctPartitionNames) {
            MTMVSnapshotIf pctCurrentSnapshot = pctTable
                    .getPartitionSnapshot(pctPartitionName, context, MvccUtil.getSnapshotFromContext(pctTable));
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("isSyncWithPartitions mvName is %s\n, mtmvPartitionName is %s\n, "
                                + "mtmv refreshSnapshot is %s\n, pctPartitionName is %s\n, "
                                + "pctCurrentSnapshot is %s", mtmv.getName(), mtmvPartitionName,
                        mtmv.getRefreshSnapshot(), pctPartitionName, pctCurrentSnapshot));
            }
            if (!mtmv.getRefreshSnapshot()
                    .equalsWithPct(mtmvPartitionName, pctPartitionName,
                            pctCurrentSnapshot, pctTableInfo)) {
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
            Set<TableNameInfo> excludedTriggerTables) throws AnalysisException {
        for (BaseTableInfo baseTableInfo : tables) {
            TableIf table = null;
            try {
                table = MTMVUtil.getTable(baseTableInfo);
            } catch (AnalysisException e) {
                LOG.warn("get table failed, {}", baseTableInfo, e);
                return false;
            }
            if (isTableExcluded(excludedTriggerTables, new TableNameInfo(table))) {
                continue;
            }
            boolean syncWithBaseTable = isSyncWithBaseTable(context, mtmvPartitionName, baseTableInfo);
            if (!syncWithBaseTable) {
                return false;
            }
        }
        return true;
    }

    public static boolean isTableExcluded(Set<TableNameInfo> excludedTriggerTables, TableNameInfo tableNameToCheck) {
        for (TableNameInfo tableName : excludedTriggerTables) {
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
    public static boolean isTableNamelike(TableNameInfo excludedTriggerTable, TableNameInfo tableNameToCheck) {
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
                            context.getPartitionMappings().get(partitionName)));
        }
        return res;
    }


    private static MTMVRefreshPartitionSnapshot generatePartitionSnapshot(MTMVRefreshContext context,
            Set<BaseTableInfo> baseTables, Map<MTMVRelatedTableIf, Set<String>> pctPartitionNames)
            throws AnalysisException {
        MTMV mtmv = context.getMtmv();
        MTMVRefreshPartitionSnapshot refreshPartitionSnapshot = new MTMVRefreshPartitionSnapshot();
        Set<MTMVRelatedTableIf> pctTables = mtmv.getMvPartitionInfo().getPctTables();
        if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE) {
            for (MTMVRelatedTableIf pctTable : pctTables) {
                Map<String, MTMVSnapshotIf> pctSnapshot = refreshPartitionSnapshot.getPctSnapshot(
                        new BaseTableInfo(pctTable));
                Set<String> oneTablePartitionNames = pctPartitionNames.get(pctTable);
                if (CollectionUtils.isEmpty(oneTablePartitionNames)) {
                    continue;
                }
                for (String pctPartitionName : oneTablePartitionNames) {
                    MTMVSnapshotIf partitionSnapshot = pctTable.getPartitionSnapshot(pctPartitionName, context,
                            MvccUtil.getSnapshotFromContext(pctTable));
                    pctSnapshot.put(pctPartitionName, partitionSnapshot);
                }
            }
            // compatible old version
            if (pctTables.size() == 1) {
                refreshPartitionSnapshot.getPartitions()
                        .putAll(refreshPartitionSnapshot.getPcts().entrySet().iterator().next().getValue());
            }
        }
        for (BaseTableInfo baseTableInfo : baseTables) {
            TableIf table = MTMVUtil.getTable(baseTableInfo);
            if (mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE && pctTables.contains(
                    table)) {
                continue;
            }
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

    private static Map<MTMVRelatedTableIf, Map<String, Long>> getPartitionVersions(MTMV mtmv) throws AnalysisException {
        Map<MTMVRelatedTableIf, Map<String, Long>> res = Maps.newHashMap();
        if (mtmv.getMvPartitionInfo().getPartitionType().equals(MTMVPartitionType.SELF_MANAGE)) {
            return res;
        }
        Set<MTMVRelatedTableIf> pctTables = mtmv.getMvPartitionInfo().getPctTables();
        for (MTMVRelatedTableIf pctTable : pctTables) {
            if (!(pctTable instanceof OlapTable)) {
                continue;
            }
            Map<String, Long> onePctResult = Maps.newHashMap();
            List<Partition> partitions = Lists.newArrayList(((OlapTable) pctTable).getPartitions());
            List<Long> versions = null;
            try {
                versions = Partition.getVisibleVersions(partitions);
            } catch (RpcException e) {
                throw new AnalysisException("getVisibleVersions failed.", e);
            }
            Preconditions.checkState(partitions.size() == versions.size());
            for (int i = 0; i < partitions.size(); i++) {
                onePctResult.put(partitions.get(i).getName(), versions.get(i));
            }
            res.put(pctTable, onePctResult);
        }
        return res;
    }

    private static Map<Long, Long> getTableVersions(MTMV mtmv) {
        Map<Long, Long> res = Maps.newHashMap();
        MTMVRelation relation = mtmv.getRelation();
        if (relation == null || relation.getBaseTablesOneLevelAndFromView() == null) {
            return res;
        }
        List<OlapTable> olapTables = Lists.newArrayList();
        for (BaseTableInfo baseTableInfo : relation.getBaseTablesOneLevelAndFromView()) {
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
