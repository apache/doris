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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.UseMvHint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * If enable query rewrite with mv, should init materialization context after analyze
 */
public class InitMaterializationContextHook implements PlannerHook {

    public static final Logger LOG = LogManager.getLogger(InitMaterializationContextHook.class);
    public static final InitMaterializationContextHook INSTANCE = new InitMaterializationContextHook();

    @Override
    public void afterRewrite(NereidsPlanner planner) {
        CascadesContext cascadesContext = planner.getCascadesContext();
        // collect partitions table used, this is for query rewrite by materialized view
        // this is needed before init hook, because compare partition version in init hook would use this
        MaterializedViewUtils.collectTableUsedPartitions(cascadesContext.getRewritePlan(), cascadesContext);
        StatementContext statementContext = cascadesContext.getStatementContext();
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsCollectTablePartitionFinishTime();
        }
        initMaterializationContext(planner.getCascadesContext());
    }

    @VisibleForTesting
    public void initMaterializationContext(CascadesContext cascadesContext) {
        if (!cascadesContext.getConnectContext().getSessionVariable().isEnableMaterializedViewRewrite()) {
            return;
        }
        doInitMaterializationContext(cascadesContext);
    }

    /**
     * Init materialization context
     * @param cascadesContext current cascadesContext in the planner
     */
    protected void doInitMaterializationContext(CascadesContext cascadesContext) {
        if (cascadesContext.getConnectContext().getSessionVariable().isInDebugMode()) {
            LOG.info("MaterializationContext init return because is in debug mode, current queryId is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
            return;
        }
        Set<TableIf> collectedTables = Sets.newHashSet(cascadesContext.getStatementContext().getTables().values());
        if (collectedTables.isEmpty()) {
            return;
        }
        // Create sync materialization context
        for (TableIf tableIf : collectedTables) {
            if (tableIf instanceof OlapTable) {
                for (MaterializationContext context : createSyncMvContexts(
                        (OlapTable) tableIf, cascadesContext)) {
                    cascadesContext.addMaterializationContext(context);
                }
            }
        }

        // Create async materialization context
        for (MaterializationContext context : createAsyncMaterializationContext(cascadesContext,
                collectedTables)) {
            cascadesContext.addMaterializationContext(context);
        }
    }

    private List<MaterializationContext> getMvIdWithUseMvHint(List<MaterializationContext> mtmvCtxs,
                                                                UseMvHint useMvHint) {
        List<MaterializationContext> hintMTMVs = new ArrayList<>();
        for (MaterializationContext mtmvCtx : mtmvCtxs) {
            List<String> mvQualifier = mtmvCtx.generateMaterializationIdentifier();
            if (useMvHint.getUseMvTableColumnMap().containsKey(mvQualifier)) {
                hintMTMVs.add(mtmvCtx);
            }
        }
        return hintMTMVs;
    }

    private List<MaterializationContext> getMvIdWithNoUseMvHint(List<MaterializationContext> mtmvCtxs,
                                                                    UseMvHint useMvHint) {
        List<MaterializationContext> hintMTMVs = new ArrayList<>();
        if (useMvHint.isAllMv()) {
            useMvHint.setStatus(Hint.HintStatus.SUCCESS);
            return hintMTMVs;
        }
        for (MaterializationContext mtmvCtx : mtmvCtxs) {
            List<String> mvQualifier = mtmvCtx.generateMaterializationIdentifier();
            if (useMvHint.getNoUseMvTableColumnMap().containsKey(mvQualifier)) {
                useMvHint.setStatus(Hint.HintStatus.SUCCESS);
                useMvHint.getNoUseMvTableColumnMap().put(mvQualifier, true);
            } else {
                hintMTMVs.add(mtmvCtx);
            }
        }
        return hintMTMVs;
    }

    /**
     * get mtmvs by hint
     * @param mtmvCtxs input mtmvs which could be used to rewrite sql
     * @return set of mtmvs which pass the check of useMvHint
     */
    public List<MaterializationContext> getMaterializationContextByHint(List<MaterializationContext> mtmvCtxs) {
        Optional<UseMvHint> useMvHint = ConnectContext.get().getStatementContext().getUseMvHint("USE_MV");
        Optional<UseMvHint> noUseMvHint = ConnectContext.get().getStatementContext().getUseMvHint("NO_USE_MV");
        if (!useMvHint.isPresent() && !noUseMvHint.isPresent()) {
            return mtmvCtxs;
        }
        List<MaterializationContext> result = mtmvCtxs;
        if (noUseMvHint.isPresent()) {
            result = getMvIdWithNoUseMvHint(result, noUseMvHint.get());
        }
        if (useMvHint.isPresent()) {
            result = getMvIdWithUseMvHint(result, useMvHint.get());
        }
        return result;
    }

    protected Set<MTMV> getAvailableMTMVs(Set<TableIf> usedTables, CascadesContext cascadesContext) {
        return Env.getCurrentEnv().getMtmvService().getRelationManager()
                .getAvailableMTMVs(cascadesContext.getStatementContext().getCandidateMTMVs(),
                        cascadesContext.getConnectContext(),
                        false, ((connectContext, mtmv) -> {
                            return MTMVUtil.mtmvContainsExternalTable(mtmv) && (!connectContext.getSessionVariable()
                                    .isEnableMaterializedViewRewriteWhenBaseTableUnawareness());
                        }));
    }

    private List<MaterializationContext> createAsyncMaterializationContext(CascadesContext cascadesContext,
            Set<TableIf> usedTables) {
        Set<MTMV> availableMTMVs;
        try {
            availableMTMVs = getAvailableMTMVs(usedTables, cascadesContext);
        } catch (Exception e) {
            LOG.warn(String.format("MaterializationContext getAvailableMTMVs generate fail, current sqlHash is %s",
                    cascadesContext.getConnectContext().getSqlHash()), e);
            return ImmutableList.of();
        }
        if (CollectionUtils.isEmpty(availableMTMVs)) {
            LOG.debug("Enable materialized view rewrite but availableMTMVs is empty, current sqlHash "
                    + "is {}", cascadesContext.getConnectContext().getSqlHash());
            return ImmutableList.of();
        }
        List<MaterializationContext> asyncMaterializationContext = new ArrayList<>();
        for (MTMV materializedView : availableMTMVs) {
            MTMVCache mtmvCache = null;
            try {
                mtmvCache = materializedView.getOrGenerateCache(cascadesContext.getConnectContext());
                if (mtmvCache == null) {
                    continue;
                }
                // For async materialization context, the cascades context when construct the struct info maybe
                // different from the current cascadesContext
                // so regenerate the struct info table bitset
                StructInfo mvStructInfo = mtmvCache.getStructInfo();
                BitSet tableBitSetInCurrentCascadesContext = new BitSet();
                BitSet relationIdBitSetInCurrentCascadesContext = new BitSet();
                mvStructInfo.getRelations().forEach(relation -> {
                    tableBitSetInCurrentCascadesContext.set(
                            cascadesContext.getStatementContext().getTableId(relation.getTable()).asInt());
                    relationIdBitSetInCurrentCascadesContext.set(relation.getRelationId().asInt());
                });
                asyncMaterializationContext.add(new AsyncMaterializationContext(materializedView,
                        mtmvCache.getLogicalPlan(), mtmvCache.getOriginalPlan(), ImmutableList.of(),
                        ImmutableList.of(), cascadesContext,
                        mtmvCache.getStructInfo().withTableBitSet(tableBitSetInCurrentCascadesContext,
                                relationIdBitSetInCurrentCascadesContext)));
            } catch (Exception e) {
                LOG.warn(String.format("MaterializationContext init mv cache generate fail, current queryId is %s",
                        cascadesContext.getConnectContext().getQueryIdentifier()), e);
            }
        }
        return getMaterializationContextByHint(asyncMaterializationContext);
    }

    private List<MaterializationContext> createSyncMvContexts(OlapTable olapTable,
            CascadesContext cascadesContext) {
        int indexNumber = olapTable.getIndexNumber();
        List<MaterializationContext> contexts = new ArrayList<>(indexNumber);
        long baseIndexId = olapTable.getBaseIndexId();
        int keyCount = 0;
        for (Column column : olapTable.getFullSchema()) {
            keyCount += column.isKey() ? 1 : 0;
        }
        for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getVisibleIndexIdToMeta().entrySet()) {
            long indexId = entry.getKey();
            String indexName = olapTable.getIndexNameById(indexId);
            try {
                if (indexId != baseIndexId) {
                    MaterializedIndexMeta meta = entry.getValue();
                    String createMvSql;
                    if (meta.getDefineStmt() != null) {
                        // get the original create mv sql
                        createMvSql = meta.getDefineStmt().originStmt;
                    } else {
                        // it's rollup, need assemble create mv sql manually
                        if (olapTable.getKeysType() == KeysType.AGG_KEYS) {
                            createMvSql = assembleCreateMvSqlForAggTable(olapTable.getQualifiedName(),
                                    indexName, meta.getSchema(false), keyCount);
                        } else {
                            createMvSql =
                                    assembleCreateMvSqlForDupOrUniqueTable(olapTable.getQualifiedName(),
                                            indexName, meta.getSchema(false));
                        }
                    }
                    if (createMvSql != null) {
                        Optional<String> querySql =
                                new NereidsParser().parseForSyncMv(createMvSql);
                        if (!querySql.isPresent()) {
                            LOG.warn(String.format("can't parse %s ", createMvSql));
                            continue;
                        }
                        ConnectContext basicMvContext = MTMVPlanUtil.createBasicMvContext(
                                cascadesContext.getConnectContext());
                        basicMvContext.setDatabase(meta.getDbName());
                        MTMVCache mtmvCache = MTMVCache.from(querySql.get(),
                                basicMvContext, true,
                                false, cascadesContext.getConnectContext());
                        contexts.add(new SyncMaterializationContext(mtmvCache.getLogicalPlan(),
                                mtmvCache.getOriginalPlan(), olapTable, meta.getIndexId(), indexName,
                                cascadesContext, mtmvCache.getStatistics()));
                    } else {
                        LOG.warn(String.format("can't assemble create mv sql for index ", indexName));
                    }
                }
            } catch (Exception exception) {
                LOG.warn(String.format("createSyncMvContexts exception, index id is %s, index name is %s, "
                                + "table name is %s", entry.getValue(), indexName, olapTable.getQualifiedName()),
                        exception);
            }
        }
        return getMaterializationContextByHint(contexts);
    }

    private String assembleCreateMvSqlForDupOrUniqueTable(String baseTableName, String mvName, List<Column> columns) {
        StringBuilder createMvSqlBuilder = new StringBuilder();
        createMvSqlBuilder.append(String.format("create materialized view %s as select ", mvName));
        for (Column col : columns) {
            createMvSqlBuilder.append(String.format("%s, ", col.getName()));
        }
        removeLastTwoChars(createMvSqlBuilder);
        createMvSqlBuilder.append(String.format(" from %s", baseTableName));
        return createMvSqlBuilder.toString();
    }

    private String assembleCreateMvSqlForAggTable(String baseTableName, String mvName,
            List<Column> columns, int keyCount) {
        StringBuilder createMvSqlBuilder = new StringBuilder();
        createMvSqlBuilder.append(String.format("create materialized view %s as select ", mvName));
        int mvKeyCount = 0;
        for (Column column : columns) {
            mvKeyCount += column.isKey() ? 1 : 0;
        }
        if (mvKeyCount < keyCount) {
            StringBuilder keyColumnsStringBuilder = new StringBuilder();
            StringBuilder aggColumnsStringBuilder = new StringBuilder();
            for (Column col : columns) {
                AggregateType aggregateType = col.getAggregationType();
                if (aggregateType != null) {
                    switch (aggregateType) {
                        case SUM:
                        case MAX:
                        case MIN:
                        case HLL_UNION:
                        case BITMAP_UNION:
                        case QUANTILE_UNION: {
                            aggColumnsStringBuilder
                                    .append(String.format("%s(%s), ", aggregateType, col.getName()));
                            break;
                        }
                        case GENERIC: {
                            AggStateType aggStateType = (AggStateType) col.getType();
                            aggColumnsStringBuilder.append(String.format("%s_union(%s), ",
                                    aggStateType.getFunctionName(), col.getName()));
                            break;
                        }
                        default: {
                            // mv agg columns mustn't be NONE, REPLACE, REPLACE_IF_NOT_NULL agg type
                            LOG.warn(String.format("mv agg column %s mustn't be %s type",
                                    col.getName(), aggregateType));
                            return null;
                        }
                    }
                } else {
                    // use column name for key
                    Preconditions.checkState(col.isKey(),
                            String.format("%s must be key", col.getName()));
                    keyColumnsStringBuilder.append(String.format("%s, ", col.getName()));
                }
            }
            Preconditions.checkState(keyColumnsStringBuilder.length() > 0,
                    "must contain at least one key column in rollup");
            if (aggColumnsStringBuilder.length() > 0) {
                removeLastTwoChars(aggColumnsStringBuilder);
            } else {
                removeLastTwoChars(keyColumnsStringBuilder);
            }
            createMvSqlBuilder.append(keyColumnsStringBuilder);
            createMvSqlBuilder.append(aggColumnsStringBuilder);
            if (aggColumnsStringBuilder.length() > 0) {
                // all key columns should be group by keys, so remove the last ", " characters
                removeLastTwoChars(keyColumnsStringBuilder);
            }
            createMvSqlBuilder.append(
                    String.format(" from %s group by %s", baseTableName, keyColumnsStringBuilder));
        } else {
            for (Column col : columns) {
                createMvSqlBuilder.append(String.format("%s, ", col.getName()));
            }
            removeLastTwoChars(createMvSqlBuilder);
            createMvSqlBuilder.append(String.format(" from %s", baseTableName));
        }

        return createMvSqlBuilder.toString();
    }

    private void removeLastTwoChars(StringBuilder stringBuilder) {
        if (stringBuilder.length() >= 2) {
            stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        }
    }
}
