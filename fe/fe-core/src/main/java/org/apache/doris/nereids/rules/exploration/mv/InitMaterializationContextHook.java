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
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector.TableCollectorContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * If enable query rewrite with mv, should init materialization context after analyze
 */
public class InitMaterializationContextHook implements PlannerHook {

    public static final Logger LOG = LogManager.getLogger(InitMaterializationContextHook.class);
    public static final InitMaterializationContextHook INSTANCE = new InitMaterializationContextHook();

    @Override
    public void afterAnalyze(NereidsPlanner planner) {
        initMaterializationContext(planner.getCascadesContext());
    }

    /**
     * init materialization context
     */
    @VisibleForTesting
    public void initMaterializationContext(CascadesContext cascadesContext) {
        if (!cascadesContext.getConnectContext().getSessionVariable().isEnableMaterializedViewRewrite()) {
            return;
        }
        TableCollectorContext collectorContext = new TableCollectorContext(Sets.newHashSet(), true);
        try {
            Plan rewritePlan = cascadesContext.getRewritePlan();
            // Keep use one connection context when in query, if new connect context,
            // the ConnectionContext.get() will change
            collectorContext.setConnectContext(cascadesContext.getConnectContext());
            rewritePlan.accept(TableCollector.INSTANCE, collectorContext);
        } catch (Exception e) {
            LOG.warn(String.format("MaterializationContext init table collect fail, current queryId is %s",
                    cascadesContext.getConnectContext().getQueryIdentifier()), e);
            return;
        }
        Set<TableIf> collectedTables = collectorContext.getCollectedTables();
        if (collectedTables.isEmpty()) {
            return;
        }

        if (cascadesContext.getConnectContext().getSessionVariable()
                .isEnableSyncMvCostBasedRewrite()) {
            for (TableIf tableIf : collectedTables) {
                if (tableIf instanceof OlapTable) {
                    for (SyncMaterializationContext context : createSyncMvContexts(
                            (OlapTable) tableIf, cascadesContext)) {
                        cascadesContext.addMaterializationContext(context);
                    }
                }
            }
        }

        List<BaseTableInfo> usedBaseTables =
                collectedTables.stream().map(BaseTableInfo::new).collect(Collectors.toList());
        Set<MTMV> availableMTMVs = Env.getCurrentEnv().getMtmvService().getRelationManager()
                .getAvailableMTMVs(usedBaseTables, cascadesContext.getConnectContext());
        if (availableMTMVs.isEmpty()) {
            LOG.debug(String.format("Enable materialized view rewrite but availableMTMVs is empty, current queryId "
                    + "is %s", cascadesContext.getConnectContext().getQueryIdentifier()));
            return;
        }
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
                mvStructInfo.getRelations().forEach(relation -> tableBitSetInCurrentCascadesContext.set(
                        cascadesContext.getStatementContext().getTableId(relation.getTable()).asInt()));
                cascadesContext.addMaterializationContext(new AsyncMaterializationContext(materializedView,
                        mtmvCache.getLogicalPlan(), mtmvCache.getOriginalPlan(), ImmutableList.of(),
                        ImmutableList.of(), cascadesContext,
                        mtmvCache.getStructInfo().withTableBitSet(tableBitSetInCurrentCascadesContext)));
            } catch (Exception e) {
                LOG.warn(String.format("MaterializationContext init mv cache generate fail, current queryId is %s",
                        cascadesContext.getConnectContext().getQueryIdentifier()), e);
            }
        }
    }

    private List<SyncMaterializationContext> createSyncMvContexts(OlapTable olapTable,
            CascadesContext cascadesContext) {
        int indexNumber = olapTable.getIndexNumber();
        List<SyncMaterializationContext> contexts = new ArrayList<>(indexNumber);
        long baseIndexId = olapTable.getBaseIndexId();
        int keyCount = 0;
        for (Column column : olapTable.getFullSchema()) {
            keyCount += column.isKey() ? 1 : 0;
        }
        for (Map.Entry<String, Long> entry : olapTable.getIndexNameToId().entrySet()) {
            long indexId = entry.getValue();
            try {
                // when doing schema change, a shadow index would be created and put together with mv indexes
                // we must roll out these unexpected shadow indexes here
                if (indexId != baseIndexId && !olapTable.isShadowIndex(indexId)) {
                    MaterializedIndexMeta meta = olapTable.getIndexMetaByIndexId(entry.getValue());
                    String createMvSql;
                    if (meta.getDefineStmt() != null) {
                        // get the original create mv sql
                        createMvSql = meta.getDefineStmt().originStmt;
                    } else {
                        // it's rollup, need assemble create mv sql manually
                        if (olapTable.getKeysType() == KeysType.AGG_KEYS) {
                            createMvSql = assembleCreateMvSqlForAggTable(olapTable.getQualifiedName(),
                                    entry.getKey(), meta.getSchema(false), keyCount);
                        } else {
                            createMvSql =
                                    assembleCreateMvSqlForDupOrUniqueTable(olapTable.getQualifiedName(),
                                            entry.getKey(), meta.getSchema(false));
                        }
                    }
                    if (createMvSql != null) {
                        Optional<String> querySql =
                                new NereidsParser().parseForSyncMv(createMvSql);
                        if (!querySql.isPresent()) {
                            LOG.warn(String.format("can't parse %s ", createMvSql));
                            continue;
                        }
                        MTMVCache mtmvCache = MaterializedViewUtils.createMTMVCache(querySql.get(),
                                cascadesContext.getConnectContext());
                        contexts.add(new SyncMaterializationContext(mtmvCache.getLogicalPlan(),
                                mtmvCache.getOriginalPlan(), olapTable, meta.getIndexId(), entry.getKey(),
                                cascadesContext, mtmvCache.getStatistics()));
                    } else {
                        LOG.warn(String.format("can't assemble create mv sql for index ", entry.getKey()));
                    }
                }
            } catch (Exception exception) {
                LOG.warn(String.format("createSyncMvContexts exception, index id is %s, index name is %s",
                        entry.getValue(), entry.getValue()), exception);
            }
        }
        return contexts;
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
