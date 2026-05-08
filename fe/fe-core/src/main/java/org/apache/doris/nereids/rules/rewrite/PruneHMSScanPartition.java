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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.HMSPartitionsUtil;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruneExpressionExtractor;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.rules.expression.rules.PredicateRewriteForPartitionFilter;
import org.apache.doris.nereids.rules.expression.rules.PredicateRewriteForPartitionPrune;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.BDPAuthContext;
import org.apache.doris.qe.InternalQueryExecutionException;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Used to prune partition of file scan for hms table.
 */
public class PruneHMSScanPartition extends OneRewriteRuleFactory {

    private static final Logger LOG = LogManager.getLogger(PruneHMSScanPartition.class);

    private static final String QUERY_FILTER_PARTITION_SQL = "SELECT * FROM ${catalogName}.${dbName}"
            + ".${tblName}$partitions where ${filterSql}";

    @Override
    public Rule build() {
        return logicalFilter(logicalFileScan()).whenNot(p -> p.child().getSelectedPartitions().isPruned)
                .when(p -> p.child().getTable() instanceof HMSExternalTable)
                .thenApply(ctx -> {
                    LogicalFilter<LogicalFileScan> filter = ctx.root;
                    LogicalFileScan scan = filter.child();
                    HMSExternalTable tbl = (HMSExternalTable) scan.getTable();

                    SelectedPartitions selectedPartitions;
                    Set<Expression> conjuncts;
                    if (tbl.supportInternalPartitionPruned()) {
                        selectedPartitions = pruneHMSPartitions(tbl, filter, scan, ctx.cascadesContext);
                        conjuncts = getConjunctsWithoutPartitionPredicate(scan, filter);
                    } else {
                        // set isPruned so that it won't go pass the partition prune again
                        selectedPartitions = new SelectedPartitions(0, ImmutableMap.of(), true);
                        conjuncts = filter.getConjuncts();
                    }
                    LogicalFileScan rewrittenScan = scan.withSelectedPartitions(selectedPartitions);
                    if (conjuncts.isEmpty()) {
                        return rewrittenScan;
                    }
                    return new LogicalFilter<>(conjuncts, rewrittenScan);
                }).toRule(RuleType.HMS_SCAN_PARTITION_PRUNE);
    }

    /**
     *  get conjuncts without partition predicate
     *
     */
    public Set<Expression> getConjunctsWithoutPartitionPredicate(LogicalFileScan fileScan,
                                                                 LogicalFilter<LogicalFileScan> filter) {
        Map<String, Slot> scanOutput = fileScan.getOutput()
                .stream()
                .collect(Collectors.toMap(slot -> slot.getName().toLowerCase(), Function.identity()));
        Set<Slot> partitionSlots = ((HMSExternalTable) fileScan.getTable()).getPartitionColumns()
                .stream()
                .map(column -> scanOutput.get(column.getName().toLowerCase()))
                .collect(Collectors.toSet());
        PartitionPruneExpressionExtractor.ExpressionEvaluableDetector detector =
                new PartitionPruneExpressionExtractor.ExpressionEvaluableDetector(partitionSlots);
        Set<Expression> result = Sets.newHashSet();
        for (Expression expression : filter.getConjuncts()) {
            if (!detector.detect(expression)) {
                result.add(expression);
            }
        }
        return result;
    }

    @VisibleForTesting
    protected Map<String, PartitionItem> getSelectedPartitionItems(HMSExternalTable hiveTable,
                                                                 List<String> partitionColumnNames,
                                                                 Expression partitionPredicate,
                                                                 int partitionNum, HiveMetaStoreCache cache) {
        Set<Expression> conjuncts = Sets.newHashSet();
        for (Expression expression : ExpressionUtils.extractConjunction(partitionPredicate)) {
            if (HMSPartitionsUtil.isFilterSupportedByListPartitions(expression)) {
                conjuncts.add(expression);
            }
        }
        if (!conjuncts.isEmpty()) {
            return cache.getPartitionValuesByFilter(hiveTable,
                ExpressionUtils.and(conjuncts).toSql(), partitionColumnNames, hiveTable.getPartitionColumnTypes());
        } else {
            return HMSPartitionsUtil.getPartitionItems(cache, hiveTable, partitionNum);
        }
    }

    protected static List<ResultRow> executePartitionFilterQuery(HMSExternalTable hiveTbl,
                                                                 AutoCloseConnectContext r, String sql) {
        List<ResultRow> partitionRows;
        try {
            partitionRows = new StmtExecutor(r.connectContext, sql).executeInternalQuery();
        } catch (Exception e) {
            String errorMessage = "prune hive partitions failed for "
                    + hiveTbl.getDbName() + "." + hiveTbl.getName();
            LOG.warn(errorMessage, e);
            throw new InternalQueryExecutionException(errorMessage, e);
        }
        return partitionRows;
    }

    private SelectedPartitions pruneHMSPartitions(HMSExternalTable hiveTable,
            LogicalFilter<LogicalFileScan> filter, LogicalFileScan scan, CascadesContext ctx) {
        Map<String, PartitionItem> selectedPartitionItems = Maps.newHashMap();
        if (CollectionUtils.isEmpty(hiveTable.getPartitionColumns())) {
            // non partitioned table, return NOT_PRUNED.
            // non partition table will be handled in HiveScanNode.
            return SelectedPartitions.NOT_PRUNED;
        }
        Map<String, Slot> scanOutput = scan.getOutput()
                .stream()
                .collect(Collectors.toMap(slot -> slot.getName().toLowerCase(), Function.identity()));
        List<Slot> partitionSlots = hiveTable.getPartitionColumns()
                .stream()
                .map(column -> scanOutput.get(column.getName().toLowerCase()))
                .collect(Collectors.toList());
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) hiveTable.getCatalog());
        int partitionNum = cache.getPartitionNum(hiveTable);
        boolean allPartitionValuesByFilter = true;
        Expression partitionPredicate = null;
        try {
            partitionPredicate = PartitionPruneExpressionExtractor.extract(filter.getPredicate(),
                ImmutableSet.copyOf(partitionSlots), ctx);
            List<String> partitionColumnNames = partitionSlots.stream().map(e -> e.getName())
                    .collect(Collectors.toList());
            partitionPredicate = PredicateRewriteForPartitionPrune.rewrite(partitionPredicate, ctx);
            partitionPredicate = PredicateRewriteForPartitionFilter.rewrite(partitionPredicate, ctx);
            if (BooleanLiteral.TRUE.equals(partitionPredicate)) {
                HMSPartitionsUtil.checkSelectedPartitionNumLimit(hiveTable, partitionNum);
                selectedPartitionItems = getSelectedPartitionItems(hiveTable, partitionColumnNames, partitionPredicate,
                        partitionNum, cache);
            } else if (BooleanLiteral.FALSE.equals(partitionPredicate) || partitionPredicate.isNullLiteral()) {
                // do nothing
            } else {
                if (HMSPartitionsUtil.isFilterSupportedByListPartitions(partitionPredicate)) {
                    selectedPartitionItems = cache.getPartitionValuesByFilter(hiveTable,
                        partitionPredicate.toSql(), partitionColumnNames, hiveTable.getPartitionColumnTypes());
                } else {
                    Map<String, PartitionItem> nameToPartitionItem = getSelectedPartitionItems(hiveTable,
                            partitionColumnNames, partitionPredicate, partitionNum, cache);
                    List<String> prunedPartitions = Lists.newArrayList();
                    if (PartitionPruner.tryPrune(partitionSlots, partitionPredicate, nameToPartitionItem,
                            prunedPartitions, ctx)) {
                        for (String name : prunedPartitions) {
                            selectedPartitionItems.put(name, nameToPartitionItem.get(name));
                        }
                    } else {
                        allPartitionValuesByFilter = false;
                    }
                }
            }
        } catch (Exception e) {
            // for some unexpected cases listPartitionsByFilter may not support
            LOG.warn("get selected partition items by listPartitionsByFilter failed, filter sql is "
                    + partitionPredicate.toSql() + ", use getPartitionValues instead");
            allPartitionValuesByFilter = false;
        }
        if (!allPartitionValuesByFilter) {
            BDPAuthContext bdpAuthContext = BDPAuthContext.get();
            BDPAuthContext partitionFilterAuthContext = hiveTable.isViewBased() ? new BDPAuthContext(
                    bdpAuthContext.getErp(), bdpAuthContext.getSource(), bdpAuthContext.getHadoopUserName(),
                    bdpAuthContext.getUserToken(), true, bdpAuthContext.getUserType(),
                    bdpAuthContext.getBusinessLine()) : bdpAuthContext;
            try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(partitionFilterAuthContext)) {
                partitionPredicate = PartitionPruneExpressionExtractor.extract(filter.getPredicate(),
                    ImmutableSet.copyOf(partitionSlots), ctx);
                Map<String, String> params = new HashMap<>();
                params.put("catalogName", hiveTable.getCatalog().getName());
                params.put("dbName", hiveTable.getDbName());
                params.put("tblName", hiveTable.getName());
                params.put("filterSql", partitionPredicate.toSql());
                StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
                String sql = stringSubstitutor.replace(QUERY_FILTER_PARTITION_SQL);
                List<ResultRow> partitionRows = executePartitionFilterQuery(hiveTable, r, sql);
                selectedPartitionItems = Maps.newHashMapWithExpectedSize(partitionRows.size());
                for (ResultRow partition : partitionRows) {
                    List<PartitionValue> values = Lists.newArrayListWithExpectedSize(partitionSlots.size());
                    for (String partitionValue : partition.getValues()) {
                        values.add(new PartitionValue(partitionValue,
                                HiveMetaStoreCache.HIVE_DEFAULT_PARTITION.equals(partitionValue)));
                    }
                    try {
                        PartitionKey partitionKey = PartitionKey.createListPartitionKeyWithTypes(values,
                                hiveTable.getPartitionColumnTypes(), true);
                        String partitionName = IntStream.range(0, partitionSlots.size())
                                .mapToObj(i -> partitionSlots.get(i).getName() + "=" + values.get(i).getStringValue())
                                .collect(Collectors.joining("/"));
                        selectedPartitionItems.put(partitionName,
                            new ListPartitionItem(Lists.newArrayList(partitionKey)));
                    } catch (AnalysisException e) {
                        throw new InternalQueryExecutionException(
                            String.format("failed to convert hive partition %s to list partition in catalog %s",
                            e, partition.getValues(), hiveTable.getCatalog().getName()), e);
                    }
                }
            } catch (Exception e) {
                LOG.warn("failed to fetch filter partitions for " + hiveTable.getDbName() + "." + hiveTable.getName());
                throw e;
            }
        }
        return new SelectedPartitions(partitionNum, selectedPartitionItems, true);
    }
}
