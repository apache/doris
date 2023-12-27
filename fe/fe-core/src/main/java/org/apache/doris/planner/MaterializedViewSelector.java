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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.analysis.VirtualSlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.mvrewrite.MVExprEquivalent;
import org.apache.doris.rewrite.mvrewrite.MVSelectFailedException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * The new materialized view selector supports SPJ<->SPJG.
 * It means this selector can judge any combination of those two options SPJ and
 * SPJG.
 * For example, the type of query is SPJG while the type of MV is SPJ.
 * At the same time, it is compatible with all the features of the old version.
 *
 * What is SPJ and SPJG?
 * The SPJ query is "Select Projection and Join" such as:
 * select t1.c1 from t1, t2 where t1.c2=t2.c2 and t1.c3=1;
 * The SPJG query is "Select Projection Join and Group-by" such as:
 * select t1.c1, sum(t2.c1) from t1, t2 where t1.c2=t2.c2 and t1.c3=1 group by
 * t1.c 1;
 */
public class MaterializedViewSelector {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewSelector.class);

    private final SelectStmt selectStmt;
    private final Analyzer analyzer;

    /**
     * The key of following maps is table id.
     * The value of following maps is column names.
     * `columnNamesInPredicates` means that the column names in where clause.
     * And so on.
     */
    private Map<Long, Set<String>> columnNamesInPredicates = Maps.newHashMap();
    private boolean isSPJQuery;
    private Map<Long, Set<String>> columnNamesInGrouping = Maps.newHashMap();
    private Map<Long, Set<FunctionCallExpr>> aggColumnsInQuery = Maps.newHashMap();
    // private Map<String, Set<AggregatedColumn>> aggregateColumnsInQuery =
    // Maps.newHashMap();
    private Map<Long, Set<String>> columnNamesInQueryOutput = Maps.newHashMap();

    private boolean disableSPJGView;

    // The Following 2 variables should be reset each time before calling
    // selectBestMV();
    // Unlike the "isPreAggregation" in OlapScanNode which defaults to false,
    // it defaults to true here. It is because in this class, we started to choose
    // MV under the premise
    // that the default base tables are duplicate key tables. For the aggregation
    // key table,
    // this variable will be set to false compensatively at the end.
    private boolean isPreAggregation = true;
    private String reasonOfDisable;

    public MaterializedViewSelector(SelectStmt selectStmt, Analyzer analyzer) {
        this.selectStmt = selectStmt;
        this.analyzer = analyzer;
        init();
    }

    /**
     * There are two stages to choosing the best MV.
     * Phase 1: Predicates = computeCandidateMVs
     * According to aggregation and column information in the select stmt,
     * the candidate MVs that meets the query conditions are selected.
     * Phase 2: Priorities = computeBestMVByCost
     * According to prefix index and row count in candidate MVs,
     * the best MV is selected.
     *
     * @param scanNode
     * @return
     */
    public BestIndexInfo selectBestMV(ScanNode scanNode) throws UserException {
        resetPreAggregationVariables();
        long start = System.currentTimeMillis();
        Preconditions.checkState(scanNode instanceof OlapScanNode);
        OlapScanNode olapScanNode = (OlapScanNode) scanNode;

        if (olapScanNode.getOlapTable().getVisibleIndex().size() == 1) {
            return new BestIndexInfo(olapScanNode.getOlapTable().getBaseIndexId(), isPreAggregation, reasonOfDisable);
        }

        Map<Long, List<Column>> candidateIndexIdToSchema = predicates(olapScanNode);
        if (candidateIndexIdToSchema.keySet().size() == 0) {
            return null;
        }
        long bestIndexId = priorities(olapScanNode, candidateIndexIdToSchema);
        LOG.debug("The best materialized view is {} for scan node {} in query {}, "
                + "isPreAggregation: {}, reasonOfDisable: {}, cost {}",
                bestIndexId, scanNode.getId(), selectStmt.toSql(), isPreAggregation, reasonOfDisable,
                (System.currentTimeMillis() - start));
        return new BestIndexInfo(bestIndexId, isPreAggregation, reasonOfDisable);
    }

    private void resetPreAggregationVariables() {
        isPreAggregation = true;
        reasonOfDisable = null;
    }

    private Map<Long, List<Column>> predicates(OlapScanNode scanNode) throws AnalysisException {
        // Step1: all predicates is compensating predicates
        Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta = scanNode.getOlapTable().getVisibleIndexIdToMeta();
        OlapTable table = scanNode.getOlapTable();
        Preconditions.checkState(table != null);
        long tableId = table.getId();

        boolean selectBaseIndex = false;
        for (Expr expr : selectStmt.getAllExprs()) {
            if (expr.isBound(scanNode.getTupleId())
                    && selectStmt.isDisableTuplesMVRewriter(selectStmt.getExprFromAliasSMap(expr))) {
                selectBaseIndex = true;
            }
        }

        // Step2: check all columns in compensating predicates are available in the view output
        checkCompensatingPredicates(columnNamesInPredicates.get(tableId), candidateIndexIdToMeta, selectBaseIndex,
                scanNode.getTupleId());
        // Step3: group by list in query is the subset of group by list in view or view contains no aggregation
        checkGrouping(table, columnNamesInGrouping.get(tableId), candidateIndexIdToMeta, selectBaseIndex,
                scanNode.getTupleId());
        // Step4: aggregation functions are available in the view output
        checkAggregationFunction(table, aggColumnsInQuery.get(tableId), candidateIndexIdToMeta, scanNode.getTupleId());
        // Step5: columns required to compute output expr are available in the view output
        checkOutputColumns(columnNamesInQueryOutput.get(tableId), candidateIndexIdToMeta, selectBaseIndex,
                scanNode.getTupleId());
        // Step6: if table type is aggregate and the candidateIndexIdToSchema is empty,
        if ((table.getKeysType() == KeysType.AGG_KEYS || (table.getKeysType() == KeysType.UNIQUE_KEYS
                && !table.getTableProperty().getEnableUniqueKeyMergeOnWrite()))
                && candidateIndexIdToMeta.size() == 0) {
            // the base index will be added in the candidateIndexIdToSchema.
            /**
             * In Doris, it is allowed that the aggregate table should be scanned directly
             * while there is no aggregation info in query.
             * For example:
             * Aggregate tableA: k1, k2, sum(v1)
             * Query: select k1, k2, v1 from tableA
             * Allowed
             * Result: same as select k1, k2, sum(v1) from tableA group by t1, t2
             *
             * However, the query should not be selector normally.
             * The reason is that the level of group by in tableA is upper then the level of
             * group by in query.
             * So, we need to compensate those kinds of index in following step.
             *
             */
            compensateCandidateIndex(candidateIndexIdToMeta, scanNode.getOlapTable().getVisibleIndexIdToMeta(),
                    table);
            checkOutputColumns(columnNamesInQueryOutput.get(tableId), candidateIndexIdToMeta, selectBaseIndex,
                    scanNode.getTupleId());
        }
        Map<Long, List<Column>> result = Maps.newHashMap();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : candidateIndexIdToMeta.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getSchema());
        }
        // For query like `select v:a from tbl` when column v is variant type but v:a is not expicity
        // in index, so the above check will filter all index. But we should at least choose the base
        // index at present.TODO we should better handle it.
        LOG.debug("result {}, has variant col {}, tuple {}", result,
                    analyzer.getTupleDesc(scanNode.getTupleId()).hasVariantCol(),
                    analyzer.getTupleDesc(scanNode.getTupleId()).toString());
        if (result.keySet().size() == 0 && scanNode.getOlapTable()
                    .getBaseSchema().stream().anyMatch(column -> column.getType().isVariantType())) {
            LOG.info("Using base schema");
            result.put(scanNode.getOlapTable().getBaseIndexId(), scanNode.getOlapTable().getBaseSchema());
        }
        return result;
    }

    private long priorities(OlapScanNode scanNode, Map<Long, List<Column>> candidateIndexIdToSchema) {
        OlapTable tbl = scanNode.getOlapTable();
        Long v2RollupIndexId = tbl.getSegmentV2FormatIndexId();
        if (v2RollupIndexId != null) {
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null && connectContext.getSessionVariable().isUseV2Rollup()) {
                // if user set `use_v2_rollup` variable to true, and there is a segment v2
                // rollup,
                // just return the segment v2 rollup, because user want to check the v2 format
                // data.
                if (candidateIndexIdToSchema.containsKey(v2RollupIndexId)) {
                    return v2RollupIndexId;
                }
            } else {
                // `use_v2_rollup` is not set, so v2 format rollup should not be selected,
                // remove it from
                // candidateIndexIdToSchema
                candidateIndexIdToSchema.remove(v2RollupIndexId);
            }
        }

        // Step1: the candidate indexes that satisfies the most prefix index
        final Set<String> equivalenceColumns = Sets.newHashSet();
        final Set<String> unequivalenceColumns = Sets.newHashSet();
        scanNode.collectColumns(analyzer, equivalenceColumns, unequivalenceColumns);
        Set<Long> indexesMatchingBestPrefixIndex = matchBestPrefixIndex(candidateIndexIdToSchema, equivalenceColumns,
                unequivalenceColumns);

        // Step2: the best index that satisfies the least number of rows
        return selectBestRowCountIndex(indexesMatchingBestPrefixIndex, scanNode.getOlapTable(), scanNode
                .getSelectedPartitionIds());
    }

    private Set<Long> matchBestPrefixIndex(Map<Long, List<Column>> candidateIndexIdToSchema,
            Set<String> equivalenceColumns,
            Set<String> unequivalenceColumns) {
        if (equivalenceColumns.size() == 0 && unequivalenceColumns.size() == 0) {
            return candidateIndexIdToSchema.keySet();
        }
        Set<Long> indexesMatchingBestPrefixIndex = Sets.newHashSet();
        int maxPrefixMatchCount = 0;
        for (Map.Entry<Long, List<Column>> entry : candidateIndexIdToSchema.entrySet()) {
            int prefixMatchCount = 0;
            long indexId = entry.getKey();
            List<Column> indexSchema = entry.getValue();
            for (Column col : indexSchema) {
                if (equivalenceColumns.contains(col.getName())) {
                    prefixMatchCount++;
                } else if (unequivalenceColumns.contains(col.getName())) {
                    // Unequivalence predicate's columns can match only first column in rollup.
                    prefixMatchCount++;
                    break;
                } else {
                    break;
                }
            }

            if (prefixMatchCount == maxPrefixMatchCount) {
                LOG.debug("find a equal prefix match index {}. match count: {}", indexId, prefixMatchCount);
                indexesMatchingBestPrefixIndex.add(indexId);
            } else if (prefixMatchCount > maxPrefixMatchCount) {
                LOG.debug("find a better prefix match index {}. match count: {}", indexId, prefixMatchCount);
                maxPrefixMatchCount = prefixMatchCount;
                indexesMatchingBestPrefixIndex.clear();
                indexesMatchingBestPrefixIndex.add(indexId);
            }
        }
        LOG.debug("Those mv match the best prefix index:" + Joiner.on(",").join(indexesMatchingBestPrefixIndex));
        return indexesMatchingBestPrefixIndex;
    }

    private long selectBestRowCountIndex(Set<Long> indexesMatchingBestPrefixIndex, OlapTable olapTable,
            Collection<Long> partitionIds) {
        long minRowCount = Long.MAX_VALUE;
        long selectedIndexId = 0;
        for (Long indexId : indexesMatchingBestPrefixIndex) {
            long rowCount = 0;
            for (Long partitionId : partitionIds) {
                rowCount += olapTable.getPartition(partitionId).getIndex(indexId).getRowCount();
            }
            LOG.debug("rowCount={} for table={}", rowCount, indexId);
            if (rowCount < minRowCount) {
                minRowCount = rowCount;
                selectedIndexId = indexId;
            } else if (rowCount == minRowCount) {
                // check column number, select one minimum column number
                int selectedColumnSize = olapTable.getSchemaByIndexId(selectedIndexId).size();
                int currColumnSize = olapTable.getSchemaByIndexId(indexId).size();
                if (currColumnSize < selectedColumnSize) {
                    selectedIndexId = indexId;
                }
            }
        }
        return selectedIndexId;
    }

    // Step2: check all columns in compensating predicates are available in the view
    // output
    private void checkCompensatingPredicates(Set<String> columnsInPredicates,
            Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta, boolean selectBaseIndex, TupleId tid)
            throws AnalysisException {
        Iterator<Map.Entry<Long, MaterializedIndexMeta>> iterator = candidateIndexIdToMeta.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MaterializedIndexMeta> entry = iterator.next();
            Set<String> indexNonAggregatedColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            List<Column> indexColumn = Lists.newArrayList();
            entry.getValue().getSchema().stream().filter(column -> !column.isAggregated())
                    .forEach(column -> indexColumn.add(column));

            indexColumn.forEach(column -> indexNonAggregatedColumnNames
                    .add(MaterializedIndexMeta.normalizeName(column.getName())));
            List<Expr> indexExprs = new ArrayList<Expr>();
            indexColumn
                    .forEach(column -> indexExprs.add(column.getDefineExpr()));
            indexExprs.removeIf(Objects::isNull);

            if (selectBaseIndex) {
                // Base index have no define expr.
                if (!indexExprs.isEmpty()) {
                    iterator.remove();
                }
                continue;
            }

            if (entry.getValue().getWhereClause() != null) {
                if (selectStmt.getOriginalWhereClause() == null || !selectStmt.getOriginalWhereClause()
                        .containsSubPredicate(entry.getValue().getWhereClause())) {
                    iterator.remove();
                }
                continue;
            }

            if (columnsInPredicates == null) {
                continue;
            }

            List<Expr> predicateExprs = Lists.newArrayList();
            if (selectStmt.getWhereClause() != null) {
                predicateExprs.add(selectStmt.getExprFromAliasSMap(selectStmt.getWhereClause()));
            }

            for (TableRef tableRef : selectStmt.getTableRefs()) {
                if (tableRef.getOnClause() == null) {
                    continue;
                }
                predicateExprs.add(selectStmt.getExprFromAliasSMap(tableRef.getOnClause()));
            }

            if (indexNonAggregatedColumnNames.containsAll(columnsInPredicates)) {
                continue;
            }

            if (!matchAllExpr(predicateExprs, indexExprs, tid)) {
                iterator.remove();
            }
        }
        LOG.debug("Those mv pass the test of compensating predicates:"
                + Joiner.on(",").join(candidateIndexIdToMeta.keySet()));
    }

    /**
     * View Query result
     * SPJ SPJG OR SPJ pass
     * SPJG SPJ fail
     * SPJG SPJG pass
     * 1. grouping columns in query is subset of grouping columns in view
     * 2. the empty grouping columns in query is subset of all of views
     *
     * @param columnsInGrouping
     * @param candidateIndexIdToMeta
     * @throws AnalysisException
     */

    // Step3: group by list in query is the subset of group by list in view or view
    // contains no aggregation
    private void checkGrouping(OlapTable table, Set<String> columnsInGrouping,
            Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta, boolean selectBaseIndex, TupleId tid)
            throws AnalysisException {
        Iterator<Map.Entry<Long, MaterializedIndexMeta>> iterator = candidateIndexIdToMeta.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MaterializedIndexMeta> entry = iterator.next();
            Set<String> indexNonAggregatedColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            MaterializedIndexMeta candidateIndexMeta = entry.getValue();
            List<Column> candidateIndexSchema = Lists.newArrayList();
            entry.getValue().getSchema().stream().filter(column -> !column.isAggregated())
                    .forEach(column -> candidateIndexSchema.add(column));
            candidateIndexSchema
                    .forEach(column -> indexNonAggregatedColumnNames
                            .add(MaterializedIndexMeta.normalizeName(column.getName())));
            /*
             * If there is no aggregated column in duplicate index, the index will be SPJ.
             * For example:
             * duplicate table (k1, k2, v1)
             * duplicate mv index (k1, v1)
             * When the candidate index is SPJ type, it passes the verification directly
             * If there is no aggregated column in aggregate index, the index will be
             * deduplicate index.
             * For example:
             * duplicate table (k1, k2, v1 sum)
             * aggregate mv index (k1, k2)
             * This kind of index is SPJG which same as select k1, k2 from aggregate_table
             * group by k1, k2.
             * It also need to check the grouping column using following steps.
             * ISSUE-3016, MaterializedViewFunctionTest: testDeduplicateQueryInAgg
             */
            boolean noNeedAggregation = candidateIndexMeta.getKeysType() == KeysType.DUP_KEYS
                    || (candidateIndexMeta.getKeysType() == KeysType.UNIQUE_KEYS
                            && table.getTableProperty().getEnableUniqueKeyMergeOnWrite());
            if (indexNonAggregatedColumnNames.size() == candidateIndexSchema.size() && noNeedAggregation) {
                continue;
            }
            // When the query is SPJ type but the candidate index is SPJG type, it will not
            // pass directly.
            if (isSPJQuery || disableSPJGView) {
                iterator.remove();
                continue;
            }
            // The query is SPJG. The candidate index is SPJG too.
            // The grouping columns in query is empty. For example: select sum(A) from T
            if (columnsInGrouping == null) {
                continue;
            }

            List<Expr> indexExprs = Lists.newArrayList();
            candidateIndexSchema.forEach(column -> indexExprs.add(column.getDefineExpr()));
            indexExprs.removeIf(Objects::isNull);

            if (selectBaseIndex) {
                // Base index have no define expr.
                if (!indexExprs.isEmpty()) {
                    iterator.remove();
                }
                continue;
            }

            List<Expr> groupingExprs = Lists.newArrayList();
            for (Expr expr : selectStmt.getAggInfo().getGroupingExprs()) {
                groupingExprs.add(selectStmt.getExprFromAliasSMap(expr));
            }

            // The grouping columns in query must be subset of the grouping columns in view
            if (indexNonAggregatedColumnNames.containsAll(columnsInGrouping)) {
                continue;
            }

            if (!matchAllExpr(groupingExprs, indexExprs, tid)) {
                iterator.remove();
            }
        }
        LOG.debug("Those mv pass the test of grouping:"
                + Joiner.on(",").join(candidateIndexIdToMeta.keySet()));
    }

    // Step4: aggregation functions are available in the view output
    private void checkAggregationFunction(OlapTable table, Set<FunctionCallExpr> aggregatedColumnsInQueryOutput,
            Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta, TupleId tid) throws AnalysisException {
        boolean haveMvSlot = false;
        if (aggregatedColumnsInQueryOutput != null) {
            for (FunctionCallExpr expr : aggregatedColumnsInQueryOutput) {
                if (expr.haveMvSlot(tid)) {
                    haveMvSlot = true;
                }
            }
        }

        Iterator<Map.Entry<Long, MaterializedIndexMeta>> iterator = candidateIndexIdToMeta.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MaterializedIndexMeta> entry = iterator.next();
            MaterializedIndexMeta candidateIndexMeta = entry.getValue();
            List<FunctionCallExpr> indexAggColumnExpsList = mvAggColumnsToExprList(candidateIndexMeta);
            // When the candidate index is SPJ type, it passes the verification directly
            boolean noNeedAggregation = candidateIndexMeta.getKeysType() == KeysType.DUP_KEYS
                    || (candidateIndexMeta.getKeysType() == KeysType.UNIQUE_KEYS
                            && table.getTableProperty().getEnableUniqueKeyMergeOnWrite());
            List<Expr> indexExprs = new ArrayList<Expr>();
            candidateIndexMeta.getSchema().forEach(column -> indexExprs.add(column.getDefineExpr()));
            indexExprs.removeIf(Objects::isNull);

            if (indexExprs.isEmpty() && !haveMvSlot && noNeedAggregation) {
                continue;
            }
            // When the query is SPJ type but the candidate index is SPJG type, it will not
            // pass directly.
            if (isSPJQuery && !indexAggColumnExpsList.isEmpty() || disableSPJGView) {
                iterator.remove();
                continue;
            }
            if (aggregatedColumnsInQueryOutput != null
                    && matchAllExpr(new ArrayList<>(aggregatedColumnsInQueryOutput), indexExprs, tid)) {
                continue;
            }

            // The query is SPJG. The candidate index is SPJG too.
            /*
             * Situation1: The query is deduplicate SPJG when aggregatedColumnsInQueryOutput
             * is null.
             * For example: select a , b from table group by a, b
             * The aggregation function check should be pass directly when MV is SPJG.
             */
            if (aggregatedColumnsInQueryOutput == null) {
                continue;
            }
            // The aggregated columns in query output must be subset of the aggregated
            // columns in view
            if (!aggFunctionsMatchAggColumns(aggregatedColumnsInQueryOutput, indexAggColumnExpsList)) {
                iterator.remove();
            }
        }
        LOG.debug("Those mv pass the test of aggregation function:"
                + Joiner.on(",").join(candidateIndexIdToMeta.keySet()));
    }

    private boolean matchAllExpr(List<Expr> exprs, List<Expr> indexExprs, TupleId tid)
            throws AnalysisException {
        for (Expr expr : exprs) {
            if (expr == null) {
                throw new AnalysisException("match expr input null");
            }
            if (expr.toSqlWithoutTbl() == null) {
                throw new AnalysisException("expr.toSqlWithoutTbl() is null, expr.toSql()=" + expr.toSql());
            }

            if (expr instanceof VirtualSlotRef) {
                continue;
            }

            if (expr.matchExprs(indexExprs, selectStmt, false, analyzer.getTupleDesc(tid))) {
                continue;
            }
            return false;
        }
        return true;
    }

    // Step5: columns required to compute output expr are available in the view
    // output
    private void checkOutputColumns(Set<String> columnNamesInQueryOutput,
            Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta, boolean selectBaseIndex,
            TupleId tid)
            throws AnalysisException {
        if (columnNamesInQueryOutput == null) {
            return;
        }
        Set<String> queryColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        List<Expr> exprs = Lists.newArrayList();
        for (Expr expr : selectStmt.getAllExprs()) {
            exprs.add(selectStmt.getExprFromAliasSMap(expr));
        }
        columnNamesInQueryOutput
                .forEach(name -> queryColumnNames.add(CreateMaterializedViewStmt.mvColumnBreaker(name)));

        if (selectBaseIndex) {
            for (Expr expr : exprs) {
                if (expr.haveMvSlot(tid)) {
                    throw new MVSelectFailedException("need selectBaseIndex but have mv expr");
                }
            }
        }

        Iterator<Map.Entry<Long, MaterializedIndexMeta>> iterator = candidateIndexIdToMeta.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MaterializedIndexMeta> entry = iterator.next();
            List<Column> candidateIndexSchema = entry.getValue().getSchema();

            List<Expr> indexExprs = new ArrayList<Expr>();
            candidateIndexSchema.forEach(column -> indexExprs.add(column.getDefineExpr()));
            indexExprs.removeIf(Objects::isNull);

            Set<String> indexColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            candidateIndexSchema
                    .forEach(column -> indexColumnNames.add(CreateMaterializedViewStmt
                            .mvColumnBreaker(MaterializedIndexMeta.normalizeName(column.getName()))));
            LOG.debug("candidateIndexSchema {}, indexColumnNames {}, queryColumnNames {}",
                            candidateIndexSchema, indexColumnNames, queryColumnNames);
            // Rollup index have no define expr.
            if (entry.getValue().getWhereClause() == null && indexExprs.isEmpty()
                    && !indexColumnNames.containsAll(queryColumnNames)) {
                iterator.remove();
                continue;
            }

            if (selectBaseIndex) {
                // Base index or rollup index have no define expr.
                if (!indexExprs.isEmpty()) {
                    iterator.remove();
                }
                continue;
            }

            if (!matchAllExpr(exprs, indexExprs, tid)) {
                iterator.remove();
            }
        }
        LOG.debug("Those mv pass the test of output columns:"
                + Joiner.on(",").join(candidateIndexIdToMeta.keySet()));
    }

    private void compensateCandidateIndex(Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta,
            Map<Long, MaterializedIndexMeta> allVisibleIndexes, OlapTable table) {
        isPreAggregation = false;
        reasonOfDisable = "The aggregate operator does not match";
        int keySizeOfBaseIndex = table.getKeyColumnsByIndexId(table.getBaseIndexId()).size();
        for (Map.Entry<Long, MaterializedIndexMeta> index : allVisibleIndexes.entrySet()) {
            long mvIndexId = index.getKey();
            if (table.getKeyColumnsByIndexId(mvIndexId).size() == keySizeOfBaseIndex) {
                candidateIndexIdToMeta.put(mvIndexId, index.getValue());
            }
        }
        LOG.debug("Those mv pass the test of output columns:"
                + Joiner.on(",").join(candidateIndexIdToMeta.keySet()));
    }

    private void init() {
        // Step1: compute the columns in compensating predicates
        Expr whereClause = selectStmt.getWhereClause();
        if (whereClause != null) {
            whereClause.getTableIdToColumnNames(columnNamesInPredicates);
        }
        for (TableRef tableRef : selectStmt.getTableRefs()) {
            if (tableRef.getOnClause() == null) {
                continue;
            }
            tableRef.getOnClause().getTableIdToColumnNames(columnNamesInPredicates);
        }

        if (selectStmt.getAggInfo() == null) {
            isSPJQuery = true;
        } else {
            // Step2: compute the columns in group by expr
            if (selectStmt.getAggInfo().getGroupingExprs() != null) {
                List<Expr> groupingExprs = selectStmt.getAggInfo().getGroupingExprs();
                for (Expr expr : groupingExprs) {
                    expr.getTableIdToColumnNames(columnNamesInGrouping);
                }
            }
            // Step3: compute the aggregation function
            for (FunctionCallExpr aggExpr : selectStmt.getAggInfo().getAggregateExprs()) {
                Map<Long, Set<String>> tableIdToAggColumnNames = Maps.newHashMap();
                aggExpr.getTableIdToColumnNames(tableIdToAggColumnNames);
                // count(*): tableIdToAggColumnNames is empty which must forbidden the SPJG MV.
                // TODO(ml): support count(*)
                if (tableIdToAggColumnNames.size() != 1) {
                    reasonOfDisable = "aggExpr[" + aggExpr.debugString() + "] should involved only one column";
                    disableSPJGView = true;
                    break;
                }
                addAggColumnInQuery(tableIdToAggColumnNames.keySet().stream().findFirst().get(), aggExpr);
                // TODO(ml): select rollup by case expr
            }
        }

        // Step4: compute the output column
        // ISSUE-3174: all columns which belong to top tuple should be considered in
        // selector.
        List<TupleId> tupleIds = selectStmt.getTableRefIdsWithoutInlineView();
        for (TupleId tupleId : tupleIds) {
            TupleDescriptor tupleDescriptor = analyzer.getTupleDesc(tupleId);
            tupleDescriptor.getTableIdToColumnNames(columnNamesInQueryOutput);
        }
    }

    private void addAggColumnInQuery(Long tableId, FunctionCallExpr fnExpr) {
        aggColumnsInQuery.computeIfAbsent(tableId, k -> Sets.newHashSet()).add(fnExpr);
    }

    private boolean aggFunctionsMatchAggColumns(Set<FunctionCallExpr> queryExprList,
            List<FunctionCallExpr> mvColumnExprList) throws AnalysisException {
        for (Expr queryExpr : queryExprList) {
            boolean match = false;
            for (Expr mvColumnExpr : mvColumnExprList) {
                if (MVExprEquivalent.mvExprEqual(queryExpr, mvColumnExpr)) {
                    match = true;
                    break;
                }
            }

            if (!match) {
                return false;
            }
        }
        return true;
    }

    private List<FunctionCallExpr> mvAggColumnsToExprList(MaterializedIndexMeta mvMeta) {
        List<FunctionCallExpr> result = Lists.newArrayList();
        List<Column> schema = mvMeta.getSchema();
        for (Column column : schema) {
            if (!column.isAggregated()) {
                continue;
            }
            SlotRef slotRef = new SlotRef(null, column.getName());
            // This slot desc is only used to temporarily store column that will be used in
            // subsequent MVExprRewriter.
            SlotDescriptor slotDescriptor = new SlotDescriptor(null, null);
            slotDescriptor.setColumn(column);
            slotRef.setDesc(slotDescriptor);
            FunctionCallExpr fnExpr = new FunctionCallExpr(column.getAggregationType().name(),
                    Lists.newArrayList(slotRef));
            result.add(fnExpr);
        }
        return result;
    }

    public class BestIndexInfo {
        private long bestIndexId;
        private boolean isPreAggregation;
        private String reasonOfDisable;

        public BestIndexInfo(long bestIndexId, boolean isPreAggregation, String reasonOfDisable) {
            this.bestIndexId = bestIndexId;
            this.isPreAggregation = isPreAggregation;
            this.reasonOfDisable = reasonOfDisable;
        }

        public long getBestIndexId() {
            return bestIndexId;
        }

        public boolean isPreAggregation() {
            return isPreAggregation;
        }

        public String getReasonOfDisable() {
            return reasonOfDisable;
        }
    }
}
