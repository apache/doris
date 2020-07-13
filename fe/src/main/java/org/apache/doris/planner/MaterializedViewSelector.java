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
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * The new materialized view selector supports SPJ<->SPJG.
 * It means this selector can judge any combination of those two options SPJ and SPJG.
 * For example, the type of query is SPJG while the type of MV is SPJ.
 * At the same time, it is compatible with all the features of the old version.
 *
 * What is SPJ and SPJG?
 * The SPJ query is "Select Projection and Join" such as:
 *     select t1.c1 from t1, t2 where t1.c2=t2.c2 and t1.c3=1;
 * The SPJG query is "Select Projection Join and Group-by" such as:
 *     select t1.c1, sum(t2.c1) from t1, t2 where t1.c2=t2.c2 and t1.c3=1 group by t1.c 1;
 */
public class MaterializedViewSelector {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewSelector.class);

    private final SelectStmt selectStmt;
    private final Analyzer analyzer;

    /**
     * The key of following maps is table name.
     * The value of following maps is column names.
     * `columnNamesInPredicates` means that the column names in where clause.
     * And so on.
     */
    private Map<String, Set<String>> columnNamesInPredicates = Maps.newHashMap();
    private boolean isSPJQuery;
    private Map<String, Set<String>> columnNamesInGrouping = Maps.newHashMap();
    private Map<String, Set<AggregatedColumn>> aggregateColumnsInQuery = Maps.newHashMap();
    private Map<String, Set<String>> columnNamesInQueryOutput = Maps.newHashMap();

    private boolean disableSPJGView;
    private String reasonOfDisable;
    private boolean isPreAggregation = true;

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
        long start = System.currentTimeMillis();
        Preconditions.checkState(scanNode instanceof OlapScanNode);
        OlapScanNode olapScanNode = (OlapScanNode) scanNode;

        Map<Long, List<Column>> candidateIndexIdToSchema = predicates(olapScanNode);
        long bestIndexId = priorities(olapScanNode, candidateIndexIdToSchema);
        LOG.debug("The best materialized view is {} for scan node {} in query {}, cost {}",
                 bestIndexId, scanNode.getId(), selectStmt.toSql(), (System.currentTimeMillis() - start));
        return new BestIndexInfo(bestIndexId, isPreAggregation, reasonOfDisable);
    }

    private Map<Long, List<Column>> predicates(OlapScanNode scanNode) {
        // Step1: all of predicates is compensating predicates
        Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta = scanNode.getOlapTable().getVisibleIndexIdToMeta();
        OlapTable table = scanNode.getOlapTable();
        Preconditions.checkState(table != null);
        String tableName = table.getName();
        // Step2: check all columns in compensating predicates are available in the view output
        checkCompensatingPredicates(columnNamesInPredicates.get(tableName), candidateIndexIdToMeta);
        // Step3: group by list in query is the subset of group by list in view or view contains no aggregation
        checkGrouping(columnNamesInGrouping.get(tableName), candidateIndexIdToMeta);
        // Step4: aggregation functions are available in the view output
        checkAggregationFunction(aggregateColumnsInQuery.get(tableName), candidateIndexIdToMeta);
        // Step5: columns required to compute output expr are available in the view output
        checkOutputColumns(columnNamesInQueryOutput.get(tableName), candidateIndexIdToMeta);
        // Step6: if table type is aggregate and the candidateIndexIdToSchema is empty,
        if ((table.getKeysType() == KeysType.AGG_KEYS || table.getKeysType() == KeysType.UNIQUE_KEYS)
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
             * The reason is that the level of group by in tableA is upper then the level of group by in query.
             * So, we need to compensate those kinds of index in following step.
             *
             */
            compensateCandidateIndex(candidateIndexIdToMeta, scanNode.getOlapTable().getVisibleIndexIdToMeta(),
                            table);
            checkOutputColumns(columnNamesInQueryOutput.get(tableName), candidateIndexIdToMeta);
        }
        Map<Long, List<Column>> result = Maps.newHashMap();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : candidateIndexIdToMeta.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getSchema());
        }
        return result;
    }

    private long priorities(OlapScanNode scanNode, Map<Long, List<Column>> candidateIndexIdToSchema) {
        OlapTable tbl = scanNode.getOlapTable();
        Long v2RollupIndexId = tbl.getSegmentV2FormatIndexId();
        if (v2RollupIndexId != null) {
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null && connectContext.getSessionVariable().isUseV2Rollup()) {
                // if user set `use_v2_rollup` variable to true, and there is a segment v2 rollup,
                // just return the segment v2 rollup, because user want to check the v2 format data.
                if (candidateIndexIdToSchema.containsKey(v2RollupIndexId)) {
                    return v2RollupIndexId;
                }
            } else {
                // `use_v2_rollup` is not set, so v2 format rollup should not be selected, remove it from
                // candidateIndexIdToSchema
                candidateIndexIdToSchema.remove(v2RollupIndexId);
            }
        }

        // Step1: the candidate indexes that satisfies the most prefix index
        final Set<String> equivalenceColumns = Sets.newHashSet();
        final Set<String> unequivalenceColumns = Sets.newHashSet();
        scanNode.collectColumns(analyzer, equivalenceColumns, unequivalenceColumns);
        Set<Long> indexesMatchingBestPrefixIndex =
                matchBestPrefixIndex(candidateIndexIdToSchema, equivalenceColumns, unequivalenceColumns);

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

    private void checkCompensatingPredicates(Set<String> columnsInPredicates, Map<Long, MaterializedIndexMeta>
            candidateIndexIdToMeta) {
        // When the query statement does not contain any columns in predicates, all candidate index can pass this check
        if (columnsInPredicates == null) {
            return;
        }
        Iterator<Map.Entry<Long, MaterializedIndexMeta>> iterator = candidateIndexIdToMeta.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MaterializedIndexMeta> entry = iterator.next();
            Set<String> indexNonAggregatedColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            entry.getValue().getSchema().stream().filter(column -> !column.isAggregated())
                    .forEach(column -> indexNonAggregatedColumnNames.add(column.getName()));
            if (!indexNonAggregatedColumnNames.containsAll(columnsInPredicates)) {
                iterator.remove();
            }
        }
        LOG.debug("Those mv pass the test of compensating predicates:"
                          + Joiner.on(",").join(candidateIndexIdToMeta.keySet()));
    }

    /**
     * View      Query        result
     * SPJ       SPJG OR SPJ  pass
     * SPJG      SPJ          fail
     * SPJG      SPJG         pass
     * 1. grouping columns in query is subset of grouping columns in view
     * 2. the empty grouping columns in query is subset of all of views
     *
     * @param columnsInGrouping
     * @param candidateIndexIdToMeta
     */

    private void checkGrouping(Set<String> columnsInGrouping, Map<Long, MaterializedIndexMeta>
            candidateIndexIdToMeta) {
        Iterator<Map.Entry<Long, MaterializedIndexMeta>> iterator = candidateIndexIdToMeta.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MaterializedIndexMeta> entry = iterator.next();
            Set<String> indexNonAggregatedColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            MaterializedIndexMeta candidateIndexMeta = entry.getValue();
            List<Column> candidateIndexSchema = candidateIndexMeta.getSchema();
            candidateIndexSchema.stream().filter(column -> !column.isAggregated())
                    .forEach(column -> indexNonAggregatedColumnNames.add(column.getName()));
            /*
            If there is no aggregated column in duplicate index, the index will be SPJ.
            For example:
                duplicate table (k1, k2, v1)
                duplicate mv index (k1, v1)
            When the candidate index is SPJ type, it passes the verification directly

            If there is no aggregated column in aggregate index, the index will be deduplicate index.
            For example:
                duplicate table (k1, k2, v1 sum)
                aggregate mv index (k1, k2)
            This kind of index is SPJG which same as select k1, k2 from aggregate_table group by k1, k2.
            It also need to check the grouping column using following steps.

            ISSUE-3016, MaterializedViewFunctionTest: testDeduplicateQueryInAgg
             */
            if (indexNonAggregatedColumnNames.size() == candidateIndexSchema.size()
                    && candidateIndexMeta.getKeysType() == KeysType.DUP_KEYS) {
                continue;
            }
            // When the query is SPJ type but the candidate index is SPJG type, it will not pass directly.
            if (isSPJQuery || disableSPJGView) {
                iterator.remove();
                continue;
            }
            // The query is SPJG. The candidate index is SPJG too.
            // The grouping columns in query is empty. For example: select sum(A) from T
            if (columnsInGrouping == null) {
                continue;
            }
            // The grouping columns in query must be subset of the grouping columns in view
            if (!indexNonAggregatedColumnNames.containsAll(columnsInGrouping)) {
                iterator.remove();
            }
        }
        LOG.debug("Those mv pass the test of grouping:"
                          + Joiner.on(",").join(candidateIndexIdToMeta.keySet()));
    }

    private void checkAggregationFunction(Set<AggregatedColumn> aggregatedColumnsInQueryOutput,
            Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta) {
        Iterator<Map.Entry<Long, MaterializedIndexMeta>> iterator = candidateIndexIdToMeta.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MaterializedIndexMeta> entry = iterator.next();
            List<AggregatedColumn> indexAggregatedColumns = Lists.newArrayList();
            List<Column> candidateIndexSchema = entry.getValue().getSchema();
            candidateIndexSchema.stream().filter(column -> column.isAggregated())
                    .forEach(column -> indexAggregatedColumns.add(
                            new AggregatedColumn(column.getName(), column.getAggregationType().name())));
            // When the candidate index is SPJ type, it passes the verification directly
            if (indexAggregatedColumns.size() == 0) {
                continue;
            }
            // When the query is SPJ type but the candidate index is SPJG type, it will not pass directly.
            if (isSPJQuery || disableSPJGView) {
                iterator.remove();
                continue;
            }
            // The query is SPJG. The candidate index is SPJG too.
            /* Situation1: The query is deduplicate SPJG when aggregatedColumnsInQueryOutput is null.
             * For example: select a , b from table group by a, b
             * The aggregation function check should be pass directly when MV is SPJG.
             */
            if (aggregatedColumnsInQueryOutput == null) {
                continue;
            }
            // The aggregated columns in query output must be subset of the aggregated columns in view
            if (!indexAggregatedColumns.containsAll(aggregatedColumnsInQueryOutput)) {
                iterator.remove();
            }
        }
        LOG.debug("Those mv pass the test of aggregation function:"
                          + Joiner.on(",").join(candidateIndexIdToMeta.keySet()));
    }

    private void checkOutputColumns(Set<String> columnNamesInQueryOutput, Map<Long, MaterializedIndexMeta>
            candidateIndexIdToMeta) {
        if (columnNamesInQueryOutput == null) {
            return;
        }
        Iterator<Map.Entry<Long, MaterializedIndexMeta>> iterator = candidateIndexIdToMeta.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MaterializedIndexMeta> entry = iterator.next();
            Set<String> indexColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            List<Column> candidateIndexSchema = entry.getValue().getSchema();
            candidateIndexSchema.stream().forEach(column -> indexColumnNames.add(column.getName()));
            // The aggregated columns in query output must be subset of the aggregated columns in view
            if (!indexColumnNames.containsAll(columnNamesInQueryOutput)) {
                iterator.remove();
            }
        }
        LOG.debug("Those mv pass the test of output columns:"
                          + Joiner.on(",").join(candidateIndexIdToMeta.keySet()));
    }

    private void compensateCandidateIndex(Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta, Map<Long,
            MaterializedIndexMeta> allVisibleIndexes, OlapTable table) {
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
            whereClause.getTableNameToColumnNames(columnNamesInPredicates);
        }
        for (TableRef tableRef : selectStmt.getTableRefs()) {
            if (tableRef.getOnClause() == null) {
                continue;
            }
            tableRef.getOnClause().getTableNameToColumnNames(columnNamesInPredicates);
        }

        if (selectStmt.getAggInfo() == null) {
            isSPJQuery = true;
        } else {
            // Step2: compute the columns in group by expr
            if (selectStmt.getAggInfo().getGroupingExprs() != null) {
                List<Expr> groupingExprs = selectStmt.getAggInfo().getGroupingExprs();
                for (Expr expr : groupingExprs) {
                    expr.getTableNameToColumnNames(columnNamesInGrouping);
                }
            }
            // Step3: compute the aggregation function
            for (FunctionCallExpr aggExpr : selectStmt.getAggInfo().getAggregateExprs()) {
                // Only sum, min, max function could appear in materialized views.
                // The number of children in these functions is one.
                if (aggExpr.getChildren().size() != 1) {
                    reasonOfDisable = "aggExpr has more than one child";
                    disableSPJGView = true;
                    break;
                }
                Expr aggChild0 = aggExpr.getChild(0);
                if (aggChild0 instanceof SlotRef) {
                    SlotRef slotRef = (SlotRef) aggChild0;
                    Table table = slotRef.getDesc().getParent().getTable();
                    /* If this column come from subquery, the parent table will be null
                     * For example: select k1 from (select name as k1 from tableA) A
                     * The parent table of k1 column in outer query is null.
                     */
                    if (table == null) {
                        continue;
                    }
                    Preconditions.checkState(slotRef.getColumnName() != null);
                    addAggregatedColumn(slotRef.getColumnName(), aggExpr.getFnName().getFunction(),
                                        table.getName());
                } else if ((aggChild0 instanceof CastExpr) && (aggChild0.getChild(0) instanceof SlotRef)) {
                    SlotRef slotRef = (SlotRef) aggChild0.getChild(0);
                    Table table = slotRef.getDesc().getParent().getTable();
                    /*
                     * Same as above
                     */
                    if (table == null) {
                        continue;
                    }
                    Preconditions.checkState(slotRef.getColumnName() != null);
                    addAggregatedColumn(slotRef.getColumnName(), aggExpr.getFnName().getFunction(),
                                        table.getName());
                } else {
                    reasonOfDisable = "aggExpr.getChild(0)[" + aggExpr.getChild(0).debugString()
                            + "] is not SlotRef or CastExpr|CaseExpr";
                    disableSPJGView = true;
                    break;
                }
                // TODO(ml): select rollup by case expr
            }
        }

        // Step4: compute the output column
        // ISSUE-3174: all of columns which belong to top tuple should be considered in selector.
        List<TupleId> topTupleIds = selectStmt.getTableRefIdsWithoutInlineView();
        for (TupleId tupleId : topTupleIds) {
            TupleDescriptor tupleDescriptor = analyzer.getTupleDesc(tupleId);
            tupleDescriptor.getTableNameToColumnNames(columnNamesInQueryOutput);

        }
    }

    private void addAggregatedColumn(String columnName, String functionName, String tableName) {
        AggregatedColumn newAggregatedColumn = new AggregatedColumn(columnName, functionName);
        Set<AggregatedColumn> aggregatedColumns = aggregateColumnsInQuery.computeIfAbsent(tableName, k -> Sets.newHashSet());
        aggregatedColumns.add(newAggregatedColumn);
    }

    class AggregatedColumn {
        private String columnName;
        private String aggFunctionName;

        public AggregatedColumn(String columnName, String aggFunctionName) {
            this.columnName = columnName;
            this.aggFunctionName = aggFunctionName;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof AggregatedColumn)) {
                return false;
            }
            AggregatedColumn input = (AggregatedColumn) obj;
            return this.columnName.equalsIgnoreCase(input.columnName)
                    && this.aggFunctionName.equalsIgnoreCase(input.aggFunctionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.columnName, this.aggFunctionName);
        }
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


