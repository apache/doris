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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/Planner.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectListItem;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.CommonResultSet;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.ResultSetMetaData;
import org.apache.doris.rewrite.mvrewrite.MVSelectFailedException;
import org.apache.doris.statistics.query.StatsDelta;
import org.apache.doris.thrift.TFetchOption;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TRuntimeFilterMode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The planner is responsible for turning parse trees into plan fragments that can be shipped off to backends for
 * execution.
 */
public class OriginalPlanner extends Planner {
    private static final Logger LOG = LogManager.getLogger(OriginalPlanner.class);

    private PlannerContext plannerContext;
    private SingleNodePlanner singleNodePlanner;
    private DistributedPlanner distributedPlanner;
    private Analyzer analyzer;

    public OriginalPlanner(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public boolean isBlockQuery() {
        return isBlockQuery;
    }

    public PlannerContext getPlannerContext() {
        return plannerContext;
    }

    public List<ScanNode> getScanNodes() {
        if (singleNodePlanner == null) {
            return Lists.newArrayList();
        }
        return singleNodePlanner.getScanNodes();
    }

    public void plan(StatementBase queryStmt, TQueryOptions queryOptions)
            throws UserException {
        createPlanFragments(queryStmt, analyzer, queryOptions);
    }

    @Override
    public List<RuntimeFilter> getRuntimeFilters() {
        return analyzer.getAssignedRuntimeFilter();
    }

    private void setResultExprScale(Analyzer analyzer, ArrayList<Expr> outputExprs) {
        for (TupleDescriptor tupleDesc : analyzer.getDescTbl().getTupleDescs()) {
            for (SlotDescriptor slotDesc : tupleDesc.getSlots()) {
                for (Expr expr : outputExprs) {
                    List<SlotId> slotList = Lists.newArrayList();
                    expr.getIds(null, slotList);
                    if ((!expr.getType().getPrimitiveType().isDecimalV2Type()
                            && expr.getType().getPrimitiveType().isDecimalV3Type())) {
                        continue;
                    }

                    if (!slotDesc.getType().getPrimitiveType().isDecimalV2Type()
                            && !slotDesc.getType().getPrimitiveType().isDecimalV3Type()) {
                        continue;
                    }

                    if (slotList.contains(slotDesc.getId()) && null != slotDesc.getColumn()) {
                        int outputScale = slotDesc.getColumn().getScale();
                        if (outputScale >= 0) {
                            if (outputScale > expr.getOutputScale()) {
                                expr.setOutputScale(outputScale);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Return combined explain string for all plan fragments.
     */
    @Override
    public void appendTupleInfo(StringBuilder str) {
        str.append(plannerContext.getRootAnalyzer().getDescTbl().getExplainString());
    }

    /**
     * Create plan fragments for an analyzed statement, given a set of execution options. The fragments are returned in
     * a list such that element i of that list can only consume output of the following fragments j > i.
     */
    public void createPlanFragments(StatementBase statement, Analyzer analyzer, TQueryOptions queryOptions)
            throws UserException {
        QueryStmt queryStmt;
        if (statement instanceof InsertStmt) {
            queryStmt = ((InsertStmt) statement).getQueryStmt();
        } else {
            queryStmt = (QueryStmt) statement;
        }
        plannerContext = new PlannerContext(analyzer, queryStmt, queryOptions, statement);
        singleNodePlanner = new SingleNodePlanner(plannerContext);
        PlanNode singleNodePlan = singleNodePlanner.createSingleNodePlan();
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setCreateSingleNodeFinishTime();
        }
        ProjectPlanner projectPlanner = new ProjectPlanner(analyzer);
        projectPlanner.projectSingleNodePlan(queryStmt.getResultExprs(), singleNodePlan);
        if (statement instanceof InsertStmt) {
            InsertStmt insertStmt = (InsertStmt) statement;
            insertStmt.prepareExpressions();
        }

        // TODO chenhao16 , no used materialization work
        // compute referenced slots before calling computeMemLayout()
        //analyzer.markRefdSlots(analyzer, singleNodePlan, resultExprs, null);

        setResultExprScale(analyzer, queryStmt.getResultExprs());

        // materialized view selector
        boolean selectFailed = singleNodePlanner.selectMaterializedView(queryStmt, analyzer);
        if (selectFailed) {
            throw new MVSelectFailedException("Failed to select materialize view");
        }

        /**
         * - Under normal circumstances, computeMemLayout() will be executed
         *     at the end of the init function of the plan node.
         * Such as :
         * OlapScanNode {
         *     init () {
         *         analyzer.materializeSlots(conjuncts);
         *         computeTupleStatAndMemLayout(analyzer);
         *         computeStat();
         *     }
         * }
         * - However Doris is currently unable to determine
         *     whether it is possible to cut or increase the columns in the tuple after PlanNode.init().
         * - Therefore, for the time being, computeMemLayout() can only be placed
         *     after the completion of the entire single node planner.
         */
        analyzer.getDescTbl().computeMemLayout();
        singleNodePlan.finalize(analyzer);
        if (Config.enable_query_hit_stats && plannerContext.getStatement() != null
                && plannerContext.getStatement().getExplainOptions() == null) {
            collectQueryStat(singleNodePlan);
        }
        checkAndSetTopnOpt(singleNodePlan);

        if (queryOptions.num_nodes == 1 || queryStmt.isPointQuery()) {
            // single-node execution; we're almost done
            singleNodePlan = addUnassignedConjuncts(analyzer, singleNodePlan);
            fragments.add(new PlanFragment(plannerContext.getNextFragmentId(), singleNodePlan,
                    DataPartition.UNPARTITIONED));
        } else {
            // all select query are unpartitioned.
            distributedPlanner = new DistributedPlanner(plannerContext);
            fragments = distributedPlanner.createPlanFragments(singleNodePlan);
        }
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setQueryDistributedFinishTime();
        }

        // Push sort node down to the bottom of olapscan.
        // Because the olapscan must be in the end. So get the last two nodes.
        pushSortToOlapScan();

        // Optimize the transfer of query statistic when query doesn't contain limit.
        PlanFragment rootFragment = fragments.get(fragments.size() - 1);
        QueryStatisticsTransferOptimizer queryStatisticTransferOptimizer
                = new QueryStatisticsTransferOptimizer(rootFragment);
        queryStatisticTransferOptimizer.optimizeQueryStatisticsTransfer();

        // Create runtime filters.
        if (!ConnectContext.get().getSessionVariable().getRuntimeFilterMode().toUpperCase()
                .equals(TRuntimeFilterMode.OFF.name())) {
            RuntimeFilterGenerator.generateRuntimeFilters(analyzer, rootFragment.getPlanRoot());
        }

        if (statement instanceof InsertStmt && !analyzer.getContext().isTxnModel()) {
            InsertStmt insertStmt = (InsertStmt) statement;
            rootFragment = distributedPlanner.createInsertFragment(rootFragment, insertStmt, fragments);
            rootFragment.setSink(insertStmt.getDataSink());
            insertStmt.complete();
            List<Expr> exprs = statement.getResultExprs();
            List<Expr> resExprs = Expr.substituteList(
                    exprs, rootFragment.getPlanRoot().getOutputSmap(), analyzer, true);
            rootFragment.setOutputExprs(resExprs);
        } else {
            List<Expr> resExprs = Expr.substituteList(queryStmt.getResultExprs(),
                    rootFragment.getPlanRoot().getOutputSmap(), analyzer, false);
            LOG.debug("result Exprs {}", queryStmt.getResultExprs());
            LOG.debug("substitute result Exprs {}", resExprs);
            rootFragment.setOutputExprs(resExprs);
        }
        LOG.debug("finalize plan fragments");
        for (PlanFragment fragment : fragments) {
            fragment.finalize(queryStmt);
        }

        Collections.reverse(fragments);

        pushDownResultFileSink(analyzer);

        pushOutColumnUniqueIdsToOlapScan(rootFragment, analyzer);

        if (queryStmt instanceof SelectStmt) {
            SelectStmt selectStmt = (SelectStmt) queryStmt;
            if (queryStmt.getSortInfo() != null || selectStmt.getAggInfo() != null) {
                isBlockQuery = true;
                LOG.debug("this is block query");
            } else {
                isBlockQuery = false;
                LOG.debug("this isn't block query");
            }
            // Check SelectStatement if optimization condition satisfied
            if (selectStmt.isPointQueryShortCircuit()) {
                // Optimize for point query like: SELECT * FROM t1 WHERE pk1 = 1 and pk2 = 2
                // such query will use direct RPC to do point query
                LOG.debug("it's a point query");
                Map<SlotRef, Expr> eqConjuncts = ((SelectStmt) selectStmt).getPointQueryEQPredicates();
                OlapScanNode olapScanNode = (OlapScanNode) singleNodePlan;
                olapScanNode.setDescTable(analyzer.getDescTbl());
                olapScanNode.setPointQueryEqualPredicates(eqConjuncts);
                if (analyzer.getPrepareStmt() != null) {
                    // Cache them for later request better performance
                    analyzer.getPrepareStmt().cacheSerializedDescriptorTable(olapScanNode.getDescTable());
                    analyzer.getPrepareStmt().cacheSerializedOutputExprs(rootFragment.getOutputExprs());
                }
            } else if (selectStmt.isTwoPhaseReadOptEnabled()) {
                // Optimize query like `SELECT ... FROM <tbl> WHERE ... ORDER BY ... LIMIT ...`
                if (singleNodePlan instanceof SortNode
                        && singleNodePlan.getChildren().size() == 1
                        && ((SortNode) singleNodePlan).getChild(0) instanceof OlapScanNode) {
                    // Double check this plan to ensure it's a general topn query
                    injectRowIdColumnSlot();
                    ((SortNode) singleNodePlan).setUseTwoPhaseReadOpt(true);
                } else if (singleNodePlan instanceof OlapScanNode &&  singleNodePlan.getChildren().size() == 0) {
                    // Optimize query like `SELECT ... FROM <tbl> WHERE ... LIMIT ...`.
                    // This typically used when row store enabled, to reduce scan cost
                    injectRowIdColumnSlot();
                } else {
                    // This is not a general topn query, rollback needMaterialize flag
                    for (SlotDescriptor slot : analyzer.getDescTbl().getSlotDescs().values()) {
                        slot.setNeedMaterialize(true);
                    }
                }
            }
        }
    }

    /**
     * If there are unassigned conjuncts, returns a SelectNode on top of root that evaluate those conjuncts; otherwise
     * returns root unchanged.
     */
    private PlanNode addUnassignedConjuncts(Analyzer analyzer, PlanNode root)
            throws UserException {
        Preconditions.checkNotNull(root);
        // List<Expr> conjuncts = analyzer.getUnassignedConjuncts(root.getTupleIds());

        List<Expr> conjuncts = analyzer.getUnassignedConjuncts(root);
        if (conjuncts.isEmpty()) {
            return root;
        }
        // evaluate conjuncts in SelectNode
        SelectNode selectNode = new SelectNode(plannerContext.getNextNodeId(), root, conjuncts);
        selectNode.init(analyzer);
        Preconditions.checkState(selectNode.hasValidStats());
        return selectNode;
    }

    /**
     * This function is mainly used to try to push the top-level result file sink down one layer.
     * The result file sink after the pushdown can realize the function of concurrently exporting the result set.
     * Push down needs to meet the following conditions:
     * 1. The query enables the session variable of the concurrent export result set
     * 2. The top-level fragment is not a merge change node
     * 3. The export method uses the s3 method
     *
     * After satisfying the above three conditions,
     * the result file sink and the associated output expr will be pushed down to the next layer.
     * The second plan fragment performs expression calculation and derives the result set.
     * The top plan fragment will only summarize the status of the exported result set and return it to fe.
     */
    private void pushDownResultFileSink(Analyzer analyzer) {
        if (fragments.size() < 1) {
            return;
        }
        if (!(fragments.get(0).getSink() instanceof ResultFileSink)) {
            return;
        }
        if (!ConnectContext.get().getSessionVariable().isEnableParallelOutfile()) {
            return;
        }
        if (!(fragments.get(0).getPlanRoot() instanceof ExchangeNode)) {
            return;
        }
        PlanFragment topPlanFragment = fragments.get(0);
        ExchangeNode topPlanNode = (ExchangeNode) topPlanFragment.getPlanRoot();
        // try to push down result file sink
        if (topPlanNode.isMergingExchange()) {
            return;
        }
        PlanFragment secondPlanFragment = fragments.get(1);
        ResultFileSink resultFileSink = (ResultFileSink) topPlanFragment.getSink();
        if (resultFileSink.getStorageType() == StorageBackend.StorageType.BROKER) {
            return;
        }
        if (secondPlanFragment.getOutputExprs() != null) {
            return;
        }
        // create result file sink desc
        TupleDescriptor fileStatusDesc = constructFileStatusTupleDesc(analyzer);
        resultFileSink.resetByDataStreamSink((DataStreamSink) secondPlanFragment.getSink());
        resultFileSink.setOutputTupleId(fileStatusDesc.getId());
        secondPlanFragment.setOutputExprs(topPlanFragment.getOutputExprs());
        secondPlanFragment.resetSink(resultFileSink);
        ResultSink resultSink = new ResultSink(topPlanNode.getId());
        topPlanFragment.resetSink(resultSink);
        topPlanFragment.resetOutputExprs(fileStatusDesc);
        topPlanFragment.getPlanRoot().resetTupleIds(Lists.newArrayList(fileStatusDesc.getId()));
    }

    private SlotDescriptor injectRowIdColumnSlot(Analyzer analyzer, TupleDescriptor tupleDesc) {
        SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(tupleDesc);
        LOG.debug("inject slot {}", slotDesc);
        String name = Column.ROWID_COL;
        Column col = new Column(name, Type.STRING, false, null, false, "",
                                        "rowid column");
        slotDesc.setType(Type.STRING);
        slotDesc.setColumn(col);
        slotDesc.setIsNullable(false);
        slotDesc.setIsMaterialized(true);
        return slotDesc;
    }

    // We use two phase read to optimize sql like: select * from tbl [where xxx = ???] [order by column1] [limit n]
    // in the first phase, we add an extra column `RowId` to Block, and sort blocks in TopN nodes
    // in the second phase, we have n rows, we do a fetch rpc to get all rowids date for the n rows
    // and reconconstruct the final block
    private void injectRowIdColumnSlot() {
        boolean injected = false;
        OlapTable olapTable = null;
        OlapScanNode scanNode = null;
        for (PlanFragment fragment : fragments) {
            PlanNode node = fragment.getPlanRoot();
            PlanNode parent = null;
            // OlapScanNode is the last node.
            // So, just get the last two node and check if they are SortNode and OlapScan.
            while (node.getChildren().size() != 0) {
                parent = node;
                node = node.getChildren().get(0);
            }

            // case1
            if ((node instanceof OlapScanNode) && (parent instanceof SortNode)) {
                SortNode sortNode = (SortNode) parent;
                scanNode = (OlapScanNode) node;
                SlotDescriptor slot = injectRowIdColumnSlot(analyzer, scanNode.getTupleDesc());
                injectRowIdColumnSlot(analyzer, sortNode.getSortInfo().getSortTupleDescriptor());
                SlotRef extSlot = new SlotRef(slot);
                sortNode.getResolvedTupleExprs().add(extSlot);
                sortNode.getSortInfo().setUseTwoPhaseRead();
                injected = true;
                olapTable = scanNode.getOlapTable();
                break;
            }
            // case2
            if ((node instanceof OlapScanNode) && parent == null) {
                scanNode = (OlapScanNode) node;
                injectRowIdColumnSlot(analyzer, scanNode.getTupleDesc());
                injected = true;
                olapTable = scanNode.getOlapTable();
                break;
            }
        }
        for (PlanFragment fragment : fragments) {
            if (injected && fragment.getSink() instanceof ResultSink) {
                TFetchOption fetchOption = olapTable.generateTwoPhaseReadOption(scanNode.getSelectedIndexId());
                ((ResultSink) fragment.getSink()).setFetchOption(fetchOption);
                break;
            }
        }
    }

    /**
     * Push sort down to olap scan.
     */
    private void pushSortToOlapScan() {
        for (PlanFragment fragment : fragments) {
            PlanNode node = fragment.getPlanRoot();
            PlanNode parent = null;

            // OlapScanNode is the last node.
            // So, just get the last two node and check if they are SortNode and OlapScan.
            while (node.getChildren().size() != 0) {
                parent = node;
                node = node.getChildren().get(0);
            }

            if (!(node instanceof OlapScanNode) || !(parent instanceof SortNode)) {
                continue;
            }
            SortNode sortNode = (SortNode) parent;
            OlapScanNode scanNode = (OlapScanNode) node;

            if (!scanNode.checkPushSort(sortNode)) {
                continue;
            }
            // If offset > 0, we push down (limit: limit + offset) to olap scan.
            if (sortNode.getOffset() > 0) {
                scanNode.setSortLimit(sortNode.getLimit() + sortNode.getOffset());
            } else {
                scanNode.setSortLimit(sortNode.getLimit());
            }
            scanNode.setSortInfo(sortNode.getSortInfo());
            scanNode.getSortInfo().setSortTupleSlotExprs(sortNode.resolvedTupleExprs);
        }
    }

    /**
     * outputColumnUniqueIds contain columns in OrderByExprs and outputExprs,
     * push output column unique id set to olap scan.
     *
     * when query to storage layer, there are need read raw data
     * for columns which in outputColumnUniqueIds
     *
     * for example:
     * select A from tb where B = 1 and C > 'hello' order by B;
     *
     * column unique id for `A` and `B` will put into outputColumnUniqueIds.
     *
    */
    // this opt will only work with nereidsPlanner
    private void pushOutColumnUniqueIdsToOlapScan(PlanFragment rootFragment, Analyzer analyzer) {
        Set<Integer> outputColumnUniqueIds = new HashSet<>();
        if (ConnectContext.get().getSessionVariable().enableIndexDataReadOptOnOrigPlanner) {
            ArrayList<Expr> outputExprs = rootFragment.getOutputExprs();
            for (Expr expr : outputExprs) {
                if (expr instanceof SlotRef) {
                    if (((SlotRef) expr).getColumn() != null) {
                        outputColumnUniqueIds.add(((SlotRef) expr).getColumn().getUniqueId());
                    }
                }
            }
            for (PlanFragment fragment : fragments) {
                PlanNode node = fragment.getPlanRoot();
                PlanNode parent = null;
                while (node.getChildren().size() != 0) {
                    for (PlanNode childNode : node.getChildren()) {
                        List<SlotId> outputSlotIds = childNode.getOutputSlotIds();
                        if (outputSlotIds != null) {
                            for (SlotId sid : outputSlotIds) {
                                SlotDescriptor slotDesc = analyzer.getSlotDesc(sid);
                                outputColumnUniqueIds.add(slotDesc.getUniqueId());
                            }
                        }
                    }
                    // OlapScanNode is the last node.
                    // So, just get the two node and check if they are SortNode and OlapScan.
                    parent = node;
                    node = node.getChildren().get(0);
                }

                if (parent instanceof SortNode) {
                    SortNode sortNode = (SortNode) parent;
                    List<Expr> orderingExprs = sortNode.getSortInfo().getOrigOrderingExprs();
                    if (orderingExprs != null) {
                        for (Expr expr : orderingExprs) {
                            if (expr instanceof SlotRef) {
                                if (((SlotRef) expr).getColumn() != null) {
                                    outputColumnUniqueIds.add(((SlotRef) expr).getColumn().getUniqueId());
                                }
                            }
                        }
                    }
                }
                if (!(node instanceof OlapScanNode)) {
                    continue;
                }

                OlapScanNode scanNode = (OlapScanNode) node;
                scanNode.setOutputColumnUniqueIds(outputColumnUniqueIds);
            }
        } else {
            // add '-1' to avoid the optimization incorrect work with OriginalPlanner,
            // because in the storage layer will skip this optimization if outputColumnUniqueIds contains '-1',
            // to ensure the optimization only correct work with nereidsPlanner
            outputColumnUniqueIds.add(-1);

            for (PlanFragment fragment : fragments) {
                PlanNode node = fragment.getPlanRoot();
                if (!(node instanceof OlapScanNode)) {
                    continue;
                }

                OlapScanNode scanNode = (OlapScanNode) node;
                scanNode.setOutputColumnUniqueIds(outputColumnUniqueIds);
            }
        }
    }

    /**
     * optimize for topn query like: SELECT * FROM t1 WHERE a>100 ORDER BY b,c LIMIT 100
     * the pre-requirement is as follows:
     * 1. only contains SortNode + OlapScanNode
     * 2. limit > 0
     * 3. first expression of order by is a table column
     */
    private void checkAndSetTopnOpt(PlanNode node) {
        if (node instanceof SortNode && node.getChildren().size() == 1) {
            SortNode sortNode = (SortNode) node;
            PlanNode child = sortNode.getChild(0);
            if (child instanceof OlapScanNode && sortNode.getLimit() > 0
                    && ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null
                    && sortNode.getLimit() <= ConnectContext.get().getSessionVariable().topnOptLimitThreshold
                    && sortNode.getSortInfo().getOrigOrderingExprs().size() > 0) {
                Expr firstSortExpr = sortNode.getSortInfo().getOrigOrderingExprs().get(0);
                if (firstSortExpr instanceof SlotRef && !firstSortExpr.getType().isStringType()
                        && !firstSortExpr.getType().isFloatingPointType()) {
                    OlapScanNode scanNode = (OlapScanNode) child;
                    if (scanNode.isDupKeysOrMergeOnWrite()) {
                        sortNode.setUseTopnOpt(true);
                        scanNode.setUseTopnOpt(true);
                    }
                }
            }
        }
    }

    /**
     * Construct a tuple for file status, the tuple schema as following:
     * | FileNumber | Int     |
     * | TotalRows  | Bigint  |
     * | FileSize   | Bigint  |
     * | URL        | Varchar |
     */
    private TupleDescriptor constructFileStatusTupleDesc(Analyzer analyzer) {
        TupleDescriptor resultFileStatusTupleDesc =
                analyzer.getDescTbl().createTupleDescriptor("result_file_status");
        resultFileStatusTupleDesc.setIsMaterialized(true);
        SlotDescriptor fileNumber = analyzer.getDescTbl().addSlotDescriptor(resultFileStatusTupleDesc);
        fileNumber.setLabel("FileNumber");
        fileNumber.setType(ScalarType.createType(PrimitiveType.INT));
        fileNumber.setIsMaterialized(true);
        fileNumber.setIsNullable(false);
        SlotDescriptor totalRows = analyzer.getDescTbl().addSlotDescriptor(resultFileStatusTupleDesc);
        totalRows.setLabel("TotalRows");
        totalRows.setType(ScalarType.createType(PrimitiveType.BIGINT));
        totalRows.setIsMaterialized(true);
        totalRows.setIsNullable(false);
        SlotDescriptor fileSize = analyzer.getDescTbl().addSlotDescriptor(resultFileStatusTupleDesc);
        fileSize.setLabel("FileSize");
        fileSize.setType(ScalarType.createType(PrimitiveType.BIGINT));
        fileSize.setIsMaterialized(true);
        fileSize.setIsNullable(false);
        SlotDescriptor url = analyzer.getDescTbl().addSlotDescriptor(resultFileStatusTupleDesc);
        url.setLabel("URL");
        url.setType(ScalarType.createType(PrimitiveType.VARCHAR));
        url.setIsMaterialized(true);
        url.setIsNullable(false);
        resultFileStatusTupleDesc.computeStatAndMemLayout();
        return resultFileStatusTupleDesc;
    }

    private static class QueryStatisticsTransferOptimizer {
        private final PlanFragment root;

        public QueryStatisticsTransferOptimizer(PlanFragment root) {
            Preconditions.checkNotNull(root);
            this.root = root;
        }

        public void optimizeQueryStatisticsTransfer() {
            optimizeQueryStatisticsTransfer(root, null);
        }

        private void optimizeQueryStatisticsTransfer(PlanFragment fragment, PlanFragment parent) {
            if (parent != null && hasLimit(parent.getPlanRoot(), fragment.getPlanRoot())) {
                fragment.setTransferQueryStatisticsWithEveryBatch(true);
            }
            for (PlanFragment child : fragment.getChildren()) {
                optimizeQueryStatisticsTransfer(child, fragment);
            }
        }

        // Check whether leaf node contains limit.
        private boolean hasLimit(PlanNode ancestor, PlanNode successor) {
            final List<PlanNode> exchangeNodes = Lists.newArrayList();
            collectExchangeNode(ancestor, exchangeNodes);
            for (PlanNode leaf : exchangeNodes) {
                if (leaf.getChild(0) == successor
                        && leaf.hasLimit()) {
                    return true;
                }
            }
            return false;
        }

        private void collectExchangeNode(PlanNode planNode, List<PlanNode> exchangeNodes) {
            if (planNode instanceof ExchangeNode) {
                exchangeNodes.add(planNode);
            }

            for (PlanNode child : planNode.getChildren()) {
                if (child instanceof ExchangeNode) {
                    exchangeNodes.add(child);
                } else {
                    collectExchangeNode(child, exchangeNodes);
                }
            }
        }
    }

    @Override
    public DescriptorTable getDescTable() {
        return analyzer.getDescTbl();
    }

    private void collectQueryStat(PlanNode root) {
        try {
            if (root instanceof ScanNode) {
                StatsDelta delta = ((ScanNode) root).genQueryStats();
                if (delta != null && !delta.empty()) {
                    Env.getCurrentEnv().getQueryStats().addStats(delta);
                    if (!delta.getTabletStats().isEmpty()) {
                        Env.getCurrentEnv().getQueryStats().addStats(delta.getTabletStats());
                    }
                }
            }
            for (PlanNode child : root.getChildren()) {
                collectQueryStat(child);
            }
        } catch (UserException e) {
            LOG.info("failed to collect query stat: {}", e.getMessage());
        }
    }

    @Override
    public Optional<ResultSet> handleQueryInFe(StatementBase parsedStmt) {
        if (!(parsedStmt instanceof SelectStmt)) {
            return Optional.empty();
        }
        SelectStmt parsedSelectStmt = (SelectStmt) parsedStmt;
        if (!parsedSelectStmt.getTableRefs().isEmpty()) {
            return Optional.empty();
        }
        List<SelectListItem> selectItems = parsedSelectStmt.getSelectList().getItems();
        List<Column> columns = new ArrayList<>(selectItems.size());
        List<String> columnLabels = parsedSelectStmt.getColLabels();
        List<String> data = new ArrayList<>();
        for (int i = 0; i < selectItems.size(); i++) {
            SelectListItem item = selectItems.get(i);
            Expr expr = item.getExpr();
            String columnName = columnLabels.get(i);
            if (expr instanceof LiteralExpr) {
                columns.add(new Column(columnName, expr.getType()));
                super.handleLiteralInFe((LiteralExpr) expr, data);
            } else {
                return Optional.empty();
            }
        }
        ResultSetMetaData metadata = new CommonResultSet.CommonResultSetMetaData(columns);
        ResultSet resultSet = new CommonResultSet(metadata, Collections.singletonList(data));
        return Optional.of(resultSet);
    }
}
