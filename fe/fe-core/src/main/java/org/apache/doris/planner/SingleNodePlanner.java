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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/SingleNodePlanner.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.AnalyticExpr;
import org.apache.doris.analysis.AnalyticInfo;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.AssertNumRowsElement;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.CaseWhenClause;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.GroupByClause;
import org.apache.doris.analysis.GroupingInfo;
import org.apache.doris.analysis.InlineViewRef;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.LateralViewRef;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SetOperationStmt;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TableValuedFunctionRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Reference;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.es.source.EsScanNode;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.source.HiveScanNode;
import org.apache.doris.datasource.hudi.source.HudiScanNode;
import org.apache.doris.datasource.iceberg.source.IcebergScanNode;
import org.apache.doris.datasource.jdbc.source.JdbcScanNode;
import org.apache.doris.datasource.lakesoul.source.LakeSoulScanNode;
import org.apache.doris.datasource.maxcompute.source.MaxComputeScanNode;
import org.apache.doris.datasource.odbc.source.OdbcScanNode;
import org.apache.doris.datasource.paimon.source.PaimonScanNode;
import org.apache.doris.datasource.trinoconnector.source.TrinoConnectorScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.mvrewrite.MVSelectFailedException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Constructs a non-executable single-node plan from an analyzed parse tree.
 * The single-node plan does not contain data exchanges or data-reduction optimizations
 * such as local aggregations that are important for distributed execution.
 * The single-node plan needs to be wrapped in a plan fragment for it to be executable.
 */
public class SingleNodePlanner {
    private static final Logger LOG = LogManager.getLogger(SingleNodePlanner.class);

    private final PlannerContext ctx;
    private final ArrayList<ScanNode> scanNodes = Lists.newArrayList();
    private Map<Analyzer, List<ScanNode>> selectStmtToScanNodes = Maps.newHashMap();

    public SingleNodePlanner(PlannerContext ctx) {
        this.ctx = ctx;
    }

    public PlannerContext getPlannerContext() {
        return ctx;
    }

    public ArrayList<ScanNode> getScanNodes() {
        return scanNodes;
    }

    /**
     * Generates and returns the root of the single-node plan for the analyzed parse tree
     * in the planner context. The planning process recursively walks the parse tree and
     * performs the following actions.
     * In the top-down phase over query statements:
     * - Materialize the slots required for evaluating expressions of that statement.
     * - Migrate conjuncts from parent blocks into inline views and union operands.
     * In the bottom-up phase generate the plan tree for every query statement:
     * - Generate the plan for the FROM-clause of a select statement: The plan trees of
     * absolute and uncorrelated table refs are connected via JoinNodes. The relative
     * and correlated table refs are associated with one or more SubplanNodes.
     * - A new SubplanNode is placed on top of an existing plan node whenever the tuples
     * materialized by that plan node enable evaluation of one or more relative or
     * correlated table refs, i.e., SubplanNodes are placed at the lowest possible point
     * in the plan, often right after a ScanNode materializing the (single) parent tuple.
     * - The right-hand side of each SubplanNode is a plan tree generated by joining a
     * SingularRowSrcTableRef with those applicable relative and correlated refs.
     * A SingularRowSrcTableRef represents the current row being processed by the
     * SubplanNode from its input (first child).
     * - Connecting table ref plans via JoinNodes is done in a cost-based fashion
     * (join-order optimization). All materialized slots, including those of tuples
     * materialized inside a SubplanNode, must be known for an accurate estimate of row
     * sizes needed for cost-based join ordering.
     * - The remaining aggregate/analytic/orderby portions of a select statement are added
     * on top of the FROM-clause plan.
     * - Whenever a new node is added to the plan tree, assign conjuncts that can be
     * evaluated at that node and compute the stats of that node (cardinality, etc.).
     * - Apply combined expression substitution map of child plan nodes; if a plan node
     * re-maps its input, set a substitution map to be applied by parents.
     */
    public PlanNode createSingleNodePlan() throws UserException, AnalysisException {
        QueryStmt queryStmt = ctx.getQueryStmt();
        // Use the stmt's analyzer which is not necessarily the root analyzer
        // to detect empty result sets.
        Analyzer analyzer = queryStmt.getAnalyzer();
        // TODO(zc)
        // analyzer.computeEquivClasses();
        // ctx_.getAnalysisResult().getTimeline().markEvent("Equivalence classes computed");

        // Mark slots referenced by output exprs as materialized, prior to generating the
        // plan tree.
        // We need to mark the result exprs of the topmost select block as materialized, so
        // that PlanNode.init() can compute the final mem layout of materialized tuples
        // (the byte size of tuples is needed for cost computations).
        // TODO: instead of materializing everything produced by the plan root, derive
        // referenced slots from destination fragment and add a materialization node
        // if not all output is needed by destination fragment
        // TODO 2: should the materialization decision be cost-based?
        if (queryStmt.getBaseTblResultExprs() != null) {
            analyzer.materializeSlots(queryStmt.getBaseTblResultExprs());
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("desctbl: " + analyzer.getDescTbl().debugString());
        }
        long sqlSelectLimit = -1;
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
            sqlSelectLimit = ConnectContext.get().getSessionVariable().getSqlSelectLimit();
        }
        PlanNode singleNodePlan = createQueryPlan(queryStmt, analyzer,
                ctx.getQueryOptions().getDefaultOrderByLimit(), sqlSelectLimit);
        Preconditions.checkNotNull(singleNodePlan);
        analyzer.getDescTbl().materializeIntermediateSlots();
        return singleNodePlan;
    }

    /**
     * Creates an EmptyNode that 'materializes' the tuples of the given stmt.
     * Marks all collection-typed slots referenced in stmt as non-materialized because
     * they are never unnested, and therefore the corresponding parent scan should not
     * materialize them.
     */
    private PlanNode createEmptyNode(PlanNode inputPlan, QueryStmt stmt, Analyzer analyzer) throws UserException {
        ArrayList<TupleId> tupleIds = Lists.newArrayList();
        if (inputPlan != null) {
            tupleIds.addAll(inputPlan.getOutputTupleIds());
        }
        if (tupleIds.isEmpty()) {
            // Constant selects do not have materialized tuples at this stage.
            Preconditions.checkState(stmt instanceof SelectStmt,
                    "Only constant selects should have no materialized tuples");
            SelectStmt selectStmt = (SelectStmt) stmt;
            Preconditions.checkState(selectStmt.getTableRefs().isEmpty());
            tupleIds.add(createResultTupleDescriptor(selectStmt, "empty", analyzer).getId());
        }
        unmarkCollectionSlots(stmt);
        EmptySetNode node = new EmptySetNode(ctx.getNextNodeId(), tupleIds);
        node.init(analyzer);
        // Set the output smap to resolve exprs referencing inline views within stmt.
        // Not needed for a UnionStmt because it materializes its input operands.
        if (stmt instanceof SelectStmt) {
            node.setOutputSmap(((SelectStmt) stmt).getBaseTblSmap(), analyzer);
        }
        return node;
    }

    /**
     * Mark all collection-typed slots in stmt as non-materialized.
     */
    private void unmarkCollectionSlots(QueryStmt stmt) {
        List<TableRef> tblRefs = Lists.newArrayList();
        stmt.collectTableRefs(tblRefs);
        for (TableRef ref : tblRefs) {
            if (!ref.isRelative()) {
                continue;
            }
            // Preconditions.checkState(ref instanceof CollectionTableRef);
            // CollectionTableRef collTblRef = (CollectionTableRef) ref;
            // Expr collExpr = collTblRef.getCollectionExpr();
            // Preconditions.checkState(collExpr instanceof SlotRef);
            // SlotRef collSlotRef = (SlotRef) collExpr;
            // collSlotRef.getDesc().setIsMaterialized(false);
            // collSlotRef.getDesc().getParent().recomputeMemLayout();
        }
    }

    /**
     * Create plan tree for single-node execution. Generates PlanNodes for the
     * Select/Project/Join/Union [All]/Group by/Having/Order by clauses of the query stmt.
     */
    private PlanNode createQueryPlan(QueryStmt stmt, Analyzer analyzer, long defaultOrderByLimit, long sqlSelectLimit)
            throws UserException {
        long newDefaultOrderByLimit = defaultOrderByLimit;
        long defaultLimit = analyzer.getContext().getSessionVariable().getDefaultOrderByLimit();
        if (newDefaultOrderByLimit == -1) {
            if (defaultLimit <= -1) {
                newDefaultOrderByLimit = Long.MAX_VALUE;
            } else {
                newDefaultOrderByLimit = defaultLimit;
            }
        }
        PlanNode root;
        if (stmt instanceof SelectStmt) {
            SelectStmt selectStmt = (SelectStmt) stmt;
            pushDownPredicates(analyzer, selectStmt);

            root = createSelectPlan(selectStmt, analyzer, newDefaultOrderByLimit);

            // TODO(zc)
            // insert possible AnalyticEvalNode before SortNode
            if (selectStmt.getAnalyticInfo() != null) {
                AnalyticInfo analyticInfo = selectStmt.getAnalyticInfo();
                AnalyticPlanner analyticPlanner = new AnalyticPlanner(analyticInfo, analyzer, ctx);
                List<Expr> inputPartitionExprs = Lists.newArrayList();
                AggregateInfo aggInfo = selectStmt.getAggInfo();
                root = analyticPlanner.createSingleNodePlan(root,
                        aggInfo != null ? aggInfo.getGroupingExprs() : null, inputPartitionExprs);
                List<Expr> predicates = getBoundPredicates(analyzer,
                        selectStmt.getAnalyticInfo().getOutputTupleDesc());
                if (!predicates.isEmpty()) {
                    root = new SelectNode(ctx.getNextNodeId(), root, predicates);
                    root.init(analyzer);
                    Preconditions.checkState(root.hasValidStats());
                }
                if (aggInfo != null && !inputPartitionExprs.isEmpty()) {
                    // analytic computation will benefit from a partition on inputPartitionExprs
                    aggInfo.setPartitionExprs(inputPartitionExprs);
                }
            }
        } else {
            Preconditions.checkState(stmt instanceof SetOperationStmt);
            root = createSetOperationPlan((SetOperationStmt) stmt, analyzer, newDefaultOrderByLimit, sqlSelectLimit);
        }
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setQueryJoinReorderFinishTime();
        }

        // Avoid adding a sort node if the sort tuple has no materialized slots.
        boolean sortHasMaterializedSlots = false;
        if (stmt.evaluateOrderBy()) {
            for (SlotDescriptor sortSlotDesc :
                    stmt.getSortInfo().getSortTupleDescriptor().getSlots()) {
                if (sortSlotDesc.isMaterialized()) {
                    sortHasMaterializedSlots = true;
                    break;
                }
            }
        }

        if (stmt.evaluateOrderBy() && sortHasMaterializedSlots) {
            long limit = stmt.getLimit();
            // TODO: External sort could be used for very large limits
            // not just unlimited order-by
            boolean useTopN = true;
            if (limit == -1 && analyzer.getContext().getSessionVariable().enableSpilling) {
                useTopN = false;
            }
            root = new SortNode(ctx.getNextNodeId(), root, stmt.getSortInfo(),
                    useTopN);
            ((SortNode) root).setDefaultLimit(limit == -1);
            root.setOffset(stmt.getOffset());
            if (useTopN) {
                if (sqlSelectLimit >= 0) {
                    newDefaultOrderByLimit = Math.min(newDefaultOrderByLimit, sqlSelectLimit);
                }
                if (newDefaultOrderByLimit == Long.MAX_VALUE) {
                    root.setLimit(limit);
                } else {
                    root.setLimit(limit != -1 ? limit : newDefaultOrderByLimit);
                }
            } else {
                root.setLimit(limit);
            }
            Preconditions.checkState(root.hasValidStats());
            root.init(analyzer);
            // TODO chenhao, before merge ValueTransferGraph, force evaluate conjuncts
            // from SelectStmt outside
            root = addUnassignedConjuncts(analyzer, root);
        } else {
            if (!stmt.hasLimit() && sqlSelectLimit >= 0) {
                root.setLimitAndOffset(sqlSelectLimit, stmt.getOffset());
            } else {
                root.setLimitAndOffset(stmt.getLimit(), stmt.getOffset());
            }
            root.computeStats(analyzer);
        }

        // adding assert node at the end of single node planner
        if (stmt.getAssertNumRowsElement() != null) {
            root = createAssertRowCountNode(root, stmt.getAssertNumRowsElement(), analyzer);
        }

        if (analyzer.hasEmptyResultSet() || root.getLimit() == 0) {
            // Must clear the scanNodes, otherwise we will get NPE in Coordinator::computeScanRangeAssignment
            Set<TupleId> scanTupleIds = new HashSet<>(root.getAllScanTupleIds());
            scanNodes.removeIf(scanNode -> scanTupleIds.contains(scanNode.getTupleIds().get(0)));
            PlanNode node = createEmptyNode(root, stmt, analyzer);
            // Ensure result exprs will be substituted by right outputSmap
            node.setOutputSmap(root.outputSmap, analyzer);
            return node;
        }

        return root;
    }

    /**
     * If there are unassigned conjuncts that are bound by tupleIds or if there are slot
     * equivalences for tupleIds that have not yet been enforced, returns a SelectNode on
     * top of root that evaluates those conjuncts; otherwise returns root unchanged.
     * TODO: change this to assign the unassigned conjuncts to root itself, if that is
     * semantically correct
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
        SelectNode selectNode = new SelectNode(ctx.getNextNodeId(), root, conjuncts);
        selectNode.init(analyzer);
        Preconditions.checkState(selectNode.hasValidStats());
        return selectNode;
    }

    private PlanNode addUnassignedConjuncts(
            Analyzer analyzer, List<TupleId> tupleIds, PlanNode root) throws UserException {
        // No point in adding SelectNode on top of an EmptyNode.
        if (root instanceof EmptySetNode) {
            return root;
        }
        Preconditions.checkNotNull(root);
        // Gather unassigned conjuncts and generate predicates to enfore
        // slot equivalences for each tuple id.
        List<Expr> conjuncts = analyzer.getUnassignedConjuncts(root);
        if (conjuncts.isEmpty()) {
            return root;
        }
        // evaluate conjuncts in SelectNode
        SelectNode selectNode = new SelectNode(ctx.getNextNodeId(), root, conjuncts);
        // init() marks conjuncts as assigned
        selectNode.init(analyzer);
        Preconditions.checkState(selectNode.hasValidStats());
        return selectNode;
    }

    private TPushAggOp freshTPushAggOpByName(String functionName, TPushAggOp originAggOp) {
        TPushAggOp newPushAggOp = null;
        if (functionName.equalsIgnoreCase("COUNT")) {
            newPushAggOp = TPushAggOp.COUNT;
        } else {
            newPushAggOp = TPushAggOp.MINMAX;
        }

        if (originAggOp == null || newPushAggOp == originAggOp) {
            return newPushAggOp;
        }
        return TPushAggOp.MIX;
    }

    private void pushDownAggNoGrouping(AggregateInfo aggInfo, SelectStmt selectStmt, Analyzer analyzer, PlanNode root) {
        do {
            if (CollectionUtils.isNotEmpty(root.getConjuncts())
                    || CollectionUtils.isNotEmpty(root.getProjectList())) {
                break;
            }

            // TODO: Support muti table in the future
            if (selectStmt.getTableRefs().size() != 1) {
                break;
            }

            // No not support group by and where clause
            if (null == aggInfo || !aggInfo.getGroupingExprs().isEmpty()) {
                break;
            }
            List<Expr> allConjuncts = analyzer.getAllConjuncts(selectStmt.getTableRefs().get(0).getId());
            // planner will generate some conjuncts for analysis. These conjuncts are marked as aux expr.
            // we skip these conjuncts when do pushAggOp
            if (allConjuncts != null && allConjuncts.stream().noneMatch(Expr::isAuxExpr)) {
                break;
            }

            List<FunctionCallExpr> aggExprs = aggInfo.getAggregateExprs();
            boolean aggExprValidate = true;
            TPushAggOp aggOp = null;
            for (FunctionCallExpr aggExpr : aggExprs) {
                String functionName = aggExpr.getFnName().getFunction();
                if (!functionName.equalsIgnoreCase("MAX")
                        && !functionName.equalsIgnoreCase("MIN")
                        && !functionName.equalsIgnoreCase("COUNT")) {
                    aggExprValidate = false;
                    break;
                }

                if (!root.pushDownAggNoGrouping(aggExpr)) {
                    aggExprValidate = false;
                    break;
                }

                aggOp = freshTPushAggOpByName(functionName, aggOp);

                if (aggExpr.getChildren().size() > 1) {
                    aggExprValidate = false;
                    break;
                }

                boolean returnColumnValidate = true;
                if (aggExpr.getChildren().size() == 1) {
                    List<Column> returnColumns = Lists.newArrayList();
                    if (!(aggExpr.getChild(0) instanceof SlotRef)) {
                        Expr child = aggExpr.getChild(0);
                        if ((child instanceof CastExpr) && (child.getChild(0) instanceof SlotRef)) {
                            if (child.getType().isNumericType()
                                    && child.getChild(0).getType().isNumericType()) {
                                returnColumns.add(((SlotRef) child.getChild(0)).getDesc().getColumn());
                            } else {
                                aggExprValidate = false;
                                break;
                            }
                        } else {
                            // want to support the agg of count(const value) in dup table
                            aggExprValidate = (aggOp == TPushAggOp.COUNT && child.isConstant() && !child.isNullable());
                            if (!aggExprValidate) {
                                break;
                            }
                        }
                    } else {
                        returnColumns.add(((SlotRef) aggExpr.getChild(0)).getDesc().getColumn());
                    }


                    // check return columns
                    for (Column col : returnColumns) {
                        // TODO(zc): Here column is null is too bad
                        // Only column of Inline-view will be null
                        if (col == null) {
                            continue;
                        }

                        if (!root.pushDownAggNoGroupingCheckCol(aggExpr, col)) {
                            returnColumnValidate = false;
                            break;
                        }

                        // The zone map max length of CharFamily is 512, do not
                        // over the length: https://github.com/apache/doris/pull/6293
                        if (aggOp == TPushAggOp.MINMAX || aggOp == TPushAggOp.MIX) {
                            PrimitiveType colType = col.getDataType();
                            if (colType.isComplexType() || colType.isHllType() || colType.isBitmapType()
                                    || colType == PrimitiveType.STRING) {
                                returnColumnValidate = false;
                                break;
                            }

                            if (colType.isCharFamily() && col.getType().getLength() > 512) {
                                returnColumnValidate = false;
                                break;
                            }
                        }

                        if (aggOp == TPushAggOp.COUNT || aggOp == TPushAggOp.MIX) {
                            // NULL value behavior in `count` function is zero, so
                            // we should not use row_count to speed up query. the col
                            // must be not null
                            if (col.isAllowNull()) {
                                returnColumnValidate = false;
                                break;
                            }
                        }
                    }
                }

                if (!returnColumnValidate) {
                    aggExprValidate = false;
                    break;
                }
            }

            if (!aggExprValidate) {
                break;
            }

            root.setPushDownAggNoGrouping(aggOp);
        } while (false);
    }

    private void turnOffPreAgg(AggregateInfo aggInfo, SelectStmt selectStmt, Analyzer analyzer, PlanNode root) {
        String turnOffReason = null;
        do {
            if (!(root instanceof OlapScanNode)) {
                turnOffReason = "left-deep Node is not OlapScanNode";
                break;
            }

            if (((OlapScanNode) root).getForceOpenPreAgg()) {
                ((OlapScanNode) root).setIsPreAggregation(true, "");
                return;
            }

            if (null == aggInfo) {
                turnOffReason = "No AggregateInfo";
                break;
            }

            ArrayList<FunctionCallExpr> aggExprs = aggInfo.getAggregateExprs();
            // multi table join
            boolean aggTableValidate = true;
            if (selectStmt.getTableRefs().size() > 1) {
                for (int i = 1; i < selectStmt.getTableRefs().size(); ++i) {
                    final JoinOperator joinOperator = selectStmt.getTableRefs().get(i).getJoinOp();
                    // TODO chenhao , right out join ?
                    if (joinOperator.isRightOuterJoin() || joinOperator.isFullOuterJoin()) {
                        turnOffReason = selectStmt.getTableRefs().get(i)
                                + " joinOp is full outer join or right outer join.";
                        aggTableValidate = false;
                        break;
                    }
                }
                if (!aggTableValidate) {
                    break;
                }
                for (FunctionCallExpr aggExpr : aggExprs) {
                    TableRef olapTableRef = selectStmt.getTableRefs().get(0);
                    if (Expr.isBound(Lists.newArrayList(aggExpr), Lists.newArrayList(olapTableRef.getId()))) {
                        // do nothing
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("All agg exprs is bound to olapTable: {}" + olapTableRef.getTable().getName());
                        }
                    } else {
                        List<TupleId> tupleIds = Lists.newArrayList();
                        List<SlotId> slotIds = Lists.newArrayList();
                        aggExpr.getIds(tupleIds, slotIds);
                        for (TupleId tupleId : tupleIds) {
                            // if tupleid is agg's result tuple, there is no tableref
                            // for only scanNode has the tableref
                            if (analyzer.getTupleDesc(tupleId).getRef() == null) {
                                aggTableValidate = false;
                                break;
                            }

                            if (analyzer.getTupleDesc(tupleId).getRef() != olapTableRef) {
                                if (analyzer.getTupleDesc(tupleId).getTable() != null
                                        && analyzer.getTupleDesc(tupleId).getTable().getType()
                                        == Table.TableType.OLAP) {
                                    turnOffReason = "agg expr [" + aggExpr.debugString() + "] is not bound ["
                                            + selectStmt.getTableRefs().get(0).toSql() + "]";
                                    aggTableValidate = false;
                                } else {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("The table which agg expr [{}] is bound to, is not OLAP table [{}]",
                                                aggExpr.debugString(),
                                                analyzer.getTupleDesc(tupleId).getTable() == null ? "inline view" :
                                                        analyzer.getTupleDesc(tupleId).getTable().getName());
                                    }
                                }
                            }

                        }
                    }
                }

                if (!aggTableValidate) {
                    break;
                }
            }

            boolean valueColumnValidate = true;
            List<Expr> allConjuncts = analyzer.getAllConjuncts(selectStmt.getTableRefs().get(0).getId());
            List<SlotId> conjunctSlotIds = Lists.newArrayList();
            if (allConjuncts != null) {
                for (Expr conjunct : allConjuncts) {
                    if (!conjunct.isAuxExpr()) {
                        conjunct.getIds(null, conjunctSlotIds);
                    }
                }
                for (SlotDescriptor slot : selectStmt.getTableRefs().get(0).getDesc().getSlots()) {
                    if (!slot.getColumn().isKey()) {
                        if (conjunctSlotIds.contains(slot.getId())) {
                            turnOffReason = "conjunct on `" + slot.getColumn().getName()
                                    + "` which is StorageEngine value column";
                            valueColumnValidate = false;
                            break;
                        }
                    }
                }
            }
            if (!valueColumnValidate) {
                break;
            }

            boolean aggExprValidate = true;
            for (FunctionCallExpr aggExpr : aggExprs) {
                if (aggExpr.getChildren().size() != 1) {
                    turnOffReason = "aggExpr has more than one child";
                    aggExprValidate = false;
                    break;
                }

                List<Column> returnColumns = Lists.newArrayList();
                List<Column> conditionColumns = Lists.newArrayList();
                if (!(aggExpr.getChild(0) instanceof SlotRef)) {
                    Expr child = aggExpr.getChild(0);

                    // ignore cast
                    boolean castReturnExprValidate = true;
                    while (child instanceof CastExpr) {
                        if (child.getChild(0) instanceof SlotRef) {
                            if (child.getType().isNumericType() && child.getChild(0).getType().isNumericType()) {
                                returnColumns.add(((SlotRef) child.getChild(0)).getDesc().getColumn());
                            } else {
                                turnOffReason = "aggExpr.getChild(0)["
                                    + aggExpr.getChild(0).toSql()
                                    + "] is not Numeric CastExpr";
                                castReturnExprValidate = false;
                                break;
                            }
                        }
                        child = child.getChild(0);
                    }
                    if (!castReturnExprValidate) {
                        aggExprValidate = false;
                        break;
                    }
                    // convert IF to CASE WHEN.
                    // For example:
                    // IF(a > 1, 1, 0) -> CASE WHEN a > 1 THEN 1 ELSE 0 END
                    if (child instanceof FunctionCallExpr && ((FunctionCallExpr) child)
                            .getFnName().getFunction().equalsIgnoreCase("IF")) {
                        Preconditions.checkArgument(child.getChildren().size() == 3);
                        CaseWhenClause caseWhenClause = new CaseWhenClause(child.getChild(0), child.getChild(1));
                        child = new CaseExpr(ImmutableList.of(caseWhenClause), child.getChild(2));
                    }
                    if (child instanceof CaseExpr) {
                        CaseExpr caseExpr = (CaseExpr) child;
                        List<Expr> conditionExprs = caseExpr.getConditionExprs();
                        for (Expr conditionExpr : conditionExprs) {
                            List<TupleId> conditionTupleIds = Lists.newArrayList();
                            List<SlotId> conditionSlotIds = Lists.newArrayList();
                            conditionExpr.getIds(conditionTupleIds, conditionSlotIds);

                            for (SlotId conditionSlotId : conditionSlotIds) {
                                conditionColumns.add(analyzer.getDescTbl().getSlotDesc(conditionSlotId).getColumn());
                            }
                        }

                        boolean caseReturnExprValidate = true;
                        List<Expr> returnExprs = caseExpr.getReturnExprs();
                        for (Expr returnExpr : returnExprs) {
                            // ignore cast in return expr
                            while (returnExpr instanceof CastExpr) {
                                returnExpr = returnExpr.getChild(0);
                            }
                            if (returnExpr instanceof SlotRef) {
                                returnColumns.add(((SlotRef) returnExpr).getDesc().getColumn());
                            } else if (returnExpr.isNullLiteral() || returnExpr.isZeroLiteral()) {
                                // If then expr is NULL or Zero, open the preaggregation
                            } else {
                                turnOffReason = "aggExpr.getChild(0)[" + aggExpr.getChild(0).toSql()
                                        + "] is not SlotExpr";
                                caseReturnExprValidate = false;
                                break;
                            }
                        }

                        if (!caseReturnExprValidate) {
                            aggExprValidate = false;
                            break;
                        }

                    } else {
                        turnOffReason = "aggExpr.getChild(0)[" + aggExpr.getChild(0).debugString()
                                + "] is not SlotRef or CastExpr|CaseExpr";
                        aggExprValidate = false;
                        break;
                    }
                } else {
                    returnColumns.add(((SlotRef) aggExpr.getChild(0)).getDesc().getColumn());
                }

                // check condition columns
                boolean conditionColumnValidate = true;
                for (Column col : conditionColumns) {
                    // TODO(zc): Here column is null is too bad
                    // Only column of Inline-view will be null
                    if (col == null) {
                        continue;
                    }
                    if (!col.isKey()) {
                        turnOffReason = "the condition column [" + col.getName() + "] is not key type in aggr expr ["
                                + aggExpr.toSql() + "].";
                        conditionColumnValidate = false;
                        break;
                    }
                }
                if (!conditionColumnValidate) {
                    aggExprValidate = false;
                    break;
                }

                // check return columns
                boolean returnColumnValidate = true;
                for (Column col : returnColumns) {
                    // TODO(zc): Here column is null is too bad
                    // Only column of Inline-view will be null
                    if (col == null) {
                        continue;
                    }

                    String functionName = aggExpr.getFnName().getFunction();

                    if (col.isKey()) {
                        if ((!functionName.equalsIgnoreCase("MAX"))
                                && (!aggExpr.getFnName().getFunction().equalsIgnoreCase("MIN"))) {
                            returnColumnValidate = false;
                            turnOffReason = "the type of agg on StorageEngine's Key column should only be MAX or MIN."
                                    + "agg expr: " + aggExpr.toSql();
                            break;
                        }
                    }

                    if (functionName.equalsIgnoreCase("SUM")) {
                        if (col.getAggregationType() != AggregateType.SUM) {
                            turnOffReason = "Aggregate Operator not match: SUM <--> " + col.getAggregationType();
                            returnColumnValidate = false;
                            break;
                        }
                    } else if (functionName.equalsIgnoreCase("MAX")) {
                        if ((!col.isKey()) && col.getAggregationType() != AggregateType.MAX) {
                            turnOffReason = "Aggregate Operator not match: MAX <--> " + col.getAggregationType();
                            returnColumnValidate = false;
                            break;
                        }
                    } else if (functionName.equalsIgnoreCase("MIN")) {
                        if ((!col.isKey()) && col.getAggregationType() != AggregateType.MIN) {
                            turnOffReason = "Aggregate Operator not match: MIN <--> " + col.getAggregationType();
                            returnColumnValidate = false;
                            break;
                        }
                    } else if (functionName.equalsIgnoreCase(FunctionSet.HLL_UNION_AGG)
                            || functionName.equalsIgnoreCase(FunctionSet.HLL_RAW_AGG)
                            || functionName.equalsIgnoreCase(FunctionSet.HLL_UNION)) {
                        if (col.getAggregationType() != AggregateType.HLL_UNION) {
                            turnOffReason =
                                    "Aggregate Operator not match: HLL_UNION <--> " + col.getAggregationType();
                            returnColumnValidate = false;
                            break;
                        }
                    } else if (functionName.equalsIgnoreCase("NDV")) {
                        if ((!col.isKey())) {
                            turnOffReason = "NDV function with non-key column: " + col.getName();
                            returnColumnValidate = false;
                            break;
                        }
                    } else if (functionName.equalsIgnoreCase(FunctionSet.BITMAP_UNION_INT)) {
                        if ((!col.isKey())) {
                            turnOffReason = "BITMAP_UNION_INT function with non-key column: " + col.getName();
                            returnColumnValidate = false;
                            break;
                        }
                    } else if (functionName.equalsIgnoreCase(FunctionSet.BITMAP_UNION)
                            || functionName.equalsIgnoreCase(FunctionSet.BITMAP_UNION_COUNT)
                            || functionName.equalsIgnoreCase(FunctionSet.ORTHOGONAL_BITMAP_UNION_COUNT)) {
                        if (col.getAggregationType() != AggregateType.BITMAP_UNION) {
                            turnOffReason =
                                    "Aggregate Operator not match: BITMAP_UNION <--> " + col.getAggregationType();
                            returnColumnValidate = false;
                            break;
                        }
                    } else if (functionName.equalsIgnoreCase(FunctionSet.QUANTILE_UNION)) {
                        if (col.getAggregationType() != AggregateType.QUANTILE_UNION) {
                            turnOffReason =
                                    "Aggregate Operator not match: QUANTILE_UNION <---> " + col.getAggregationType();
                            returnColumnValidate = false;
                            break;
                        }
                    } else if (functionName.equalsIgnoreCase("multi_distinct_count")) {
                        // count(distinct k1), count(distinct k2) / count(distinct k1,k2) can turn on pre aggregation
                        if ((!col.isKey())) {
                            turnOffReason = "Multi count or sum distinct with non-key column: " + col.getName();
                            returnColumnValidate = false;
                            break;
                        }
                    } else {
                        turnOffReason = "Invalid Aggregate Operator: " + functionName;
                        returnColumnValidate = false;
                        break;
                    }
                }
                if (!returnColumnValidate) {
                    aggExprValidate = false;
                    break;
                }
            }

            if (!aggExprValidate) {
                break;
            }

            boolean groupExprValidate = true;
            ArrayList<Expr> groupExprs = aggInfo.getGroupingExprs();
            for (Expr groupExpr : groupExprs) {
                List<SlotId> groupSlotIds = Lists.newArrayList();
                groupExpr.getIds(null, groupSlotIds);

                for (SlotDescriptor slot : selectStmt.getTableRefs().get(0).getDesc().getSlots()) {
                    if (!slot.getColumn().isKey()) {
                        if (groupSlotIds.contains(slot.getId())) {
                            turnOffReason = "groupExpr contains StorageEngine's Value";
                            groupExprValidate = false;
                            break;
                        }
                    }
                }
                if (!groupExprValidate) {
                    break;
                }
            }

            if (!groupExprValidate) {
                break;
            }

            OlapScanNode olapNode = (OlapScanNode) root;
            if (!olapNode.getCanTurnOnPreAggr()) {
                turnOffReason = "this olap scan node[" + olapNode.debugString()
                        + "] has already been turned off pre-aggregation.";
                break;
            }

            olapNode.setIsPreAggregation(true, null);
        } while (false);

        if ((root instanceof OlapScanNode) && turnOffReason != null) {
            ((OlapScanNode) root).setIsPreAggregation(false, turnOffReason);
        }
    }

    /**
     * Return the cheapest plan that materializes the joins of all TableRefs in
     * refPlans and the subplans of all applicable TableRefs in subplanRefs.
     * Assumes that refPlans are in the order as they originally appeared in
     * the query.
     * For this plan:
     * - the plan is executable, ie, all non-cross joins have equi-join predicates
     * - the leftmost scan is over the largest of the inputs for which we can still
     * construct an executable plan
     * - from bottom to top, all rhs's are in increasing order of selectivity (percentage
     * of surviving rows)
     * - outer/cross/semi joins: rhs serialized size is < lhs serialized size;
     * enforced via join inversion, if necessary
     * - SubplanNodes are placed as low as possible in the plan tree - as soon as the
     * required tuple ids of one or more TableRefs in subplanRefs are materialized
     * Returns null if we can't create an executable plan.
     */
    private PlanNode createCheapestJoinPlan(Analyzer analyzer,
            List<Pair<TableRef, PlanNode>> refPlans) throws UserException {
        if (refPlans.size() == 1) {
            return refPlans.get(0).second;
        }

        // collect eligible candidates for the leftmost input; list contains
        // (plan, materialized size)
        List<Pair<TableRef, Long>> candidates = new ArrayList<>();
        for (Pair<TableRef, PlanNode> entry : refPlans) {
            TableRef ref = entry.first;
            JoinOperator joinOp = ref.getJoinOp();

            // Avoid reordering outer/semi joins which is generally incorrect.
            // consideration of the joinOps that result from such a re-ordering (IMPALA-1281).
            // TODO: Allow the rhs of any cross join as the leftmost table. This needs careful
            if (joinOp.isOuterJoin() || joinOp.isSemiJoin() || joinOp.isCrossJoin()) {
                continue;
            }

            PlanNode plan = entry.second;
            if (plan.getCardinality() == -1) {
                // use 0 for the size to avoid it becoming the leftmost input
                // TODO: Consider raw size of scanned partitions in the absence of stats.
                candidates.add(Pair.of(ref, new Long(0)));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("The candidate of " + ref.getUniqueAlias() + ": -1. "
                            + "Using 0 instead of -1 to avoid error");
                }
                continue;
            }
            Preconditions.checkState(ref.isAnalyzed());
            long materializedSize = plan.getCardinality();
            candidates.add(Pair.of(ref, new Long(materializedSize)));
            if (LOG.isDebugEnabled()) {
                LOG.debug("The candidate of " + ref.getUniqueAlias() + ": " + materializedSize);
            }
        }
        if (candidates.isEmpty()) {
            // This branch should not be reached, because the first one should be inner join.
            LOG.warn("Something wrong happens, the code should not be runned");
            return null;
        }

        // order candidates by descending materialized size; we want to minimize the memory
        // consumption of the materialized hash tables required for the join sequence
        Collections.sort(candidates,
                new Comparator<Pair<TableRef, Long>>() {
                    @Override
                    public int compare(Pair<TableRef, Long> a, Pair<TableRef, Long> b) {
                        long diff = b.second - a.second;
                        return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
                    }
                });

        for (Pair<TableRef, Long> candidate : candidates) {
            PlanNode result = createJoinPlan(analyzer, candidate.first, refPlans);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    boolean candidateCardinalityIsSmaller(PlanNode candidate, long candidateInnerNodeCardinality,
                                          PlanNode newRoot, long newRootInnerNodeCardinality) {
        if (candidate.getCardinality() < newRoot.getCardinality()) {
            return true;
        } else if (candidate.getCardinality() == newRoot.getCardinality()) {
            if (((candidate instanceof HashJoinNode) && ((HashJoinNode) candidate).getJoinOp().isInnerJoin())
                    && ((newRoot instanceof HashJoinNode) && ((HashJoinNode) newRoot).getJoinOp().isInnerJoin())) {
                if (candidateInnerNodeCardinality < newRootInnerNodeCardinality) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns a plan with leftmostRef's plan as its leftmost input; the joins
     * are in decreasing order of selectiveness (percentage of rows they eliminate).
     * Creates and adds subplan nodes as soon as the tuple ids required by at least one
     * subplan ref are materialized by a join node added during plan generation.
     */
    // (ML): change the function name
    private PlanNode createJoinPlan(Analyzer analyzer, TableRef leftmostRef, List<Pair<TableRef, PlanNode>> refPlans)
            throws UserException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Try to create a query plan starting with " + leftmostRef.getUniqueAlias());
        }

        // the refs that have yet to be joined
        List<Pair<TableRef, PlanNode>> remainingRefs = new ArrayList<>();
        PlanNode root = null;  // root of accumulated join plan
        for (Pair<TableRef, PlanNode> entry : refPlans) {
            if (entry.first == leftmostRef) {
                root = entry.second;
            } else {
                remainingRefs.add(entry);
            }
        }
        Preconditions.checkNotNull(root);

        // Maps from a TableRef in refPlans with an outer/semi join op to the set of
        // TableRefs that precede it refPlans (i.e., in FROM-clause order).
        // The map is used to place outer/semi joins at a fixed position in the plan tree
        // (IMPALA-860), s.t. all the tables appearing to the left/right of an outer/semi
        // join in the original query still remain to the left/right after join ordering.
        // This prevents join re-ordering across outer/semi joins which is generally wrong.


        // Key of precedingRefs: the right table ref of outer or semi join
        // Value of precedingRefs: the preceding refs of this key
        // For example:
        // select * from t1, t2, t3 left join t4, t5, t6 right semi join t7, t8, t9
        // Map:
        // { t4: [t1, t2, t3],
        //   t7: [t1, t2, t3, t4, t5, t6]
        // }
        Map<TableRef, Set<TableRef>> precedingRefs = new HashMap<>();
        List<TableRef> tmpTblRefs = new ArrayList<>();
        for (Pair<TableRef, PlanNode> entry : refPlans) {
            TableRef tblRef = entry.first;
            if (tblRef.getJoinOp().isOuterJoin() || tblRef.getJoinOp().isSemiJoin()) {
                precedingRefs.put(tblRef, Sets.newHashSet(tmpTblRefs));
            }
            tmpTblRefs.add(tblRef);
        }

        // Refs that have been joined. The union of joinedRefs and the refs in remainingRefs
        // are the set of all table refs.
        Set<TableRef> joinedRefs = Sets.newHashSet(leftmostRef);
        // two statistical value
        long numOps = 0;
        // A total of several rounds of successful selection
        int successfulSelectionTimes = 0;
        while (!remainingRefs.isEmpty()) {
            // We minimize the resulting cardinality at each step in the join chain,
            // which minimizes the total number of hash table lookups.
            PlanNode newRoot = null;
            Pair<TableRef, PlanNode> minEntry = null;
            long newRootRightChildCardinality = 0;
            for (Pair<TableRef, PlanNode> tblRefToPlanNodeOfCandidate : remainingRefs) {
                TableRef tblRefOfCandidate = tblRefToPlanNodeOfCandidate.first;
                long cardinalityOfCandidate = tblRefToPlanNodeOfCandidate.second.getCardinality();
                PlanNode rootPlanNodeOfCandidate = tblRefToPlanNodeOfCandidate.second;
                JoinOperator joinOp = tblRefOfCandidate.getJoinOp();

                // Place outer/semi joins at a fixed position in the plan tree.
                Set<TableRef> requiredRefs = precedingRefs.get(tblRefOfCandidate);
                if (requiredRefs != null) {
                    Preconditions.checkState(joinOp.isOuterJoin()
                            || joinOp.isSemiJoin());
                    // The semi and outer join nodes are similar to the stop nodes in each round of the algorithm.
                    // If the stop node is encountered during the current round of optimal selection,
                    // it means that the following nodes do not need to be referred to.
                    // This round has been completed.
                    // There are two situation in here.
                    // Situation 1: required table refs have not been placed yet
                    // t1, t2, t3 left join t4, t5
                    //     Round 1: t3, t1(new root) meets t4(stop)
                    //              stop this round and begin next round
                    // Situation 2: the remaining table refs to prevent incorrect re-ordering
                    //              of tables across outer/semi joins
                    //     Round 1: t5, t1, t2, t3(root) meets t4(stop)
                    //              stop this round while the new root is null
                    //              planning failed and return null
                    if (!requiredRefs.equals(joinedRefs)) {
                        break;
                    }
                }
                // reset assigned conjuncts of analyzer in every compare
                analyzer.setAssignedConjuncts(root.getAssignedConjuncts());
                PlanNode candidate = createJoinNode(analyzer, root, rootPlanNodeOfCandidate, tblRefOfCandidate);
                // it may not return null, but protect.
                if (candidate == null) {
                    continue;
                }
                // Have the build side of a join copy data to a compact representation
                // in the tuple buffer.
                candidate.getChildren().get(1).setCompactData(true);

                if (LOG.isDebugEnabled()) {
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append("The " + tblRefOfCandidate.getUniqueAlias() + " is right child of join node.");
                    stringBuilder.append("The join cardinality is " + candidate.getCardinality() + ".");
                    stringBuilder.append("In round " + successfulSelectionTimes);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(stringBuilder.toString());
                    }
                }

                // Use 'candidate' as the new root; don't consider any other table refs at this
                // position in the plan.
                if (joinOp.isOuterJoin() || joinOp.isSemiJoin()) {
                    newRoot = candidate;
                    minEntry = tblRefToPlanNodeOfCandidate;
                    break;
                }

                // Always prefer Hash Join over Nested-Loop Join due to limited costing
                // infrastructure.
                //
                // The following three conditions are met while the candidate is better.
                // 1. The first candidate
                // 2. The candidate is better than new root: [t3, t2] pk [t3, t1] => [t3, t1]
                // 3. The hash join is better than cross join: [t3 cross t1] pk [t3 hash t2] => t3 hash t2
                if (newRoot == null
                        || ((candidate.getClass().equals(newRoot.getClass())
                        && candidateCardinalityIsSmaller(
                                candidate, tblRefToPlanNodeOfCandidate.second.getCardinality(),
                                newRoot, newRootRightChildCardinality)))
                        || (candidate instanceof HashJoinNode && newRoot instanceof NestedLoopJoinNode)) {
                    newRoot = candidate;
                    minEntry = tblRefToPlanNodeOfCandidate;
                    newRootRightChildCardinality = cardinalityOfCandidate;
                }
            }

            // The table after the outer or semi join is wrongly planned to the front,
            // causing the first tblRefToPlanNodeOfCandidate (outer or semi tbl ref)
            // in this round of loop to fail and exit the loop.
            // This means that the current leftmost node must be wrong, and the correct result cannot be planned.
            //
            // For example:
            // Query: t1 left join t2 inner join t3
            // Input params: t3(left most tbl ref), [t1,t2] (remaining refs)
            //     Round 1: t3, t1 (joined refs) t2 (remaining refs)
            //     Round 2: requiredRefs.equals(joinedRefs) is false and break, the newRoot is null
            // Result: null
            // The t3 should not appear before t2 so planning is fail
            if (newRoot == null) {
                // Could not generate a valid plan.
                // for example: the biggest table is the last table
                return null;
            }

            // we need to insert every rhs row into the hash table and then look up
            // every lhs row
            long lhsCardinality = root.getCardinality();
            long rhsCardinality = minEntry.second.getCardinality();
            numOps += lhsCardinality + rhsCardinality;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Round " + successfulSelectionTimes + " chose " + minEntry.first.getUniqueAlias()
                        + " #lhs=" + lhsCardinality + " #rhs=" + rhsCardinality + " #ops=" + numOps);
            }
            remainingRefs.remove(minEntry);
            joinedRefs.add(minEntry.first);
            root = newRoot;
            analyzer.setAssignedConjuncts(root.getAssignedConjuncts());
            ++successfulSelectionTimes;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("The final join sequence is "
                    + joinedRefs.stream().map(TableRef::getUniqueAlias).collect(Collectors.joining(",")));
        }
        return root;
    }

    /**
     * Create tree of PlanNodes that implements the Select/Project/Join/Group by/Having
     * of the selectStmt query block.
     */
    private PlanNode createSelectPlan(SelectStmt selectStmt, Analyzer analyzer, long defaultOrderByLimit)
            throws UserException, AnalysisException {
        // no from clause -> nothing to plan
        if (selectStmt.getTableRefs().isEmpty()) {
            return createConstantSelectPlan(selectStmt, analyzer);
        }

        if (analyzer.enableStarJoinReorder()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("use old reorder logical in select stmt");
            }
            selectStmt.reorderTable(analyzer);
        }

        // Slot materialization:
        // We need to mark all slots as materialized that are needed during the execution
        // of selectStmt, and we need to do that prior to creating plans for the TableRefs
        // (because createTableRefNode() might end up calling computeMemLayout() on one or
        // more TupleDescriptors, at which point all referenced slots need to be marked).
        //
        // For non-join predicates, slots are marked as follows:
        // - for base table scan predicates, this is done directly by ScanNode.init(), which
        //   can do a better job because it doesn't need to materialize slots that are only
        //   referenced for partition pruning, for instance
        // - for inline views, non-join predicates are pushed down, at which point the
        //   process repeats itself.
        selectStmt.materializeRequiredSlots(analyzer);

        // collect ids of tuples materialized by the subtree that includes all joins
        // and scans
        ArrayList<TupleId> rowTuples = Lists.newArrayList();
        for (TableRef tblRef : selectStmt.getTableRefs()) {
            rowTuples.addAll(tblRef.getMaterializedTupleIds());
        }

        if (analyzer.hasEmptySpjResultSet() && selectStmt.getAggInfo() != null) {
            GroupByClause groupByClause = selectStmt.getGroupByClause();
            if (Objects.nonNull(groupByClause) && groupByClause.isGroupByExtension()) {
                rowTuples.add(selectStmt.getGroupingInfo().getVirtualTuple().getId());
            }
            final PlanNode emptySetNode = new EmptySetNode(ctx.getNextNodeId(), rowTuples);
            emptySetNode.init(analyzer);
            emptySetNode.setOutputSmap(selectStmt.getBaseTblSmap(), analyzer);
            return createAggregationPlan(selectStmt, analyzer, emptySetNode);
        }

        PlanNode root = null;
        AggregateInfo aggInfo = selectStmt.getAggInfo();

        if (analyzer.safeIsEnableJoinReorderBasedCost()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Using new join reorder strategy when enable_join_reorder_based_cost is true");
            }
            // create plans for our table refs; use a list here instead of a map to
            // maintain a deterministic order of traversing the TableRefs during join
            // plan generation (helps with tests)
            List<Pair<TableRef, PlanNode>> refPlans = Lists.newArrayList();
            for (TableRef ref : selectStmt.getTableRefs()) {
                materializeTableResultForCrossJoinOrCountStar(ref, analyzer);
                PlanNode plan = createTableRefNode(analyzer, ref, selectStmt);
                turnOffPreAgg(aggInfo, selectStmt, analyzer, plan);
                if (ConnectContext.get().getSessionVariable().enablePushDownNoGroupAgg) {
                    pushDownAggNoGrouping(aggInfo, selectStmt, analyzer, plan);
                }

                if (plan instanceof OlapScanNode) {
                    OlapScanNode olapNode = (OlapScanNode) plan;
                    // this olap scan node has been turn off pre-aggregation, should not be turned on again.
                    // e.g. select sum(v1) from (select v1 from test_table);
                    if (!olapNode.isPreAggregation()) {
                        olapNode.setCanTurnOnPreAggr(false);
                    }
                }

                Preconditions.checkState(plan != null);
                refPlans.add(Pair.of(ref, plan));
            }
            // save state of conjunct assignment; needed for join plan generation
            for (Pair<TableRef, PlanNode> entry : refPlans) {
                entry.second.setAssignedConjuncts(analyzer.getAssignedConjuncts());
            }
            root = createCheapestJoinPlan(analyzer, refPlans);
            Preconditions.checkState(root != null);
        } else {
            // create left-deep sequence of binary hash joins; assign node ids as we go along
            TableRef tblRef = selectStmt.getTableRefs().get(0);
            materializeTableResultForCrossJoinOrCountStar(tblRef, analyzer);
            if (selectStmt.getResultExprs().size() == 1) {
                final List<SlotId> slotIds = Lists.newArrayList();
                final List<TupleId> tupleIds = Lists.newArrayList();
                Expr resultExprSelected =  selectStmt.getResultExprs().get(0);
                if (resultExprSelected != null && resultExprSelected instanceof SlotRef) {
                    resultExprSelected.getIds(tupleIds, slotIds);
                    for (SlotId id : slotIds) {
                        final SlotDescriptor slot = analyzer.getDescTbl().getSlotDesc(id);
                        slot.setIsMaterialized(true);
                        slot.materializeSrcExpr();
                    }
                    for (TupleId id : tupleIds) {
                        final TupleDescriptor tuple = analyzer.getDescTbl().getTupleDesc(id);
                        tuple.setIsMaterialized(true);
                    }
                }
            }
            root = createTableRefNode(analyzer, tblRef, selectStmt);
            // to change the inner contains analytic function
            // selectStmt.seondSubstituteInlineViewExprs(analyzer.getChangeResSmap());

            turnOffPreAgg(aggInfo, selectStmt, analyzer, root);
            if (ConnectContext.get().getSessionVariable().enablePushDownNoGroupAgg) {
                pushDownAggNoGrouping(aggInfo, selectStmt, analyzer, root);
            }

            if (root instanceof OlapScanNode) {
                OlapScanNode olapNode = (OlapScanNode) root;
                // this olap scan node has been turn off pre-aggregation, should not be turned on again.
                // e .g. select sum(v1) from (select v1 from test_table);
                if (!olapNode.isPreAggregation()) {
                    olapNode.setCanTurnOnPreAggr(false);
                }
            }

            for (int i = 1; i < selectStmt.getTableRefs().size(); ++i) {
                TableRef innerRef = selectStmt.getTableRefs().get(i);
                // Optimization of CountStar would change the materialization of some outputSlot,
                // it would cause the materilization inconsistent of SlotRef between outputTupleDesc
                // and agg/groupBy expr. So if some not materialized slots in outputTuple of aggInfo
                // is set to materialized after the optimization of CountStar, we need to call
                // aggregateInfo.materializeRequiredSlots again to make sure all required slots is
                // materialized.
                boolean aggOutputNotHaveMaterializedSlot = false;
                AggregateInfo aggregateInfo = null;
                TupleDescriptor output = null;
                if (innerRef instanceof InlineViewRef) {
                    InlineViewRef inlineViewRef = (InlineViewRef) innerRef;
                    QueryStmt queryStmt = inlineViewRef.getViewStmt();
                    if (queryStmt instanceof SelectStmt) {
                        aggregateInfo = ((SelectStmt) queryStmt).getAggInfo();
                        if (aggregateInfo != null) {
                            output = aggregateInfo.getOutputTupleDesc();
                            aggOutputNotHaveMaterializedSlot =
                                    output.getSlots().stream().noneMatch(SlotDescriptor::isMaterialized);
                        }
                    }
                }

                root = createJoinNode(analyzer, root, innerRef, selectStmt);
                // Have the build side of a join copy data to a compact representation
                // in the tuple buffer.
                if (aggOutputNotHaveMaterializedSlot
                        && aggregateInfo
                        .getOutputTupleDesc()
                        .getSlots()
                        .stream()
                        .anyMatch(SlotDescriptor::isMaterialized)) {
                    aggregateInfo.materializeRequiredSlots(analyzer, null);
                }
                root.getChildren().get(1).setCompactData(true);
                root.assignConjuncts(analyzer);
            }
        }

        if (selectStmt.getSortInfo() != null && selectStmt.getLimit() == -1
                && defaultOrderByLimit == -1) {
            // TODO: only use topN if the memory footprint is expected to be low;
            // how to account for strings?
            throw new AnalysisException("ORDER BY without LIMIT currently not supported");
        }

        if (root != null) {
            // add unassigned conjuncts before aggregation
            // (scenario: agg input comes from an inline view which wasn't able to
            // evaluate all Where clause conjuncts from this scope)
            if (!selectStmt.hasOrderByClause()) {
                root = addUnassignedConjuncts(analyzer, root);
            }
        }

        // add aggregation, if required
        if (aggInfo != null) {
            // introduce repeatNode for group by extension
            GroupByClause groupByClause = selectStmt.getGroupByClause();
            if (groupByClause != null && groupByClause.isGroupByExtension()) {
                root = createRepeatNodePlan(selectStmt, analyzer, root);
            }

            root = createAggregationPlan(selectStmt, analyzer, root);
        }

        return root;
    }

    /**
     * Returns a new RepeatNode.
     */
    private PlanNode createRepeatNodePlan(SelectStmt selectStmt, Analyzer analyzer,
                                          PlanNode root) throws UserException {
        GroupByClause groupByClause = selectStmt.getGroupByClause();
        GroupingInfo groupingInfo = selectStmt.getGroupingInfo();
        Preconditions.checkState(groupByClause != null && groupByClause.isGroupByExtension()
                && groupingInfo != null);
        root = new RepeatNode(ctx.getNextNodeId(), root, groupingInfo, groupByClause);
        root.init(analyzer);
        return root;
    }

    public boolean selectMaterializedView(QueryStmt queryStmt, Analyzer analyzer) throws UserException {
        boolean selectFailed = false;
        boolean haveError = false;
        StringBuilder errorMsg = new StringBuilder("select fail reason: ");
        if (queryStmt instanceof SelectStmt) {
            SelectStmt selectStmt = (SelectStmt) queryStmt;
            Set<TupleId> disableTuplesMVRewriter = Sets.newHashSet();
            for (TableRef tableRef : selectStmt.getTableRefs()) {
                if (tableRef instanceof InlineViewRef) {
                    selectFailed |= selectMaterializedView(((InlineViewRef) tableRef).getViewStmt(),
                            ((InlineViewRef) tableRef).getAnalyzer());
                }
            }
            List<ScanNode> scanNodeList = selectStmtToScanNodes.get(selectStmt.getAnalyzer());
            if (scanNodeList == null) {
                return selectFailed;
            }
            MaterializedViewSelector materializedViewSelector = new MaterializedViewSelector(selectStmt, analyzer);
            for (ScanNode scanNode : scanNodeList) {
                if (!(scanNode instanceof OlapScanNode)) {
                    continue;
                }
                OlapScanNode olapScanNode = (OlapScanNode) scanNode;
                if (olapScanNode.getSelectedPartitionIds().size() == 0 && !FeConstants.runningUnitTest) {
                    continue;
                }
                boolean tupleSelectFailed = false;

                try {
                    // select index by the old Rollup selector
                    olapScanNode.selectBestRollupByRollupSelector(analyzer);
                } catch (UserException e) {
                    tupleSelectFailed = true;
                }

                // select index by the new Materialized selector
                MaterializedViewSelector.BestIndexInfo bestIndexInfo = materializedViewSelector
                        .selectBestMV(olapScanNode);
                if (bestIndexInfo == null) {
                    tupleSelectFailed = true;
                } else {
                    try {
                        // if the new selected index id is different from the old one, scan node will be
                        // updated.
                        olapScanNode.updateScanRangeInfoByNewMVSelector(bestIndexInfo.getBestIndexId(),
                                bestIndexInfo.isPreAggregation(), bestIndexInfo.getReasonOfDisable());

                        // mv index have where clause, so where expr on scan node is unused.
                        olapScanNode.ignoreConjuncts(olapScanNode.getOlapTable()
                                .getIndexMetaByIndexId(bestIndexInfo.getBestIndexId())
                                .getWhereClause());

                        if (selectStmt.getAggInfo() != null) {
                            selectStmt.getAggInfo().updateTypeOfAggregateExprs();
                        }
                    } catch (Exception e) {
                        if (haveError) {
                            errorMsg.append(",");
                        }
                        errorMsg.append(e.getMessage());
                        haveError = true;
                        tupleSelectFailed = true;
                    }
                }
                if (tupleSelectFailed) {
                    selectFailed = true;
                    disableTuplesMVRewriter.add(olapScanNode.getTupleId());
                }
            }
            selectStmt.updateDisableTuplesMVRewriter(disableTuplesMVRewriter);
        } else {
            Preconditions.checkState(queryStmt instanceof SetOperationStmt);
            SetOperationStmt unionStmt = (SetOperationStmt) queryStmt;
            for (SetOperationStmt.SetOperand unionOperand : unionStmt.getOperands()) {
                selectFailed |= selectMaterializedView(unionOperand.getQueryStmt(), analyzer);
            }
        }
        if (haveError) {
            throw new MVSelectFailedException(errorMsg.toString());
        }
        return selectFailed;
    }

    /**
     * Returns a new AggregationNode that materializes the aggregation of the given stmt.
     * Assigns conjuncts from the Having clause to the returned node.
     */
    private PlanNode createAggregationPlan(SelectStmt selectStmt, Analyzer analyzer,
                                           PlanNode root) throws UserException {
        Preconditions.checkState(selectStmt.getAggInfo() != null);
        // add aggregation, if required
        AggregateInfo aggInfo = selectStmt.getAggInfo();
        // aggInfo.substitueGroupingExpr(analyzer);
        PlanNode newRoot = new AggregationNode(ctx.getNextNodeId(), root, aggInfo);
        newRoot.init(analyzer);
        Preconditions.checkState(newRoot.hasValidStats());
        // if we're computing DISTINCT agg fns, the analyzer already created the
        // 2nd phase agginfo
        if (aggInfo.isDistinctAgg()) {
            ((AggregationNode) newRoot).unsetNeedsFinalize();
            // The output of the 1st phase agg is the 1st phase intermediate.
            ((AggregationNode) newRoot).setIntermediateTuple();
            newRoot = new AggregationNode(ctx.getNextNodeId(), newRoot,
                    aggInfo.getSecondPhaseDistinctAggInfo());
            newRoot.init(analyzer);
            Preconditions.checkState(newRoot.hasValidStats());
        }
        // add Having clause
        newRoot.assignConjuncts(analyzer);
        return newRoot;
    }

    /**
     * Returns a MergeNode that materializes the exprs of the constant selectStmt. Replaces the resultExprs of the
     * selectStmt with SlotRefs into the materialized tuple.
     */
    private PlanNode createConstantSelectPlan(SelectStmt selectStmt, Analyzer analyzer) throws UserException {
        Preconditions.checkState(selectStmt.getTableRefs().isEmpty());
        ArrayList<Expr> resultExprs = selectStmt.getResultExprs();
        // Create tuple descriptor for materialized tuple.
        TupleDescriptor tupleDesc = createResultTupleDescriptor(selectStmt, "union", analyzer);
        UnionNode unionNode = new UnionNode(ctx.getNextNodeId(), tupleDesc.getId());

        // Analysis guarantees that selects without a FROM clause only have constant exprs.
        if (selectStmt.getValueList() != null) {
            for (ArrayList<Expr> row : selectStmt.getValueList().getRows()) {
                unionNode.addConstExprList(row);
            }
        } else {
            unionNode.addConstExprList(Lists.newArrayList(resultExprs));
        }

        // Replace the select stmt's resultExprs with SlotRefs into tupleDesc.
        for (int i = 0; i < resultExprs.size(); ++i) {
            SlotRef slotRef = new SlotRef(tupleDesc.getSlots().get(i));
            resultExprs.set(i, slotRef);
            selectStmt.getBaseTblResultExprs().set(i, slotRef);
        }

        // UnionNode.init() needs tupleDesc to have been initialized
        unionNode.init(analyzer);
        return unionNode;
    }

    /**
     * Create tuple descriptor that can hold the results of the given SelectStmt, with one
     * slot per result expr.
     */
    private TupleDescriptor createResultTupleDescriptor(SelectStmt selectStmt,
                                                        String debugName, Analyzer analyzer) {
        TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor(
                debugName);
        tupleDesc.setIsMaterialized(true);

        List<Expr> resultExprs = selectStmt.getResultExprs();
        List<String> colLabels = selectStmt.getColLabels();
        for (int i = 0; i < resultExprs.size(); ++i) {
            Expr resultExpr = resultExprs.get(i);
            String colLabel = colLabels.get(i);
            SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
            slotDesc.setLabel(colLabel);
            slotDesc.setSourceExpr(resultExpr);
            slotDesc.setType(resultExpr.getType());
            // slotDesc.setStats(ColumnStats.fromExpr(resultExpr));
            slotDesc.setIsMaterialized(true);
        }
        tupleDesc.computeStatAndMemLayout();
        return tupleDesc;
    }


    /**
     * Returns plan tree for an inline view ref:
     * - predicates from the enclosing scope that can be evaluated directly within
     * the inline-view plan are pushed down
     * - predicates that cannot be evaluated directly within the inline-view plan
     * but only apply to the inline view are evaluated in a SelectNode placed
     * on top of the inline view plan
     * - all slots that are referenced by predicates from the enclosing scope that cannot
     * be pushed down are marked as materialized (so that when computeMemLayout() is
     * called on the base table descriptors materialized by the inline view it has a
     * complete picture)
     */
    private PlanNode createInlineViewPlan(Analyzer analyzer, InlineViewRef inlineViewRef)
            throws UserException, AnalysisException {
        // If possible, "push down" view predicates; this is needed in order to ensure
        // that predicates such as "x + y = 10" are evaluated in the view's plan tree
        // rather than a SelectNode grafted on top of that plan tree.
        // This doesn't prevent predicate propagation, because predicates like
        // "x = 10" that get pushed down are still connected to equivalent slots
        // via the equality predicates created for the view's select list.
        // Include outer join conjuncts here as well because predicates from the
        // On-clause of an outer join may be pushed into the inline view as well.
        migrateConjunctsToInlineView(analyzer, inlineViewRef);

        // Turn a constant select into a MergeNode that materializes the exprs.
        QueryStmt viewStmt = inlineViewRef.getViewStmt();
        if (viewStmt instanceof SelectStmt) {
            SelectStmt selectStmt = (SelectStmt) viewStmt;
            if (selectStmt.getTableRefs().isEmpty()) {
                if (inlineViewRef.getAnalyzer().hasEmptyResultSet()) {
                    PlanNode emptySetNode = createEmptyNode(null, viewStmt, inlineViewRef.getAnalyzer());
                    // Still substitute exprs in parent nodes with the inline-view's smap to make
                    // sure no exprs reference the non-materialized inline view slots. No wrapping
                    // with TupleIsNullPredicates is necessary here because we do not migrate
                    // conjuncts into outer-joined inline views, so hasEmptyResultSet() cannot be
                    // true for an outer-joined inline view that has no table refs.
                    Preconditions.checkState(!analyzer.isOuterJoined(inlineViewRef.getId()));
                    emptySetNode.setOutputSmap(inlineViewRef.getSmap(), analyzer);
                    return emptySetNode;
                }
                // Analysis should have generated a tuple id into which to materialize the exprs.
                Preconditions.checkState(inlineViewRef.getMaterializedTupleIds().size() == 1);
                // we need to materialize all slots of our inline view tuple
                analyzer.getTupleDesc(inlineViewRef.getId()).materializeSlots();
                UnionNode unionNode = new UnionNode(ctx.getNextNodeId(),
                        inlineViewRef.getMaterializedTupleIds().get(0));
                if (analyzer.hasEmptyResultSet()) {
                    return unionNode;
                }
                unionNode.setTblRefIds(Lists.newArrayList(inlineViewRef.getId()));
                if (selectStmt.getValueList() != null) {
                    for (List<Expr> row : selectStmt.getValueList().getRows()) {
                        unionNode.addConstExprList(row);
                    }
                } else {
                    unionNode.addConstExprList(selectStmt.getBaseTblResultExprs());
                }
                unionNode.init(analyzer);
                //set outputSmap to substitute literal in outputExpr
                unionNode.setWithoutTupleIsNullOutputSmap(inlineViewRef.getSmap());
                unionNode.setOutputSmap(inlineViewRef.getSmap(), analyzer);
                return unionNode;
            }
        }

        PlanNode rootNode = createQueryPlan(inlineViewRef.getViewStmt(), inlineViewRef.getAnalyzer(), -1, -1);
        // TODO: we should compute the "physical layout" of the view's descriptor, so that
        // the avg row size is available during optimization; however, that means we need to
        // select references to its resultExprs from the enclosing scope(s)
        rootNode.setTblRefIds(Lists.newArrayList(inlineViewRef.getId()));

        // The output smap is the composition of the inline view's smap and the output smap
        // of the inline view's plan root. This ensures that all downstream exprs referencing
        // the inline view are replaced with exprs referencing the physical output of the
        // inline view's plan.
        ExprSubstitutionMap outputSmap = ExprSubstitutionMap.compose(
                inlineViewRef.getSmap(), rootNode.getOutputSmap(), analyzer);

        // Set output smap of rootNode *before* creating a SelectNode for proper resolution.
        rootNode.setOutputSmap(outputSmap, analyzer);
        if (rootNode instanceof UnionNode && ((UnionNode) rootNode).isConstantUnion()) {
            rootNode.setWithoutTupleIsNullOutputSmap(outputSmap);
        }
        // rootNode.setOutputSmap(ExprSubstitutionMap.compose(inlineViewRef.getBaseTblSmap(),
        //         rootNode.getOutputSmap(), analyzer));
        // Expr.substituteList(inlineViewRef.getViewStmt().getResultExprs(), analyzer.getChangeResSmap());
        // analyzer.setChangeResSmap(inlineViewRef.getAnalyzer().getChangeResSmap());

        //        Expr.SubstitutionMap inlineViewSmap = inlineViewRef.getExprSMap();
        //        if (analyzer.isOuterJoined(inlineViewRef.getId())) {
        //            // Exprs against non-matched rows of an outer join should always return NULL.
        //            // Make the rhs exprs of the inline view's smap nullable, if necessary.
        //            List<Expr> nullableRhs = TupleIsNullPredicate.wrapExprs(
        //                    inlineViewSmap.getRhs(), node.getTupleIds(), analyzer);
        //            inlineViewSmap = new Expr.SubstitutionMap(inlineViewSmap.getLhs(), nullableRhs);
        //        }
        //
        //        // Set output smap of rootNode *before* creating a SelectNode for proper resolution.
        //        // The output smap is the composition of the inline view's smap and the output smap
        //        // of the inline view's plan root. This ensures that all downstream exprs referencing
        //        // the inline view are replaced with exprs referencing the physical output of
        //        // the inline view's plan.
        //        Expr.SubstitutionMap composedSmap = Expr.SubstitutionMap.compose(inlineViewSmap,
        //                node.getOutputSmap(), analyzer);
        //        node.setOutputSmap(composedSmap);

        // If the inline view has a LIMIT/OFFSET or unassigned conjuncts due to analytic
        // functions, we may have conjuncts that need to be assigned to a SELECT node on
        // top of the current plan root node.
        //
        // TODO: This check is also repeated in migrateConjunctsToInlineView() because we
        // need to make sure that equivalences are not enforced multiple times. Consolidate
        // the assignment of conjuncts and the enforcement of equivalences into a single
        // place.
        if (!canMigrateConjuncts(inlineViewRef)) {
            rootNode = addUnassignedConjuncts(
                    analyzer, inlineViewRef.getDesc().getId().asList(), rootNode);
        }
        return rootNode;
    }

    /**
     * Migrates unassigned conjuncts into an inline view. Conjuncts are not
     * migrated into the inline view if the view has a LIMIT/OFFSET clause or if the
     * view's stmt computes analytic functions (see IMPALA-1243/IMPALA-1900).
     * The reason is that analytic functions compute aggregates over their entire input,
     * and applying filters from the enclosing scope *before* the aggregate computation
     * would alter the results. This is unlike regular aggregate computation, which only
     * makes the *output* of the computation visible to the enclosing scope, so that
     * filters from the enclosing scope can be safely applied (to the grouping cols, say).
     */
    public void migrateConjunctsToInlineView(Analyzer analyzer,
                                             InlineViewRef inlineViewRef) throws AnalysisException {
        // All conjuncts
        final List<Expr> unassignedConjuncts =
                analyzer.getUnassignedConjuncts(inlineViewRef.getId().asList(), true);

        // Constant conjuncts
        final List<Expr> unassignedConstantConjuncts = Lists.newArrayList();
        for (Expr e : unassignedConjuncts) {
            if (e.isConstant()) {
                unassignedConstantConjuncts.add(e);
            }
        }
        // Non constant conjuncts
        unassignedConjuncts.removeAll(unassignedConstantConjuncts);
        migrateNonconstantConjuncts(inlineViewRef, unassignedConjuncts, analyzer);
        migrateConstantConjuncts(inlineViewRef, unassignedConstantConjuncts);
    }

    /**
     * For handling non-constant conjuncts. This only substitute conjunct's tuple with InlineView's
     * and register it in InlineView's Analyzer, whcih will be assigned by the next planning.
     *
     * @param inlineViewRef
     * @param unassignedConjuncts
     * @param analyzer
     * @throws AnalysisException
     */
    private void migrateNonconstantConjuncts(
            InlineViewRef inlineViewRef, List<Expr> unassignedConjuncts, Analyzer analyzer) throws AnalysisException {
        final List<Expr> preds = Lists.newArrayList();
        for (Expr e : unassignedConjuncts) {
            if (analyzer.canEvalPredicate(inlineViewRef.getId().asList(), e)) {
                preds.add(e);
            }
        }

        final List<Expr> pushDownFailedPredicates = Lists.newArrayList();
        final List<Expr> viewPredicates = getPushDownPredicatesForInlineView(
                inlineViewRef, preds, analyzer, pushDownFailedPredicates);
        if (viewPredicates.size() <= 0) {
            // mark (fully resolve) slots referenced by unassigned conjuncts as
            // materialized
            List<Expr> substUnassigned = Expr.substituteList(unassignedConjuncts,
                    inlineViewRef.getBaseTblSmap(), analyzer, false);
            analyzer.materializeSlots(substUnassigned);
            return;
        }
        preds.removeAll(pushDownFailedPredicates);
        unassignedConjuncts.removeAll(preds);
        unassignedConjuncts.addAll(pushDownFailedPredicates);

        // Remove unregistered predicates that reference the same slot on
        // both sides (e.g. a = a). Such predicates have been generated from slot
        // equivalences and may incorrectly reject rows with nulls (IMPALA-1412/IMPALA-2643).
        // TODO(zc)
        final Predicate<Expr> isIdentityPredicate = new Predicate<Expr>() {
            @Override
            public boolean apply(Expr expr) {
                return org.apache.doris.analysis.Predicate.isEquivalencePredicate(expr)
                        && ((BinaryPredicate) expr).isInferred()
                        && expr.getChild(0).equals(expr.getChild(1));
            }
        };
        Iterables.removeIf(viewPredicates, isIdentityPredicate);

        // Migrate the conjuncts by marking the original ones as assigned, and
        // re-registering the substituted ones with new ids.
        analyzer.markConjunctsAssigned(preds);
        // Unset the On-clause flag of the migrated conjuncts because the migrated conjuncts
        // apply to the post-join/agg/analytic result of the inline view.
        for (Expr e : viewPredicates) {
            e.setIsOnClauseConjunct(false);
        }
        inlineViewRef.getAnalyzer().registerConjuncts(viewPredicates, inlineViewRef.getAllTupleIds());
        // mark (fully resolve) slots referenced by remaining unassigned conjuncts as
        // materialized
        List<Expr> substUnassigned = Expr.substituteList(unassignedConjuncts,
                inlineViewRef.getBaseTblSmap(), analyzer, false);
        analyzer.materializeSlots(substUnassigned);

    }

    /**
     * For handling constant conjuncts when migrating conjuncts to InlineViews. Constant conjuncts
     * should be assigned to query block from top to bottom, it will try to push down constant conjuncts.
     *
     * @param inlineViewRef
     * @param conjuncts
     * @throws AnalysisException
     */
    private void migrateConstantConjuncts(InlineViewRef inlineViewRef, List<Expr> conjuncts) throws AnalysisException {
        if (conjuncts.isEmpty()) {
            return;
        }

        if (!canMigrateConjuncts(inlineViewRef)) {
            return;
        }

        final QueryStmt stmt = inlineViewRef.getViewStmt();
        final Analyzer viewAnalyzer = inlineViewRef.getAnalyzer();
        viewAnalyzer.markConjunctsAssigned(conjuncts);
        // even if the conjuncts are constant, they may contains slotRef
        // for example: case when slotRef is null then 0 else 1
        // we need substitute the conjuncts using inlineViewRef's analyzer
        // otherwise, when analyzing the conjunct in the inline view
        // the analyzer is not able to find the column because it comes from outside
        List<Expr> newConjuncts =
                Expr.substituteList(conjuncts, inlineViewRef.getSmap(), viewAnalyzer, false);
        if (stmt instanceof SelectStmt) {
            final SelectStmt select = (SelectStmt) stmt;
            if (select.getAggInfo() != null) {
                viewAnalyzer.registerConjuncts(newConjuncts, select.getAggInfo().getOutputTupleId().asList());
            } else if (select.getTableRefs().size() > 0) {
                for (int i = select.getTableRefs().size() - 1; i >= 0; i--) {
                    viewAnalyzer.registerConjuncts(newConjuncts,
                            select.getTableRefs().get(i).getDesc().getId().asList());
                    newConjuncts = cloneExprs(newConjuncts);
                }
            } else {
                for (Expr e : conjuncts) {
                    viewAnalyzer.registerMigrateFailedConjuncts(inlineViewRef, e);
                }
            }
        } else {
            Preconditions.checkArgument(stmt instanceof SetOperationStmt);
            final SetOperationStmt union = (SetOperationStmt) stmt;
            viewAnalyzer.registerConjuncts(newConjuncts, union.getTupleId().asList());
        }
    }

    private List<Expr> cloneExprs(List<Expr> candidates) {
        final List<Expr> clones = Lists.newArrayList();
        candidates.forEach(expr -> clones.add(expr.clone()));
        return clones;
    }

    /**
     * Get predicates can be migrated into an inline view.
     */
    private List<Expr> getPushDownPredicatesForInlineView(
            InlineViewRef inlineViewRef, List<Expr> viewPredicates,
            Analyzer analyzer, List<Expr> pushDownFailedPredicates) {
        // TODO chenhao, remove evaluateOrderBy when SubQuery's default limit is removed.
        final List<Expr> pushDownPredicates = Lists.newArrayList();
        if (inlineViewRef.getViewStmt().evaluateOrderBy()
                || inlineViewRef.getViewStmt().hasLimit()
                || inlineViewRef.getViewStmt().hasOffset()) {
            return pushDownPredicates;
        }

        // UnionNode will handle predicates and assigns predicates to it's children.
        final List<Expr> candicatePredicates =
                Expr.substituteList(viewPredicates, inlineViewRef.getSmap(), analyzer, false);
        if (inlineViewRef.getViewStmt() instanceof SetOperationStmt) {
            final SetOperationStmt setOperationStmt = (SetOperationStmt) inlineViewRef.getViewStmt();
            for (int i = 0; i < candicatePredicates.size(); i++) {
                final Expr predicate = candicatePredicates.get(i);
                if (predicate.isBound(setOperationStmt.getTupleId())) {
                    pushDownPredicates.add(predicate);
                } else {
                    pushDownFailedPredicates.add(viewPredicates.get(i));
                }
            }
            return pushDownPredicates;
        }

        final SelectStmt selectStmt = (SelectStmt) inlineViewRef.getViewStmt();
        if (selectStmt.hasAnalyticInfo()) {
            pushDownPredicates.addAll(getWindowsPushDownPredicates(candicatePredicates, viewPredicates,
                    selectStmt.getAnalyticInfo(), pushDownFailedPredicates));
        } else {
            pushDownPredicates.addAll(candicatePredicates);
        }
        return pushDownPredicates;
    }

    /**
     * Get predicates which can be pushed down past Windows.
     *
     * @param predicates
     * @param viewPredicates
     * @param analyticInfo
     * @param pushDownFailedPredicates
     * @return
     */
    private List<Expr> getWindowsPushDownPredicates(
            List<Expr> predicates, List<Expr> viewPredicates,
            AnalyticInfo analyticInfo, List<Expr> pushDownFailedPredicates) {
        final List<Expr> pushDownPredicates = Lists.newArrayList();
        final List<Expr> partitionExprs = analyticInfo.getCommonPartitionExprs();
        final List<SlotId> partitionByIds = Lists.newArrayList();
        for (Expr expr : partitionExprs) {
            if (expr instanceof SlotRef) {
                final SlotRef slotRef = (SlotRef) expr;
                partitionByIds.add(slotRef.getSlotId());
            }
        }

        if (partitionByIds.size() <= 0) {
            return pushDownPredicates;
        }

        for (int i = 0; i < predicates.size(); i++) {
            final Expr predicate = predicates.get(i);
            if (predicate.isBound(partitionByIds)) {
                pushDownPredicates.add(predicate);
            } else {
                pushDownFailedPredicates.add(viewPredicates.get(i));
            }
        }
        return pushDownPredicates;
    }

    /**
     * Checks if conjuncts can be migrated into an inline view.
     */
    private boolean canMigrateConjuncts(InlineViewRef inlineViewRef) {
        // TODO chenhao, remove evaluateOrderBy when SubQuery's default limit is removed.
        return inlineViewRef.getViewStmt().evaluateOrderBy() ? false :
                (!inlineViewRef.getViewStmt().hasLimit()
                        && !inlineViewRef.getViewStmt().hasOffset()
                        && (!(inlineViewRef.getViewStmt() instanceof SelectStmt)
                        || !((SelectStmt) inlineViewRef.getViewStmt()).hasAnalyticInfo()));
    }

    /**
     * Create node for scanning all data files of a particular table.
     */
    private PlanNode createScanNode(Analyzer analyzer, TableRef tblRef, SelectStmt selectStmt)
            throws UserException {
        ScanNode scanNode = null;

        switch (tblRef.getTable().getType()) {
            case OLAP:
            case MATERIALIZED_VIEW:
                OlapScanNode olapNode = new OlapScanNode(ctx.getNextNodeId(), tblRef.getDesc(),
                        "OlapScanNode");
                olapNode.setForceOpenPreAgg(tblRef.isForcePreAggOpened());
                olapNode.setSampleTabletIds(tblRef.getSampleTabletIds());
                olapNode.setTableSample(tblRef.getTableSample());
                scanNode = olapNode;
                break;
            case ODBC:
                scanNode = new OdbcScanNode(ctx.getNextNodeId(), tblRef.getDesc(), (OdbcTable) tblRef.getTable());
                break;
            case MYSQL:
                scanNode = new MysqlScanNode(ctx.getNextNodeId(), tblRef.getDesc(), (MysqlTable) tblRef.getTable());
                break;
            case SCHEMA:
                if (BackendPartitionedSchemaScanNode.isBackendPartitionedSchemaTable(
                        tblRef.getDesc().getTable().getName())) {
                    scanNode = new BackendPartitionedSchemaScanNode(ctx.getNextNodeId(), tblRef.getDesc());
                } else {
                    scanNode = new SchemaScanNode(ctx.getNextNodeId(), tblRef.getDesc());
                }
                break;
            case BROKER:
                throw new RuntimeException("Broker external table is not supported, try to use table function please");
            case ELASTICSEARCH:
                scanNode = new EsScanNode(ctx.getNextNodeId(), tblRef.getDesc());
                break;
            case HIVE:
                throw new RuntimeException("Hive external table is not supported, try to use hive catalog please");
            case ICEBERG:
                throw new RuntimeException("Iceberg external table is not supported, use iceberg catalog please");
            case JDBC:
                scanNode = new JdbcScanNode(ctx.getNextNodeId(), tblRef.getDesc(), false);
                break;
            case TABLE_VALUED_FUNCTION:
                scanNode = ((TableValuedFunctionRef) tblRef).getScanNode(ctx.getNextNodeId());
                break;
            case HMS_EXTERNAL_TABLE:
                TableIf table = tblRef.getDesc().getTable();
                switch (((HMSExternalTable) table).getDlaType()) {
                    case HUDI:
                        // Old planner does not support hudi incremental read,
                        // so just pass Optional.empty() to HudiScanNode
                        if (tblRef.getScanParams() != null) {
                            throw new UserException("Hudi incremental read is not supported, "
                                    + "please set enable_nereids_planner = true to enable new optimizer");
                        }
                        scanNode = new HudiScanNode(ctx.getNextNodeId(), tblRef.getDesc(), true,
                                Optional.empty(), Optional.empty());
                        break;
                    case ICEBERG:
                        scanNode = new IcebergScanNode(ctx.getNextNodeId(), tblRef.getDesc(), true);
                        break;
                    case HIVE:
                        scanNode = new HiveScanNode(ctx.getNextNodeId(), tblRef.getDesc(), true);
                        ((HiveScanNode) scanNode).setTableSample(tblRef.getTableSample());
                        break;
                    default:
                        throw new UserException("Not supported table type" + table.getType());
                }
                break;
            case ICEBERG_EXTERNAL_TABLE:
                scanNode = new IcebergScanNode(ctx.getNextNodeId(), tblRef.getDesc(), true);
                break;
            case PAIMON_EXTERNAL_TABLE:
                scanNode = new PaimonScanNode(ctx.getNextNodeId(), tblRef.getDesc(), true);
                break;
            case TRINO_CONNECTOR_EXTERNAL_TABLE:
                scanNode = new TrinoConnectorScanNode(ctx.getNextNodeId(), tblRef.getDesc(), true);
                break;
            case MAX_COMPUTE_EXTERNAL_TABLE:
                // TODO: support max compute scan node
                scanNode = new MaxComputeScanNode(ctx.getNextNodeId(), tblRef.getDesc(), "MCScanNode",
                        StatisticalType.MAX_COMPUTE_SCAN_NODE, true);
                break;
            case ES_EXTERNAL_TABLE:
                scanNode = new EsScanNode(ctx.getNextNodeId(), tblRef.getDesc(), true);
                break;
            case JDBC_EXTERNAL_TABLE:
                scanNode = new JdbcScanNode(ctx.getNextNodeId(), tblRef.getDesc(), true);
                break;
            case LAKESOUl_EXTERNAL_TABLE:
                scanNode = new LakeSoulScanNode(ctx.getNextNodeId(), tblRef.getDesc(), true);
                break;
            case TEST_EXTERNAL_TABLE:
                scanNode = new TestExternalTableScanNode(ctx.getNextNodeId(), tblRef.getDesc());
                break;
            default:
                throw new UserException("Not supported table type: " + tblRef.getTable().getType());
        }
        if (scanNode instanceof OlapScanNode || scanNode instanceof EsScanNode
                || scanNode instanceof OdbcScanNode || scanNode instanceof JdbcScanNode
                || scanNode instanceof FileQueryScanNode || scanNode instanceof MysqlScanNode) {
            if (analyzer.enableInferPredicate()) {
                PredicatePushDown.visitScanNode(scanNode, tblRef.getJoinOp(), analyzer);
            }
            scanNode.setSortColumn(tblRef.getSortColumn());
        }

        scanNodes.add(scanNode);
        // now we put the selectStmtToScanNodes's init before the scanNode.init
        List<ScanNode> scanNodeList = selectStmtToScanNodes.computeIfAbsent(
                selectStmt.getAnalyzer(), k -> Lists.newArrayList());
        scanNodeList.add(scanNode);

        scanNode.init(analyzer);
        return scanNode;
    }

    /**
     * Return join conjuncts that can be used for hash table lookups. - for inner joins, those are equi-join predicates
     * in which one side is fully bound by lhsIds and the other by rhs' id; - for outer joins: same type of conjuncts as
     * inner joins, but only from the JOIN clause Returns the original form in 'joinPredicates'.
     */
    private void getHashLookupJoinConjuncts(Analyzer analyzer, PlanNode left, PlanNode right,
                                            List<Expr> joinConjuncts,
                                            Reference<String> errMsg, JoinOperator op) {
        joinConjuncts.clear();
        final List<TupleId> lhsIds = left.getTblRefIds();
        final List<TupleId> rhsIds = right.getTblRefIds();
        List<Expr> candidates;
        candidates = analyzer.getEqJoinConjuncts(lhsIds, rhsIds);
        if (candidates == null) {
            if (op.isOuterJoin() || op.isSemiAntiJoin()) {
                errMsg.setRef("non-equal " + op.toString() + " is not supported");
                LOG.warn(errMsg);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("no candidates for join.");
            }
            return;
        }

        for (Expr e : candidates) {
            // Ignore predicate if one of its children is a constant.
            if (e.getChild(0).isLiteral() || e.getChild(1).isLiteral()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("double is constant.");
                }
                continue;
            }

            /**
             * The left and right child of origin predicates need to be swap sometimes.
             * Case A:
             * select * from t1 join t2 on t2.id=t1.id
             * The left plan node is t1 and the right plan node is t2.
             * The left child of origin predicate is t2.id and the right child of origin predicate is t1.id.
             * In this situation, the children of predicate need to be swap => t1.id=t2.id.
             */
            Expr rhsExpr = null;
            if (e.getChild(0).isBoundByTupleIds(rhsIds)) {
                rhsExpr = e.getChild(0);
            } else {
                Preconditions.checkState(e.getChild(1).isBoundByTupleIds(rhsIds));
                rhsExpr = e.getChild(1);
            }

            Expr lhsExpr = null;
            if (e.getChild(1).isBoundByTupleIds(lhsIds)) {
                lhsExpr = e.getChild(1);
            } else if (e.getChild(0).isBoundByTupleIds(lhsIds)) {
                lhsExpr = e.getChild(0);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("not an equi-join condition between lhsIds and rhsId");
                }
                continue;
            }

            Preconditions.checkState(lhsExpr != rhsExpr);
            Preconditions.checkState(e instanceof BinaryPredicate);
            // The new predicate id must same as the origin predicate.
            // This expr id is used to mark as assigned in the future.
            BinaryPredicate newEqJoinPredicate = (BinaryPredicate) e.clone();
            newEqJoinPredicate.setChild(0, lhsExpr);
            newEqJoinPredicate.setChild(1, rhsExpr);
            joinConjuncts.add(newEqJoinPredicate);
        }
    }

    private PlanNode createJoinNodeBase(Analyzer analyzer, PlanNode outer, PlanNode inner, TableRef innerRef)
            throws UserException {
        materializeTableResultForCrossJoinOrCountStar(innerRef, analyzer);

        List<Expr> eqJoinConjuncts = Lists.newArrayList();
        Reference<String> errMsg = new Reference<String>();
        // get eq join predicates for the TableRefs' ids (not the PlanNodes' ids, which
        // are materialized)
        getHashLookupJoinConjuncts(analyzer, outer, inner,
                eqJoinConjuncts, errMsg, innerRef.getJoinOp());
        analyzer.markConjunctsAssigned(eqJoinConjuncts);

        List<Expr> ojConjuncts = Lists.newArrayList();
        if (innerRef.getJoinOp().isOuterJoin()) {
            // Also assign conjuncts from On clause. All remaining unassigned conjuncts
            // that can be evaluated by this join are assigned in createSelectPlan().
            ojConjuncts = analyzer.getUnassignedOjConjuncts(innerRef);
        } else if (innerRef.getJoinOp().isAntiJoinNullAware()) {
            ojConjuncts = analyzer.getUnassignedAntiJoinNullAwareConjuncts(innerRef);
        } else if (innerRef.getJoinOp().isSemiOrAntiJoinNoNullAware()) {
            ojConjuncts = analyzer.getUnassignedSemiAntiJoinNoNullAwareConjuncts(innerRef);
        }
        analyzer.markConjunctsAssigned(ojConjuncts);
        if (eqJoinConjuncts.isEmpty()) {
            NestedLoopJoinNode result =
                    new NestedLoopJoinNode(ctx.getNextNodeId(), outer, inner, innerRef);
            List<Expr> joinConjuncts = Lists.newArrayList(eqJoinConjuncts);
            joinConjuncts.addAll(ojConjuncts);
            result.setJoinConjuncts(joinConjuncts);
            result.addConjuncts(analyzer.getMarkConjuncts(innerRef));
            result.init(analyzer);
            result.setOutputLeftSideOnly(innerRef.isInBitmap() && joinConjuncts.isEmpty());
            return result;
        }

        HashJoinNode result = new HashJoinNode(ctx.getNextNodeId(), outer, inner,
                innerRef, eqJoinConjuncts, ojConjuncts);
        result.addConjuncts(analyzer.getMarkConjuncts(innerRef));
        result.init(analyzer);
        return result;
    }

    /*
    for joinreorder
    */
    public PlanNode createJoinNode(Analyzer analyzer, PlanNode outer, PlanNode inner, TableRef innerRef)
            throws UserException {
        return createJoinNodeBase(analyzer, outer, inner, innerRef);
    }

    /**
     * Creates a new node to join outer with inner. Collects and assigns join conjunct
     * as well as regular conjuncts. Calls init() on the new join node.
     * Throws if the JoinNode.init() fails.
     */
    private PlanNode createJoinNode(Analyzer analyzer, PlanNode outer, TableRef innerRef,
                                    SelectStmt selectStmt) throws UserException {
        // the rows coming from the build node only need to have space for the tuple
        // materialized by that node
        PlanNode inner = createTableRefNode(analyzer, innerRef, selectStmt);

        return createJoinNodeBase(analyzer, outer, inner, innerRef);
    }

    /**
     * Create a tree of PlanNodes for the given tblRef, which can be a BaseTableRef,
     * CollectionTableRef or an InlineViewRef.
     * <p>
     * 'fastPartitionKeyScans' indicates whether to try to produce the slots with
     * metadata instead of table scans. Only applicable to BaseTableRef which is also
     * an HDFS table.
     * <p>
     * Throws if a PlanNode.init() failed or if planning of the given
     * table ref is not implemented.
     */
    private PlanNode createTableRefNode(Analyzer analyzer, TableRef tblRef, SelectStmt selectStmt)
            throws UserException {
        PlanNode scanNode = null;
        if (tblRef instanceof BaseTableRef || tblRef instanceof TableValuedFunctionRef) {
            scanNode = createScanNode(analyzer, tblRef, selectStmt);
        }
        if (tblRef instanceof InlineViewRef) {
            InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
            scanNode = createInlineViewPlan(analyzer, inlineViewRef);
            Analyzer viewAnalyzer = inlineViewRef.getAnalyzer();
            Set<Expr> exprs = viewAnalyzer.findMigrateFailedConjuncts(inlineViewRef);
            if (CollectionUtils.isNotEmpty(exprs)) {
                for (Expr expr : exprs) {
                    scanNode.addConjunct(expr);
                }
            }
        }
        if (scanNode == null) {
            throw new UserException("unknown TableRef node");
        }
        List<LateralViewRef> lateralViewRefs = tblRef.getLateralViewRefs();
        if (lateralViewRefs == null || lateralViewRefs.size() == 0) {
            return scanNode;
        }
        return createTableFunctionNode(analyzer, scanNode, lateralViewRefs, selectStmt);
    }

    private PlanNode createTableFunctionNode(Analyzer analyzer, PlanNode inputNode,
                                              List<LateralViewRef> lateralViewRefs, SelectStmt selectStmt)
            throws UserException {
        Preconditions.checkNotNull(lateralViewRefs);
        Preconditions.checkState(lateralViewRefs.size() > 0);
        TableFunctionNode tableFunctionNode = new TableFunctionNode(ctx.getNextNodeId(), inputNode,
                lateralViewRefs);
        tableFunctionNode.init(analyzer);
        tableFunctionNode.projectSlots(analyzer, selectStmt);
        inputNode = tableFunctionNode;
        return inputNode;
    }

    /**
     * Create a plan tree corresponding to 'setOperands' for the given SetOperationStmt.
     * The individual operands' plan trees are attached to a single SetOperationNode.
     * If result is not null, it is expected to contain the plan for the
     * distinct portion of the given SetOperationStmt. The result is then added
     * as a child of the returned SetOperationNode.
     */
    private SetOperationNode createSetOperationPlan(
            Analyzer analyzer, SetOperationStmt setOperationStmt, List<SetOperationStmt.SetOperand> setOperands,
            PlanNode result, long defaultOrderByLimit, long sqlSelectLimit)
            throws UserException, AnalysisException {
        SetOperationNode setOpNode;
        SetOperationStmt.Operation operation = null;
        for (SetOperationStmt.SetOperand setOperand : setOperands) {
            if (setOperand.getOperation() != null) {
                if (operation == null) {
                    operation = setOperand.getOperation();
                }
                Preconditions.checkState(operation == setOperand.getOperation(), "can not support mixed set "
                        + "operations at here");
            }
        }
        switch (operation) {
            case UNION:
                setOpNode = new UnionNode(ctx.getNextNodeId(), setOperationStmt.getTupleId(),
                        setOperationStmt.getSetOpsResultExprs(), false);
                break;
            case INTERSECT:
                setOpNode = new IntersectNode(ctx.getNextNodeId(), setOperationStmt.getTupleId(),
                        setOperationStmt.getSetOpsResultExprs(), false);
                break;
            case EXCEPT:
                setOpNode = new ExceptNode(ctx.getNextNodeId(), setOperationStmt.getTupleId(),
                        setOperationStmt.getSetOpsResultExprs(), false);
                break;
            default:
                throw new AnalysisException("not supported set operations: " + operation);
        }
        // If it is a union or other same operation, there are only two possibilities,
        // one is the root node, and the other is a distinct node in front, so the setOperationDistinctPlan will
        // be aggregate node, if this is a mixed operation
        // e.g. :
        // a union b -> result == null
        // a union b union all c -> result == null -> result == AggregationNode
        // a union all b except c -> result == null -> result == UnionNode
        // a union b except c -> result == null -> result == AggregationNode
        if (result != null && result instanceof SetOperationNode) {
            Preconditions.checkState(!result.getClass().equals(setOpNode.getClass()));
            setOpNode.addChild(result, setOperationStmt.getResultExprs());
        } else if (result != null) {
            Preconditions.checkState(setOperationStmt.hasDistinctOps());
            Preconditions.checkState(result instanceof AggregationNode);
            setOpNode.addChild(result,
                    setOperationStmt.getDistinctAggInfo().getGroupingExprs());
        }
        for (SetOperationStmt.SetOperand op : setOperands) {
            if (op.getAnalyzer().hasEmptyResultSet()) {
                unmarkCollectionSlots(op.getQueryStmt());
                continue;
            }
            QueryStmt queryStmt = op.getQueryStmt();
            if (queryStmt instanceof SelectStmt) {
                SelectStmt selectStmt = (SelectStmt) queryStmt;
                if (selectStmt.getTableRefs().isEmpty() && setOpNode instanceof UnionNode) {
                    setOpNode.addConstExprList(selectStmt.getResultExprs());
                    continue;
                }
            }
            PlanNode opPlan = createQueryPlan(queryStmt, op.getAnalyzer(), defaultOrderByLimit, sqlSelectLimit);
            // There may still be unassigned conjuncts if the operand has an order by + limit.
            // Place them into a SelectNode on top of the operand's plan.
            opPlan = addUnassignedConjuncts(analyzer, opPlan.getTupleIds(), opPlan);
            if (opPlan instanceof EmptySetNode) {
                continue;
            }
            setOpNode.addChild(opPlan, op.getQueryStmt().getResultExprs());
        }
        setOpNode.init(analyzer);
        return setOpNode;
    }

    /**
     * Returns plan tree for unionStmt:
     * - distinctOperands' plan trees are collected in a single UnionNode
     * and duplicates removed via distinct aggregation
     * - the output of that plus the allOperands' plan trees are collected in
     * another UnionNode which materializes the result of unionStmt
     * - if any of the union operands contains analytic exprs, we avoid pushing
     * predicates directly into the operands and instead evaluate them
     * *after* the final UnionNode (see createInlineViewPlan() for the reasoning)
     * TODO: optimize this by still pushing predicates into the union operands
     * that don't contain analytic exprs and evaluating the conjuncts in Select
     * directly above the AnalyticEvalNodes
     * TODO: Simplify the plan of unions with empty operands using an empty set node.
     * TODO: Simplify the plan of unions with only a single non-empty operand to not
     * use a union node (this is tricky because a union materializes a new tuple).
     */
    private PlanNode createSetOperationPlan(
            SetOperationStmt setOperationStmt, Analyzer analyzer, long defaultOrderByLimit, long sqlSelectLimit)
            throws UserException, AnalysisException {
        // TODO(zc): get unassigned conjuncts
        // List<Expr> conjuncts =
        //         analyzer.getUnassignedConjuncts(unionStmt.getTupleId().asList(), false);
        List<Expr> conjuncts = analyzer.getUnassignedConjuncts(setOperationStmt.getTupleId().asList());
        // TODO chenhao
        // Because Conjuncts can't be assigned to UnionNode and Palo's fe can't evaluate conjuncts,
        // it needs to add SelectNode as UnionNode's parent, when UnionStmt's Ops contains constant
        // Select.
        boolean hasConstantOp = false;
        if (!setOperationStmt.hasAnalyticExprs()) {
            // Turn unassigned predicates for unionStmt's tupleId_ into predicates for
            // the individual operands.
            // Do this prior to creating the operands' plan trees so they get a chance to
            // pick up propagated predicates.
            for (SetOperationStmt.SetOperand op : setOperationStmt.getOperands()) {
                List<Expr> opConjuncts =
                        Expr.substituteList(conjuncts, op.getSmap(), analyzer, false);
                boolean selectHasTableRef = true;
                final QueryStmt queryStmt = op.getQueryStmt();
                // Check whether UnionOperand is constant Select.
                if (queryStmt instanceof SelectStmt) {
                    final SelectStmt selectStmt = (SelectStmt) queryStmt;
                    if (selectStmt.getTableRefs().isEmpty()) {
                        selectHasTableRef = false;
                        hasConstantOp = !selectHasTableRef;
                    }
                }
                // Forbid to register Conjuncts with SelectStmt' tuple when Select is constant
                if ((queryStmt instanceof SelectStmt) && selectHasTableRef) {
                    final SelectStmt select = (SelectStmt) queryStmt;
                    // if there is an agg node, we need register the constant conjuncts on agg node's tuple
                    // this is consistent with migrateConstantConjuncts()
                    if (select.getAggInfo() != null) {
                        Map<Boolean, List<Expr>> splittedConjuncts = opConjuncts.stream()
                                .collect(Collectors.partitioningBy(expr -> expr.isConstant()));
                        op.getAnalyzer().registerConjuncts(splittedConjuncts.get(true),
                                select.getAggInfo().getOutputTupleId().asList());
                        op.getAnalyzer().registerConjuncts(splittedConjuncts.get(false),
                                select.getTableRefIds());
                    } else {
                        op.getAnalyzer().registerConjuncts(opConjuncts, select.getTableRefIds());
                    }
                } else if (queryStmt instanceof SetOperationStmt) {
                    final SetOperationStmt subSetOp = (SetOperationStmt) queryStmt;
                    op.getAnalyzer().registerConjuncts(opConjuncts, subSetOp.getTupleId().asList());
                } else {
                    if (selectHasTableRef) {
                        Preconditions.checkArgument(false);
                    }
                }
            }
            if (!hasConstantOp) {
                analyzer.markConjunctsAssigned(conjuncts);
            }
        } else {
            // mark slots referenced by the yet-unassigned conjuncts
            analyzer.materializeSlots(conjuncts);
        }
        // mark slots after predicate propagation but prior to plan tree generation
        setOperationStmt.materializeRequiredSlots(analyzer);

        PlanNode result = null;
        SetOperationStmt.Operation operation = null;
        List<SetOperationStmt.SetOperand> partialOperands = new ArrayList<>();
        // create plan for a union b intersect c except b to three fragments
        // 3:[2:[1:[a union b] intersect c] except c]
        for (SetOperationStmt.SetOperand op : setOperationStmt.getOperands()) {
            if (op.getOperation() == null) {
                partialOperands.add(op);
            } else if (operation == null && op.getOperation() != null) {
                operation = op.getOperation();
                partialOperands.add(op);
            } else if (operation != null && op.getOperation() == operation) {
                partialOperands.add(op);
            } else if (operation != null && op.getOperation() != operation) {
                if (partialOperands.size() > 0) {
                    if (operation == SetOperationStmt.Operation.INTERSECT
                            || operation == SetOperationStmt.Operation.EXCEPT) {
                        result = createSetOperationPlan(analyzer, setOperationStmt, partialOperands, result,
                                defaultOrderByLimit, sqlSelectLimit);
                    } else {
                        result = createUnionPartialSetOperationPlan(analyzer, setOperationStmt, partialOperands, result,
                                defaultOrderByLimit, sqlSelectLimit);
                    }
                    partialOperands.clear();
                }
                operation = op.getOperation();
                partialOperands.add(op);
            } else {
                throw new AnalysisException("invalid set operation statement.");
            }
        }
        if (partialOperands.size() > 0) {
            if (operation == SetOperationStmt.Operation.INTERSECT
                    || operation == SetOperationStmt.Operation.EXCEPT) {
                result = createSetOperationPlan(analyzer, setOperationStmt, partialOperands, result,
                        defaultOrderByLimit, sqlSelectLimit);
            } else {
                result = createUnionPartialSetOperationPlan(analyzer, setOperationStmt, partialOperands, result,
                        defaultOrderByLimit, sqlSelectLimit);
            }
        }

        if (setOperationStmt.hasAnalyticExprs() || hasConstantOp) {
            result = addUnassignedConjuncts(
                    analyzer, setOperationStmt.getTupleId().asList(), result);
        }
        return result;
    }

    // create the partial plan, or example: a union b intersect c
    // the first partial plan is a union b as a result d,
    // the second partial plan d intersect c
    // notice that when query is a union b the union operation is in right-hand child(b),
    // while the left-hand child(a)'s operation is null
    private PlanNode createUnionPartialSetOperationPlan(Analyzer analyzer, SetOperationStmt setOperationStmt,
                                                        List<SetOperationStmt.SetOperand> setOperands,
                                                        PlanNode result, long defaultOrderByLimit, long sqlSelectLimit)
            throws UserException {
        boolean hasDistinctOps = false;
        boolean hasAllOps = false;
        List<SetOperationStmt.SetOperand> allOps = new ArrayList<>();
        List<SetOperationStmt.SetOperand> distinctOps = new ArrayList<>();
        for (SetOperationStmt.SetOperand op : setOperands) {
            if (op.getQualifier() == SetOperationStmt.Qualifier.DISTINCT) {
                hasDistinctOps = true;
                distinctOps.add(op);
            }
            if (op.getQualifier() == SetOperationStmt.Qualifier.ALL) {
                hasAllOps = true;
                allOps.add(op);
            }
        }
        // create DISTINCT tree
        if (hasDistinctOps) {
            result = createSetOperationPlan(
                    analyzer, setOperationStmt, distinctOps, result, defaultOrderByLimit, sqlSelectLimit);
            result = new AggregationNode(ctx.getNextNodeId(), result,
                    setOperationStmt.getDistinctAggInfo());
            result.init(analyzer);
        }
        // create ALL tree
        if (hasAllOps) {
            result = createSetOperationPlan(analyzer, setOperationStmt, allOps,
                    result, defaultOrderByLimit, sqlSelectLimit);
        }
        return result;
    }

    private PlanNode createAssertRowCountNode(PlanNode input, AssertNumRowsElement assertNumRowsElement,
                                              Analyzer analyzer) throws UserException {
        AssertNumRowsNode root = new AssertNumRowsNode(ctx.getNextNodeId(), input, assertNumRowsElement);
        root.init(analyzer);
        return root;
    }

    /**
     * According to the way to materialize slots from top to bottom, Materialization will prune columns
     * which are not referenced by Statement outside. However, in some cases, in order to ensure The
     * correct execution, it is necessary to materialize the slots that are not needed by Statement
     * outside.
     *
     * @param tblRef
     * @param analyzer
     */
    private void materializeTableResultForCrossJoinOrCountStar(TableRef tblRef, Analyzer analyzer) {
        if (tblRef instanceof BaseTableRef || tblRef instanceof TableValuedFunctionRef) {
            materializeSlotForEmptyMaterializedTableRef(tblRef, analyzer);
        } else if (tblRef instanceof InlineViewRef) {
            materializeInlineViewResultExprForCrossJoinOrCountStar((InlineViewRef) tblRef, analyzer);
        } else {
            Preconditions.checkArgument(false);
        }
    }

    /**
     * When materialized table ref is a empty tbl ref, the planner should add a mini column for this tuple.
     * There are situations:
     * 1. The tbl ref is empty, such as select a from (select 'c1' a from test) t;
     * Inner tuple: tuple 0 without slot
     * 2. The materialized slot in tbl ref is empty, such as select a from (select 'c1' a, k1 from test) t;
     * Inner tuple: tuple 0 with a not materialized slot k1
     * In the above two cases, it is necessary to add a mini column to the inner tuple
     * to ensure that the number of rows in the inner query result is the number of rows in the table.
     * 2. count star: select count(*) from t;
     * <p>
     * After this function, the inner tuple is following:
     * case1. tuple 0: slot<k1> materialized true (new slot)
     * case2. tuple 0: slot<k1> materialized true (changed)
     *
     * @param tblRef
     * @param analyzer
     */
    private void materializeSlotForEmptyMaterializedTableRef(TableRef tblRef, Analyzer analyzer) {
        if (tblRef.getDesc().getMaterializedSlots().isEmpty()) {
            Column minimuColumn = null;
            for (Column col : tblRef.getTable().getBaseSchema()) {
                if (minimuColumn == null || col.getDataType().getSlotSize() < minimuColumn
                        .getDataType().getSlotSize()) {
                    minimuColumn = col;
                }
            }
            if (minimuColumn != null) {
                SlotDescriptor slot = tblRef.getDesc().getColumnSlot(minimuColumn.getName());
                if (slot != null) {
                    slot.setIsMaterialized(true);
                } else {
                    slot = analyzer.getDescTbl().addSlotDescriptor(tblRef.getDesc());
                    slot.setColumn(minimuColumn);
                    slot.setIsMaterialized(true);
                    slot.setIsNullable(minimuColumn.isAllowNull());
                }
            }
        }
    }

    /**
     * Materialize InlineViewRef result'exprs for Cross Join or Count Star
     * For example: select count(*) from (select k1+1 ,k2 ,k3 from base) tmp
     * Columns: k1 tinyint, k2 bigint, k3 double
     * Input: tmp, analyzer
     * Output:
     *   Materialized slot: k1 true, k2 false, k3 false
     *   Materialized tuple: base
     *
     * @param inlineView
     * @param analyzer
     */
    private void materializeInlineViewResultExprForCrossJoinOrCountStar(InlineViewRef inlineView, Analyzer analyzer) {
        final List<Expr> baseResultExprs = inlineView.getViewStmt().getBaseTblResultExprs();
        if (baseResultExprs.size() <= 0) {
            return;
        }
        Expr resultExprSelected = null;
        int resultExprSelectedSize = 0;
        // check whether inlineView contains materialized result expr
        for (Expr e : baseResultExprs) {
            final List<SlotId> slotIds = Lists.newArrayList();
            e.getIds(null, slotIds);
            boolean exprIsMaterialized = true;
            int exprSize = 0;
            for (SlotId id : slotIds) {
                final SlotDescriptor slot = analyzer.getDescTbl().getSlotDesc(id);
                if (!slot.isMaterialized()) {
                    exprIsMaterialized = false;
                }
                exprSize += slot.getType().getSlotSize();
            }

            // Result Expr contains materialized expr, return
            if (exprIsMaterialized) {
                return;
            }

            if (resultExprSelected == null || exprSize < resultExprSelectedSize) {
                resultExprSelectedSize = exprSize;
                resultExprSelected = e;
            }
        }

        // materialize slots which expr refer and It's total size is smallest
        final List<SlotId> slotIds = Lists.newArrayList();
        final List<TupleId> tupleIds = Lists.newArrayList();
        resultExprSelected.getIds(tupleIds, slotIds);
        for (SlotId id : slotIds) {
            final SlotDescriptor slot = analyzer.getDescTbl().getSlotDesc(id);
            slot.setIsMaterialized(true);
            slot.materializeSrcExpr();
        }
        for (TupleId id : tupleIds) {
            final TupleDescriptor tuple = analyzer.getDescTbl().getTupleDesc(id);
            tuple.setIsMaterialized(true);
        }
    }

    /**
     ------------------------------------------------------------------------------
     */
    /**
     * Push down predicates rules
     */

    /**
     * Entrance for push-down rules, it will execute possible push-down rules from top to down
     * and the planner will be responsible for assigning all predicates to PlanNode.
     */
    private void pushDownPredicates(Analyzer analyzer, SelectStmt stmt) throws AnalysisException {
        // Push down predicates according to the semantic requirements of SQL.
        pushDownPredicatesPastSort(analyzer, stmt);
        pushDownPredicatesPastWindows(analyzer, stmt);
        pushDownPredicatesPastAggregation(analyzer, stmt);
    }

    private void pushDownPredicatesPastSort(Analyzer analyzer, SelectStmt stmt) throws AnalysisException {
        // TODO chenhao, remove isEvaluateOrderBy when SubQuery's default limit is removed.
        if (stmt.evaluateOrderBy() || stmt.getLimit() >= 0 || stmt.getOffset() > 0 || stmt.getSortInfo() == null) {
            return;
        }
        final List<Expr> predicates = getBoundPredicates(analyzer, stmt.getSortInfo().getSortTupleDescriptor());
        if (predicates.size() <= 0) {
            return;
        }
        final List<Expr> pushDownPredicates = getPredicatesReplacedSlotWithSourceExpr(predicates, analyzer);
        if (pushDownPredicates.size() <= 0) {
            return;
        }

        // Push down predicates to sort's child until they are assigned successfully.
        if (putPredicatesOnWindows(stmt, analyzer, pushDownPredicates)) {
            return;
        }
        if (putPredicatesOnAggregation(stmt, analyzer, pushDownPredicates)) {
            return;
        }
        putPredicatesOnTargetTupleIds(stmt.getTableRefIds(), analyzer, predicates);
    }

    private void pushDownPredicatesPastWindows(Analyzer analyzer, SelectStmt stmt) throws AnalysisException {
        final AnalyticInfo analyticInfo = stmt.getAnalyticInfo();
        if (analyticInfo == null || analyticInfo.getCommonPartitionExprs().size() == 0) {
            return;
        }
        final List<Expr> predicates = getBoundPredicates(analyzer, analyticInfo.getOutputTupleDesc());
        if (predicates.size() <= 0) {
            return;
        }

        // Push down predicates to Windows' child until they are assigned successfully.
        final List<Expr> pushDownPredicates = getPredicatesBoundedByGroupbysSourceExpr(predicates, analyzer, stmt);
        if (pushDownPredicates.size() <= 0) {
            return;
        }
        if (putPredicatesOnAggregation(stmt, analyzer, pushDownPredicates)) {
            return;
        }
        putPredicatesOnTargetTupleIds(stmt.getTableRefIds(), analyzer, predicates);
    }

    /**
     * Push down predicates past one phase aggregation.
     *
     * @param aggregateInfo one phase aggregate info. Either first phase or second phase
     * @param analyzer current statement's analyzer
     * @param stmt current stmt
     * @param targetTupleIds target tuple to register.
     *                      Table tuple ids when process first phase agg.
     *                      First aggregate's tuple id when process second phase agg.
     * @throws AnalysisException throw exception when register predicate to tuple failed
     */
    private void pushDownPredicatesPastAggregationOnePhase(AggregateInfo aggregateInfo,
            Analyzer analyzer, SelectStmt stmt, List<TupleId> targetTupleIds) throws AnalysisException {
        if (aggregateInfo == null || aggregateInfo.getGroupingExprs().isEmpty()) {
            return;
        }
        // The output of the 1st phase agg is the 1st phase intermediate.
        // see createSecondPhaseAggInfo method
        final List<Expr> predicates = getBoundPredicates(analyzer,
                aggregateInfo.getSecondPhaseDistinctAggInfo() != null
                        ? aggregateInfo.getIntermediateTupleDesc()
                        : aggregateInfo.getOutputTupleDesc());
        if (predicates.isEmpty()) {
            return;
        }
        // Push down predicates to aggregation's child until they are assigned successfully.
        final List<Expr> pushDownPredicates = getPredicatesBoundedByGroupbysSourceExpr(predicates, analyzer, stmt);
        if (CollectionUtils.isEmpty(pushDownPredicates)) {
            return;
        }
        putPredicatesOnTargetTupleIds(targetTupleIds, analyzer, pushDownPredicates);
    }

    /**
     * Push down predicates past whole aggregate stage. Include first phase and second phase.
     *
     * @param analyzer current statement's analyzer
     * @param stmt current stmt
     * @throws AnalysisException throw exception when register predicate to tuple failed
     */
    private void pushDownPredicatesPastAggregation(Analyzer analyzer, SelectStmt stmt) throws AnalysisException {
        final AggregateInfo firstPhaseAggInfo = stmt.getAggInfo();
        if (firstPhaseAggInfo == null) {
            return;
        }
        final AggregateInfo secondPhaseAggInfo = firstPhaseAggInfo.getSecondPhaseDistinctAggInfo();

        // The output of the 1st phase agg is the 1st phase intermediate.
        // see createSecondPhaseAggInfo method
        final List<TupleId> firstPhaseTupleIds = Lists.newArrayList(
                secondPhaseAggInfo != null ? firstPhaseAggInfo.getIntermediateTupleId()
                        : firstPhaseAggInfo.getOutputTupleId());
        pushDownPredicatesPastAggregationOnePhase(secondPhaseAggInfo, analyzer, stmt, firstPhaseTupleIds);
        pushDownPredicatesPastAggregationOnePhase(firstPhaseAggInfo, analyzer, stmt, stmt.getTableRefIds());
    }

    private List<Expr> getPredicatesBoundedByGroupbysSourceExpr(
            List<Expr> predicates, Analyzer analyzer, SelectStmt stmt) {
        final List<Expr> predicatesCanPushDown = Lists.newArrayList();
        for (Expr predicate : predicates) {
            if (predicate.isConstant()) {
                // Constant predicates can't be pushed down past Groupby.
                continue;
            }

            final List<TupleId> tupleIds = Lists.newArrayList();
            final List<SlotId> slotIds = Lists.newArrayList();
            predicate.getIds(tupleIds, slotIds);

            boolean isAllSlotReferToGroupBys = true;
            for (SlotId slotId : slotIds) {
                Expr sourceExpr = new SlotRef(analyzer.getDescTbl().getSlotDesc(slotId));
                // Every phase in aggregate will wrap expression with SlotRef.
                // When we process one phase aggregate, we just need to unwrap once.
                // But when we process 2 phase aggregate, we need to unwrap twice.
                // So use loop here to adapt to different situations.
                while (sourceExpr instanceof SlotRef) {
                    SlotRef slotRef = (SlotRef) sourceExpr;
                    SlotDescriptor slotDesc = slotRef.getDesc();
                    if (slotDesc.getSourceExprs().size() != 1) {
                        break;
                    }
                    sourceExpr = slotDesc.getSourceExprs().get(0);
                }
                if (sourceExpr instanceof AnalyticExpr) {
                    isAllSlotReferToGroupBys = false;
                    break;
                }
                // if grouping set is given and column is not in all grouping set list
                // we cannot push the predicate since the column value can be null
                if (stmt.getGroupByClause() == null) {
                    //group by clause may be null when distinct grouping.
                    //eg: select distinct c from ( select distinct c from table) t where c > 1;
                    continue;
                }

                if (sourceExpr.getFn() instanceof AggregateFunction) {
                    isAllSlotReferToGroupBys = false;
                } else if (stmt.getGroupByClause().isGroupByExtension()) {
                    // if grouping type is CUBE or ROLLUP will definitely produce null
                    if (stmt.getGroupByClause().getGroupingType() == GroupByClause.GroupingType.CUBE
                            || stmt.getGroupByClause().getGroupingType() == GroupByClause.GroupingType.ROLLUP) {
                        isAllSlotReferToGroupBys = false;
                    } else {
                        // if grouping type is GROUPING_SETS and the predicate not in all grouping list,
                        // the predicate cannot be push down
                        for (List<Expr> exprs : stmt.getGroupByClause().getGroupingSetList()) {
                            if (!exprs.contains(sourceExpr)) {
                                isAllSlotReferToGroupBys = false;
                                break;
                            }
                        }
                    }
                }
                GroupByClause groupByClause = stmt.getGroupByClause();
                List<Expr> exprs = groupByClause.getGroupingExprs();
                final Expr srcExpr = sourceExpr;
                if (!exprs.contains(srcExpr) && !exprs.stream().anyMatch(expr -> expr.comeFrom(srcExpr))) {
                    // the sourceExpr doesn't come from any of the group by exprs
                    isAllSlotReferToGroupBys = false;
                    break;
                }
            }

            if (isAllSlotReferToGroupBys) {
                predicatesCanPushDown.add(predicate);
            }
        }
        return getPredicatesReplacedSlotWithSourceExpr(predicatesCanPushDown, analyzer);
    }

    private List<Expr> getPredicatesReplacedSlotWithSourceExpr(List<Expr> predicates, Analyzer analyzer) {
        final List<Expr> predicatesCanPushDown = Lists.newArrayList();
        analyzer.markConjunctsAssigned(predicates);
        for (Expr predicate : predicates) {
            final Expr newPredicate = predicate.clone();
            replacePredicateSlotRefWithSource(newPredicate, analyzer);
            predicatesCanPushDown.add(newPredicate);
        }
        return predicatesCanPushDown;
    }

    private void replacePredicateSlotRefWithSource(Expr predicate, Analyzer analyzer) {
        replacePredicateSlotRefWithSource(null, predicate, -1, analyzer);
    }

    private void replacePredicateSlotRefWithSource(Expr parent, Expr predicate, int childIndex, Analyzer analyzer) {
        if (predicate instanceof SlotRef) {
            final SlotRef slotRef = (SlotRef) predicate;
            if (parent != null && childIndex >= 0) {
                final Expr newReplacedExpr = slotRef.getDesc().getSourceExprs().get(0).clone();
                parent.setChild(childIndex, newReplacedExpr);
            }
        }

        for (int i = 0; i < predicate.getChildren().size(); i++) {
            final Expr child = predicate.getChild(i);
            replacePredicateSlotRefWithSource(predicate, child, i, analyzer);
        }
    }

    // Register predicates with Aggregation's output tuple id.
    private boolean putPredicatesOnAggregation(SelectStmt stmt, Analyzer analyzer,
                                               List<Expr> predicates) throws AnalysisException {
        final AggregateInfo aggregateInfo = stmt.getAggInfo();
        if (aggregateInfo != null) {
            analyzer.registerConjuncts(predicates, aggregateInfo.getOutputTupleId());
            return true;
        }
        return false;
    }

    // Register predicates with Windows's tuple id.
    private boolean putPredicatesOnWindows(SelectStmt stmt, Analyzer analyzer,
                                           List<Expr> predicates) throws AnalysisException {
        final AnalyticInfo analyticInfo = stmt.getAnalyticInfo();
        if (analyticInfo != null) {
            analyzer.registerConjuncts(predicates, analyticInfo.getOutputTupleId());
            return true;
        }
        return false;
    }

    /**
     * Register predicates on target tuple ids.
     *
     * @param analyzer current stmt analyzer
     * @param predicates predicates try to register
     * @param tupleIds target tupleIds
     * @throws AnalysisException throw exception when register failed
     */
    private void putPredicatesOnTargetTupleIds(List<TupleId> tupleIds,
            Analyzer analyzer, List<Expr> predicates)
            throws AnalysisException {
        if (CollectionUtils.isEmpty(tupleIds)) {
            return;
        }
        for (Expr predicate : predicates) {
            Preconditions.checkArgument(predicate.isBoundByTupleIds(tupleIds),
                    "Predicate:" + predicate.toSql() + " can't be assigned to some PlanNode.");
            final List<TupleId> predicateTupleIds = Lists.newArrayList();
            predicate.getIds(predicateTupleIds, null);
            analyzer.registerConjunct(predicate, predicateTupleIds);
        }
    }

    /**
     * ------------------------------------------------------------------------------
     */

    private List<Expr> getBoundPredicates(Analyzer analyzer, TupleDescriptor tupleDesc) {
        final List<TupleId> tupleIds = Lists.newArrayList();
        if (tupleDesc != null) {
            tupleIds.add(tupleDesc.getId());
        }
        return analyzer.getUnassignedConjuncts(tupleIds);
    }

    /**
     * Returns a normalized version of a binary equality predicate 'expr' where the lhs
     * child expr is bound by some tuple in 'lhsTids' and the rhs child expr is bound by
     * some tuple in 'rhsTids'. Returns 'expr' if this predicate is already normalized.
     * Returns null in any of the following cases:
     * 1. It is not an equality predicate
     * 2. One of the operands is a constant
     * 3. Both children of this predicate are the same expr
     * The so-called normalization is to ensure that the above conditions are met, and then
     * to ensure that the order of expr is consistent with the order of node
     */
    public static BinaryPredicate getNormalizedEqPred(Expr expr, List<TupleId> lhsTids,
                                                      List<TupleId> rhsTids, Analyzer analyzer) {
        if (!(expr instanceof BinaryPredicate)) {
            return null;
        }
        BinaryPredicate pred = (BinaryPredicate) expr;
        if (!pred.getOp().isEquivalence()) {
            return null;
        }
        if (pred.getChild(0).isConstant() || pred.getChild(1).isConstant()) {
            return null;
        }

        // Use the child that contains lhsTids as lhsExpr, for example, A join B on B.k = A.k,
        // where lhsExpr=A.k, rhsExpr=B.k, changed the order, A.k = B.k
        Expr lhsExpr = Expr.getFirstBoundChild(pred, lhsTids);
        Expr rhsExpr = Expr.getFirstBoundChild(pred, rhsTids);
        if (lhsExpr == null || rhsExpr == null || lhsExpr == rhsExpr) {
            return null;
        }

        BinaryPredicate result = new BinaryPredicate(pred.getOp(), lhsExpr, rhsExpr);
        result.analyzeNoThrow(analyzer);
        return result;
    }
}

