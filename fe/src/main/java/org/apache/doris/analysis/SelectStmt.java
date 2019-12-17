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

package org.apache.doris.analysis;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ColumnAliasGenerator;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.TableAliasGenerator;
import org.apache.doris.common.TreeNode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.ExprRewriter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Representation of a single select block, including GROUP BY, ORDER BY and HAVING
 * clauses.
 */
public class SelectStmt extends QueryStmt {
    private final static Logger LOG = LogManager.getLogger(SelectStmt.class);
    
    // ///////////////////////////////////////
    // BEGIN: Members that need to be reset()

    protected SelectList selectList;
    private final ArrayList<String> colLabels; // lower case column labels
    protected final FromClause fromClause_;
    protected ArrayList<Expr>   groupingExprs;
    private List<Expr> originalExpr;
    //
    private Expr havingClause;  // original having clause
    protected Expr whereClause;
    // havingClause with aliases and agg output resolved
    private Expr havingPred;

    // set if we have any kind of aggregation operation, include SELECT DISTINCT
    private AggregateInfo aggInfo;
    // set if we have analytic function
    private AnalyticInfo analyticInfo;
    // substitutes all exprs in this select block to reference base tables
    // directly
    private ExprSubstitutionMap baseTblSmap = new ExprSubstitutionMap();

    private ValueList valueList;

    // END: Members that need to be reset()
    // ///////////////////////////////////////

    // SQL string of this SelectStmt before inline-view expression substitution.
    // Set in analyze().
    protected String sqlString_;

    public SelectStmt(ValueList valueList, ArrayList<OrderByElement> orderByElement, LimitElement limitElement) {
        super(orderByElement, limitElement);
        this.valueList = valueList;
        this.selectList = new SelectList();
        this.fromClause_ = new FromClause();
        this.colLabels = Lists.newArrayList();
    }

    SelectStmt(
            SelectList selectList,
            FromClause fromClause,
            Expr wherePredicate,
            ArrayList<Expr> groupingExprs,
            Expr havingPredicate,
            ArrayList<OrderByElement> orderByElements,
            LimitElement limitElement) {
        super(orderByElements, limitElement);
        this.selectList = selectList;
        if (fromClause == null) {
            fromClause_ = new FromClause();
        } else {
            fromClause_ = fromClause;
        }
        this.whereClause = wherePredicate;
        this.groupingExprs = groupingExprs;
        this.havingClause = havingPredicate;

        this.colLabels = Lists.newArrayList();
        this.havingPred = null;
        this.aggInfo = null;
        this.sortInfo = null;
    }

    protected SelectStmt(SelectStmt other) {
        super(other);
        selectList = other.selectList.clone();
        fromClause_ = other.fromClause_.clone();
        whereClause = (other.whereClause != null) ? other.whereClause.clone() : null;
        groupingExprs = (other.groupingExprs != null) ? Expr.cloneAndResetList(other.groupingExprs) : null;
        havingClause = (other.havingClause != null) ? other.havingClause.clone() : null;

        colLabels = Lists.newArrayList(other.colLabels);
        aggInfo = (other.aggInfo != null) ? other.aggInfo.clone() : null;
        analyticInfo = (other.analyticInfo != null) ? other.analyticInfo.clone() : null;
        sqlString_ = (other.sqlString_ != null) ? new String(other.sqlString_) : null;
        baseTblSmap = other.baseTblSmap.clone();
    }

    @Override
    public void reset() {
        super.reset();
        selectList.reset();
        colLabels.clear();
        fromClause_.reset();
        if (whereClause != null) whereClause.reset();
        if (groupingExprs != null) Expr.resetList(groupingExprs);
        if (havingClause != null) havingClause.reset();
        havingPred = null;
        aggInfo = null;
        analyticInfo = null;
        baseTblSmap.clear();
    }
    
    @Override
    public QueryStmt clone() {
        return new SelectStmt(this);
    }

    /**
     * @return the original select list items from the query
     */
    public SelectList getSelectList() {
        return selectList;
    }

    public ValueList getValueList() {
        return valueList;
    }

    /**
     * @return the HAVING clause post-analysis and with aliases resolved
     */
    public Expr getHavingPred() {
        return havingPred;
    }

    public List<TableRef> getTableRefs() {
        return fromClause_.getTableRefs();
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    public ArrayList<Expr> getGroupingExprs() {
        return groupingExprs;
    }

    public void setWhereClause(Expr whereClause) {
        this.whereClause = whereClause;
    }
    public AggregateInfo getAggInfo() {
        return aggInfo;
    }

    public AnalyticInfo getAnalyticInfo() {
        return analyticInfo;
    }
    public boolean hasAnalyticInfo() {
        return analyticInfo != null;
    }
    public boolean hasHavingClause() { return havingClause != null; }

    @Override
    public SortInfo getSortInfo() {
        return sortInfo;
    }

    @Override
    public ArrayList<String> getColLabels() {
        return colLabels;
    }

    public ExprSubstitutionMap getBaseTblSmap() {
        return baseTblSmap;
    }

    @Override
    public void getDbs(Analyzer analyzer, Map<String, Database> dbs) throws AnalysisException {
        getWithClauseDbs(analyzer, dbs);
        for (TableRef tblRef : fromClause_) {
            if (tblRef instanceof InlineViewRef) {
                // Inline view reference
                QueryStmt inlineStmt = ((InlineViewRef) tblRef).getViewStmt();
                inlineStmt.getDbs(analyzer, dbs);
            } else {
                String dbName = tblRef.getName().getDb();
                if (Strings.isNullOrEmpty(dbName)) {
                    dbName = analyzer.getDefaultDb();
                } else {
                    dbName = ClusterNamespace.getFullName(analyzer.getClusterName(), tblRef.getName().getDb());
                }
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
                }

                Database db = analyzer.getCatalog().getDb(dbName);
                if (db == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
                }

                // check auth
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                                                                        tblRef.getName().getTbl(),
                                                                        PrivPredicate.SELECT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                                                        ConnectContext.get().getQualifiedUser(),
                                                        ConnectContext.get().getRemoteIP(),
                                                        tblRef.getName().getTbl());
                }

                dbs.put(dbName, db);
            }
        }
    }

    // Column alias generator used during query rewriting.
    private ColumnAliasGenerator columnAliasGenerator = null;

    public ColumnAliasGenerator getColumnAliasGenerator() {
        if (columnAliasGenerator == null) {
            columnAliasGenerator = new ColumnAliasGenerator(colLabels, null);
        }
        return columnAliasGenerator;
    }

    // Table alias generator used during query rewriting.
    private TableAliasGenerator tableAliasGenerator = null;

    public TableAliasGenerator getTableAliasGenerator() {
        if (tableAliasGenerator == null) {
            tableAliasGenerator = new TableAliasGenerator(analyzer, null);
        }
        return tableAliasGenerator;
    }

    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (isAnalyzed()) return;
        super.analyze(analyzer);

        fromClause_.setNeedToSql(needToSql);
        fromClause_.analyze(analyzer);

        // Generate !empty() predicates to filter out empty collections.
        // Skip this step when analyzing a WITH-clause because CollectionTableRefs
        // do not register collection slots in their parent in that context
        // (see CollectionTableRef.analyze()).
        if (!analyzer.isWithClause()) registerIsNotEmptyPredicates(analyzer);

        // populate selectListExprs, aliasSMap, and colNames
        for (SelectListItem item : selectList.getItems()) {
            if (item.isStar()) {
                TableName tblName = item.getTblName();
                if (tblName == null) {
                    expandStar(analyzer);
                } else {
                    expandStar(analyzer, tblName);
                }
            } else {
                // Analyze the resultExpr before generating a label to ensure enforcement
                // of expr child and depth limits (toColumn() label may call toSql()).
                item.getExpr().analyze(analyzer);
                if (item.getExpr().contains(Predicates.instanceOf(Subquery.class))) {
                    throw new AnalysisException(
                            "Subqueries are not supported in the select list.");
                }

                resultExprs.add(item.getExpr());
                SlotRef aliasRef = new SlotRef(null, item.toColumnLabel());
                Expr existingAliasExpr = aliasSMap.get(aliasRef);
                if (existingAliasExpr != null && !existingAliasExpr.equals(item.getExpr())) {
                    // If we have already seen this alias, it refers to more than one column and
                    // therefore is ambiguous.
                    ambiguousAliasList.add(aliasRef);
                }
                aliasSMap.put(aliasRef, item.getExpr().clone());
                colLabels.add(item.toColumnLabel());
            }
        }
        if (valueList != null) {
            if (!fromInsert) {
                valueList.analyzeForSelect(analyzer);
            }
            for (Expr expr : valueList.getFirstRow()) {
                if (expr instanceof DefaultValueExpr) {
                    resultExprs.add(new IntLiteral(1));
                } else {
                    resultExprs.add(expr);
                }
                colLabels.add(expr.toColumnLabel());
            }
        }
        // analyze valueList if exists
        if (needToSql) {
            originalExpr = Expr.cloneList(resultExprs);
        }

        // analyze selectListExprs
        Expr.analyze(resultExprs, analyzer);
        if (TreeNode.contains(resultExprs, AnalyticExpr.class)) {
            if (fromClause_.isEmpty()) {
                throw new AnalysisException("Analytic expressions require FROM clause.");
            }

            // do this here, not after analyzeAggregation(), otherwise the AnalyticExprs
            // will get substituted away
            if (selectList.isDistinct()) {
                throw new AnalysisException(
                        "cannot combine SELECT DISTINCT with analytic functions");
            }
        }

        if (whereClause != null) {
            whereClauseRewrite();
            whereClause.analyze(analyzer);
            if (whereClause.containsAggregate()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_INVALID_GROUP_FUNC_USE);
            }

            whereClause.checkReturnsBool("WHERE clause", false);
            Expr e = whereClause.findFirstOf(AnalyticExpr.class);
            if (e != null) {
                throw new AnalysisException(
                        "WHERE clause must not contain analytic expressions: " + e.toSql());
            }
            analyzer.registerConjuncts(whereClause, false, getTableRefIds());
        }

        createSortInfo(analyzer);
        analyzeAggregation(analyzer);
        createAnalyticInfo(analyzer);
        if (evaluateOrderBy) {
            createSortTupleInfo(analyzer);
        }

        if (needToSql) {
            sqlString_ = toSql();
        }
        reorderTable(analyzer);

        resolveInlineViewRefs(analyzer);

        if (analyzer.hasEmptySpjResultSet() && aggInfo == null) {
            analyzer.setHasEmptyResultSet();
        }

        if (aggInfo != null) {
            if (LOG.isDebugEnabled()) LOG.debug("post-analysis " + aggInfo.debugString());
        }
    }

    public List<TupleId> getTableRefIds() {
        List<TupleId> result = Lists.newArrayList();

        for (TableRef ref: fromClause_) {
            result.add(ref.getId());
        }

        return result;
    }

    private void whereClauseRewrite() {
        if (whereClause instanceof IntLiteral) {
            if (((IntLiteral)whereClause).getLongValue() == 0) {
                whereClause = new BoolLiteral(false);
            } else {
                whereClause = new BoolLiteral(true);
            }
        }
    }

    /**
     * Generates and registers !empty() predicates to filter out empty collections directly
     * in the parent scan of collection table refs. This is a performance optimization to
     * avoid the expensive processing of empty collections inside a subplan that would
     * yield an empty result set.
     *
     * For correctness purposes, the predicates are generated in cases where we can ensure
     * that they will be assigned only to the parent scan, and no other plan node.
     *
     * The conditions are as follows:
     * - collection table ref is relative and non-correlated
     * - collection table ref represents the rhs of an inner/cross/semi join
     * - collection table ref's parent tuple is not outer joined
     *
     * TODO: In some cases, it is possible to generate !empty() predicates for a correlated
     * table ref, but in general, that is not correct for non-trivial query blocks.
     * For example, if the block with the correlated ref has an aggregation then adding a
     * !empty() predicate would incorrectly discard rows from the final result set.
     * TODO: Evaluating !empty() predicates at non-scan nodes interacts poorly with our BE
     * projection of collection slots. For example, rows could incorrectly be filtered if
     * a !empty() predicate is assigned to a plan node that comes after the unnest of the
     * collection that also performs the projection.
     */
    private void registerIsNotEmptyPredicates(Analyzer analyzer) throws AnalysisException {
        /*
        for (TableRef tblRef: fromClause_.getTableRefs()) {
            Preconditions.checkState(tblRef.isResolved());
            if (!(tblRef instanceof CollectionTableRef)) continue;
            CollectionTableRef ref = (CollectionTableRef) tblRef;
            // Skip non-relative and correlated refs.
            if (!ref.isRelative() || ref.isCorrelated()) continue;
            // Skip outer and anti joins.
            if (ref.getJoinOp().isOuterJoin() || ref.getJoinOp().isAntiJoin()) continue;
            // Do not generate a predicate if the parent tuple is outer joined.
            if (analyzer.isOuterJoined(ref.getResolvedPath().getRootDesc().getId())) continue;
            IsNotEmptyPredicate isNotEmptyPred =
                    new IsNotEmptyPredicate(ref.getCollectionExpr().clone());
            isNotEmptyPred.analyze(analyzer);
            // Register the predicate as an On-clause conjunct because it should only
            // affect the result of this join and not the whole FROM clause.
            analyzer.registerOnClauseConjuncts(
                    Lists.<Expr>newArrayList(isNotEmptyPred), ref);
        }
        */
    }

    /**
     * Marks all unassigned join predicates as well as exprs in aggInfo and sortInfo.
     */
    public void materializeRequiredSlots(Analyzer analyzer) throws AnalysisException {
        // Mark unassigned join predicates. Some predicates that must be evaluated by a join
        // can also be safely evaluated below the join (picked up by getBoundPredicates()).
        // Such predicates will be marked twice and that is ok.
        List<Expr> unassigned =
            analyzer.getUnassignedConjuncts(getTableRefIds(), true);
        List<Expr> unassignedJoinConjuncts = Lists.newArrayList();
        for (Expr e: unassigned) {
            if (analyzer.evalAfterJoin(e)) {
                unassignedJoinConjuncts.add(e);
            }
        }
        List<Expr> baseTblJoinConjuncts =
            Expr.trySubstituteList(unassignedJoinConjuncts, baseTblSmap, analyzer, false);
        analyzer.materializeSlots(baseTblJoinConjuncts);

        if (evaluateOrderBy) {
            // mark ordering exprs before marking agg/analytic exprs because they could contain
            // agg/analytic exprs that are not referenced anywhere but the ORDER BY clause
            sortInfo.materializeRequiredSlots(analyzer, baseTblSmap);
        }

        if (hasAnalyticInfo()) {
            // Mark analytic exprs before marking agg exprs because they could contain agg
            // exprs that are not referenced anywhere but the analytic expr.
            // Gather unassigned predicates and mark their slots. It is not desirable
            // to account for propagated predicates because if an analytic expr is only
            // referenced by a propagated predicate, then it's better to not materialize the
            // analytic expr at all.
            ArrayList<TupleId> tids = Lists.newArrayList();
            getMaterializedTupleIds(tids); // includes the analytic tuple
            List<Expr> conjuncts = analyzer.getUnassignedConjuncts(tids);
            analyzer.materializeSlots(conjuncts);
            analyticInfo.materializeRequiredSlots(analyzer, baseTblSmap);
        }

        if (aggInfo != null) {
            // mark all agg exprs needed for HAVING pred and binding predicates as materialized
            // before calling AggregateInfo.materializeRequiredSlots(), otherwise they won't
            // show up in AggregateInfo.getMaterializedAggregateExprs()
            ArrayList<Expr> havingConjuncts = Lists.newArrayList();
            if (havingPred != null) havingConjuncts.add(havingPred);
            // Ignore predicates bound to a group-by slot because those
            // are already evaluated below this agg node (e.g., in a scan).
            Set<SlotId> groupBySlots = Sets.newHashSet();
            for (int i = 0; i < aggInfo.getGroupingExprs().size(); ++i) {
                groupBySlots.add(aggInfo.getOutputTupleDesc().getSlots().get(i).getId());
            }
            // Binding predicates are assigned to the final output tuple of the aggregation,
            // which is the tuple of the 2nd phase agg for distinct aggs.
            // TODO(zc):
            // ArrayList<Expr> bindingPredicates =
            //         analyzer.getBoundPredicates(aggInfo.getResultTupleId(), groupBySlots, false);
            // havingConjuncts.addAll(bindingPredicates);
            havingConjuncts.addAll(
                    analyzer.getUnassignedConjuncts(aggInfo.getResultTupleId().asList()));
            materializeSlots(analyzer, havingConjuncts);
            aggInfo.materializeRequiredSlots(analyzer, baseTblSmap);
        }
    }

    protected void reorderTable(Analyzer analyzer) throws AnalysisException {
        List<Pair<TableRef, Long>> candidates = Lists.newArrayList();

        // New pair of table ref and row count
        for (TableRef tblRef : fromClause_) {
            if (tblRef.getJoinOp() != JoinOperator.INNER_JOIN) {
                // Unsupported reorder outer join
                return;
            }
            long rowCount = 0;
            if (tblRef.getTable().getType() == TableType.OLAP) {
                rowCount = ((OlapTable) (tblRef.getTable())).getRowCount();
                LOG.debug("tableName={} rowCount={}", tblRef.getAlias(), rowCount);
            }
            candidates.add(new Pair(tblRef, rowCount));
        }
        // give InlineView row count
        long last = 0;
        for (int i = candidates.size() - 1; i >= 0; --i) {
            Pair<TableRef, Long> candidate = candidates.get(i);
            if (candidate.first instanceof InlineViewRef) {
                candidate.second = last;
            }
            last = candidate.second + 1;
        }

        // order oldRefList by row count
        Collections.sort(candidates, new Comparator<Pair<TableRef, Long>>() {
            public int compare(Pair<TableRef, Long> a, Pair<TableRef, Long> b) {
                long diff = b.second - a.second;
                return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
            }
        });

        boolean isAllEqualJoin = false;
        for (Pair<TableRef, Long> candidate : candidates) {
            if (reorderTable(analyzer, candidate.first)) {
                isAllEqualJoin = true;
                break;
            }
        }
        if (isAllEqualJoin) {
            return;
        }
        // can not get AST only with equal join, MayBe cross join can help
        fromClause_.clear();
        for (Pair<TableRef, Long> candidate : candidates) {
            fromClause_.add(candidate.first);
        }
    }

    // reorder select table
    protected boolean reorderTable(Analyzer analyzer, TableRef firstRef)
            throws AnalysisException {
        List<TableRef> tmpRefList = Lists.newArrayList();
        Map<TupleId, TableRef> tableRefMap = Maps.newHashMap();

        // set Map and push list
        for (TableRef tblRef : fromClause_) {
            tableRefMap.put(tblRef.getId(), tblRef);
            tmpRefList.add(tblRef);
        }
        // clear tableRefList
        fromClause_.clear();
        // mark first table
        fromClause_.add(firstRef);
        tableRefMap.remove(firstRef.getId());


        // reserve TupleId has been added successfully
        Set<TupleId> validTupleId = Sets.newHashSet();
        validTupleId.add(firstRef.getId());
        // find table
        int i = 0;
        while (i < fromClause_.size()) {
            TableRef tblRef = fromClause_.get(i);
            // get all equal
            List<Expr> eqJoinPredicates = analyzer.getEqJoinConjuncts(tblRef.getId());
            List<TupleId> tuple_list = Lists.newArrayList();
            Expr.getIds(eqJoinPredicates, tuple_list, null);
            for (TupleId tid : tuple_list) {
                TableRef candidateTableRef = tableRefMap.get(tid);
                if (candidateTableRef != null) {
                    
                    // When sorting table according to the rows, you must ensure 
                    // that all tables On-conjuncts referenced has been added or
                    // is being added.
                    List<Expr> candidateEqJoinPredicates = analyzer.getEqJoinConjunctsExcludeAuxPredicates(
                            candidateTableRef.getId());
                    List<TupleId> candidateTupleList = Lists.newArrayList();
                    Expr.getIds(candidateEqJoinPredicates, candidateTupleList, null);
                    int count = candidateTupleList.size();
                    for (TupleId tupleId : candidateTupleList) {
                        if (validTupleId.contains(tupleId) || candidateTableRef.getId() == tupleId) {
                            count--;
                        }
                    }
                    
                    if (count == 0) {
                        fromClause_.add(candidateTableRef);
                        validTupleId.add(candidateTableRef.getId());
                        tableRefMap.remove(tid);
                    }
                }
            }
            i++;
        }
        // find path failed.
        if (0 != tableRefMap.size()) {
            fromClause_.clear();
            fromClause_.addAll(tmpRefList);
            return false;
        }
        return true;
    }

    /**
     * Populates baseTblSmap_ with our combined inline view smap and creates
     * baseTblResultExprs.
     */
    protected void resolveInlineViewRefs(Analyzer analyzer) throws AnalysisException {
        // Gather the inline view substitution maps from the enclosed inline views
        for (TableRef tblRef : fromClause_) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
                baseTblSmap = ExprSubstitutionMap.combine(baseTblSmap, inlineViewRef.getBaseTblSmap());
            }
        }

        baseTblResultExprs = Expr.trySubstituteList(resultExprs, baseTblSmap, analyzer, false);
        if (LOG.isDebugEnabled()) {
            LOG.debug("baseTblSmap_: " + baseTblSmap.debugString());
            LOG.debug("resultExprs: " + Expr.debugString(resultExprs));
            LOG.debug("baseTblResultExprs: " + Expr.debugString(baseTblResultExprs));
        }
    }

    /**
     * This select block might contain inline views.
     * Substitute all exprs (result of the analysis) of this select block referencing any
     * of our inlined views, including everything registered with the analyzer.
     * Expressions created during parsing (such as whereClause) are not touched.
     * @throws AnalysisException
     */
      public void seondSubstituteInlineViewExprs(ExprSubstitutionMap sMap) throws AnalysisException {
          // we might not have anything to substitute
          if (sMap.size() == 0) {
              return;
          }

          // select
          // Expr.substituteList(resultExprs, sMap);

          // aggregation (group by and aggregation expr)
          if (aggInfo != null) {
              aggInfo.substitute(sMap, analyzer);
          }

          // having
          if (havingPred != null) {
              havingPred.substitute(sMap);
          }

          // ordering
          //if (sortInfo != null) {
              // sortInfo.substitute(sMap);
          //}
      }

    /**
     * Expand "*" select list item.
     */
    private void expandStar(Analyzer analyzer) throws AnalysisException {
        if (fromClause_.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        // expand in From clause order
        for (TableRef tableRef : fromClause_) {
            if (analyzer.isSemiJoined(tableRef.getId())) {
                continue;
            }
            expandStar(new TableName(null,tableRef.getAlias()), tableRef.getDesc());
        }
    }

    /**
     * Expand "<tbl>.*" select list item.
     */
    private void expandStar(Analyzer analyzer, TableName tblName) throws AnalysisException {
        Collection<TupleDescriptor> descs = analyzer.getDescriptor(tblName);
        if (descs == null || descs.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tblName.getTbl());
        }
        for (TupleDescriptor desc : descs) {
            expandStar(tblName, desc);
        }
    }

    /**
     * Expand "*" for a particular tuple descriptor by appending
     * refs for each column to selectListExprs.
     */
    private void expandStar(TableName tblName, TupleDescriptor desc) throws AnalysisException {
        for (Column col : desc.getTable().getBaseSchema()) {
            if (col.getDataType() == PrimitiveType.HLL && !fromInsert) {
                throw new AnalysisException (
                        "hll only use in HLL_UNION_AGG or HLL_CARDINALITY , HLL_HASH and so on.");
            }
            if (col.getAggregationType() == AggregateType.BITMAP_UNION && !fromInsert) {
                throw new AnalysisException (
                        "BITMAP_UNION agg column only use in TO_BITMAP or BITMAP_UNION , BITMAP_COUNT and so on.");
            }
            resultExprs.add(new SlotRef(tblName, col.getName()));
            colLabels.add(col.getName());
        }
    }

    /**
     * Analyze aggregation-relevant components of the select block (Group By clause,
     * select list, Order By clause),
     * Create the AggregationInfo, including the agg output tuple, and transform all post-agg exprs
     * given AggregationInfo's smap.
     */
    private void analyzeAggregation(Analyzer analyzer) throws AnalysisException {
        // check having clause
        if (havingClause != null) {
            if (havingClause.contains(Predicates.instanceOf(Subquery.class))) {
                throw new AnalysisException(
                        "Subqueries are not supported in the HAVING clause.");
            }
            Expr ambiguousAlias = getFirstAmbiguousAlias(havingClause);
            if (ambiguousAlias != null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NON_UNIQ_ERROR, ambiguousAlias.toColumnLabel());
            }
            // substitute aliases in place (ordinals not allowed in having clause)
            havingPred = havingClause.substitute(aliasSMap, analyzer, false);
            havingPred.checkReturnsBool("HAVING clause", true);
            // can't contain analytic exprs
            Expr analyticExpr = havingPred.findFirstOf(AnalyticExpr.class);
            if (analyticExpr != null) {
                throw new AnalysisException(
                        "HAVING clause must not contain analytic expressions: "
                                + analyticExpr.toSql());
            }
        }

        if (groupingExprs == null && !selectList.isDistinct()
                && !TreeNode.contains(resultExprs, Expr.isAggregatePredicate())
                && (havingPred == null || !havingPred.contains(Expr.isAggregatePredicate()))
                && (sortInfo == null || !TreeNode.contains(sortInfo.getOrderingExprs(),
                Expr.isAggregatePredicate()))) {
            // We're not computing aggregates but we still need to register the HAVING
            // clause which could, e.g., contain a constant expression evaluating to false.
            if (havingPred != null) analyzer.registerConjuncts(havingPred, true);
            return;
        }

        // If we're computing an aggregate, we must have a FROM clause.
        if (fromClause_.size() == 0) {
            throw new AnalysisException("Aggregation without a FROM clause is not allowed");
        }

        if (selectList.isDistinct() && groupingExprs == null) {
            List<Expr> aggregateExpr = Lists.newArrayList();
            TreeNode.collect(resultExprs, Expr.isAggregatePredicate(), aggregateExpr);
            if (aggregateExpr.size() == resultExprs.size()) {
                selectList.setIsDistinct(false);
            }
        }

        if (selectList.isDistinct()
                && (groupingExprs != null
                    || TreeNode.contains(resultExprs, Expr.isAggregatePredicate())
                    || (havingPred != null && havingPred.contains(Expr.isAggregatePredicate())))) {
            throw new AnalysisException("cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
        }

        // disallow '*' and explicit GROUP BY (we can't group by '*', and if you need to
        // name all star-expanded cols in the group by clause you might as well do it
        // in the select list)
        if (groupingExprs != null ||
                TreeNode.contains(resultExprs, Expr.isAggregatePredicate())) {
            for (SelectListItem item : selectList.getItems()) {
                if (item.isStar()) {
                    throw new AnalysisException(
                      "cannot combine '*' in select list with GROUP BY: " + item.toSql());
                }
            }
        }

        // disallow subqueries in the GROUP BY clause
        if (groupingExprs != null) {
            for (Expr expr: groupingExprs) {
                if (expr.contains(Predicates.instanceOf(Subquery.class))) {
                    throw new AnalysisException(
                            "Subqueries are not supported in the GROUP BY clause.");
                }
            }
        }

        // analyze grouping exprs
        ArrayList<Expr> groupingExprsCopy = Lists.newArrayList();
        if (groupingExprs != null) {
            // make a deep copy here, we don't want to modify the original
            // exprs during analysis (in case we need to print them later)
            groupingExprsCopy = Expr.cloneList(groupingExprs);

            substituteOrdinalsAliases(groupingExprsCopy, "GROUP BY", analyzer);

            for (int i = 0; i < groupingExprsCopy.size(); ++i) {
                groupingExprsCopy.get(i).analyze(analyzer);
                if (groupingExprsCopy.get(i).contains(Expr.isAggregatePredicate())) {
                    // reference the original expr in the error msg
                    throw new AnalysisException(
                            "GROUP BY expression must not contain aggregate functions: "
                                    + groupingExprs.get(i).toSql());
                }
                if (groupingExprsCopy.get(i).contains(AnalyticExpr.class)) {
                    // reference the original expr in the error msg
                    throw new AnalysisException(
                            "GROUP BY expression must not contain analytic expressions: "
                                    + groupingExprsCopy.get(i).toSql());
                }

                if (groupingExprsCopy.get(i).type.isHllType()) {
                    throw new AnalysisException(
                            "GROUP BY expression must not contain hll column: "
                            + groupingExprsCopy.get(i).toSql());
                }

                if (groupingExprsCopy.get(i).type.isBitmapType()) {
                    throw new AnalysisException(
                            "GROUP BY expression must not contain bitmap column: "
                                    + groupingExprsCopy.get(i).toSql());
                }
            }
        }

        // Collect the aggregate expressions from the SELECT, HAVING and ORDER BY clauses
        // of this statement.
        ArrayList<FunctionCallExpr> aggExprs = Lists.newArrayList();
        TreeNode.collect(resultExprs, Expr.isAggregatePredicate(), aggExprs);
        if (havingPred != null) {
            havingPred.collect(Expr.isAggregatePredicate(), aggExprs);
        }
        if (sortInfo != null) {
            // TODO: Avoid evaluating aggs in ignored order-bys
            TreeNode.collect(sortInfo.getOrderingExprs(), Expr.isAggregatePredicate(), aggExprs);
        }

        // When DISTINCT aggregates are present, non-distinct (i.e. ALL) aggregates are
        // evaluated in two phases (see AggregateInfo for more details). In particular,
        // COUNT(c) in "SELECT COUNT(c), AGG(DISTINCT d) from R" is transformed to
        // "SELECT SUM(cnt) FROM (SELECT COUNT(c) as cnt from R group by d ) S".
        // Since a group-by expression is added to the inner query it returns no rows if
        // R is empty, in which case the SUM of COUNTs will return NULL.
        // However the original COUNT(c) should have returned 0 instead of NULL in this case.
        // Therefore, COUNT([ALL]) is transformed into zeroifnull(COUNT([ALL]) if
        // i) There is no GROUP-BY clause, and
        // ii) Other DISTINCT aggregates are present.
        ExprSubstitutionMap countAllMap = createCountAllMap(aggExprs, analyzer);
        final ExprSubstitutionMap multiCountOrSumDistinctMap = 
                createSumOrCountMultiDistinctSMap(aggExprs, analyzer);
        countAllMap = ExprSubstitutionMap.compose(multiCountOrSumDistinctMap, countAllMap, analyzer);
        List<Expr> substitutedAggs =
                Expr.substituteList(aggExprs, countAllMap, analyzer, false);
        aggExprs.clear();
        TreeNode.collect(substitutedAggs, Expr.isAggregatePredicate(), aggExprs);
        createAggInfo(groupingExprsCopy, aggExprs, analyzer);

        // combine avg smap with the one that produces the final agg output
        AggregateInfo finalAggInfo =
                aggInfo.getSecondPhaseDistinctAggInfo() != null
                        ? aggInfo.getSecondPhaseDistinctAggInfo()
                        : aggInfo;

        ExprSubstitutionMap combinedSmap =
                ExprSubstitutionMap.compose(countAllMap, finalAggInfo.getOutputSmap(), analyzer);

        // change select list, having and ordering exprs to point to agg output. We need
        // to reanalyze the exprs at this point.
        if (LOG.isDebugEnabled()) {
            LOG.debug("combined smap: " + combinedSmap.debugString());
            LOG.debug("desctbl: " + analyzer.getDescTbl().debugString());
            LOG.debug("resultexprs: " + Expr.debugString(resultExprs));
        }
        resultExprs = Expr.substituteList(resultExprs, combinedSmap, analyzer, false);
        if (LOG.isDebugEnabled()) {
            LOG.debug("post-agg selectListExprs: " + Expr.debugString(resultExprs));
        }
        if (havingPred != null) {
            // Make sure the predicate in the HAVING clause does not contain a
            // subquery.
            Preconditions.checkState(!havingPred.contains(
                    Predicates.instanceOf(Subquery.class)));
            havingPred = havingPred.substitute(combinedSmap, analyzer, false);
            analyzer.registerConjuncts(havingPred, true, finalAggInfo.getOutputTupleId().asList());
            if (LOG.isDebugEnabled()) {
                LOG.debug("post-agg havingPred: " + havingPred.debugString());
            }
        }

        if (sortInfo != null) {
            sortInfo.substituteOrderingExprs(combinedSmap, analyzer);
            if (LOG.isDebugEnabled()) {
                LOG.debug("post-agg orderingExprs: " +
                        Expr.debugString(sortInfo.getOrderingExprs()));
            }
        }

        // check that all post-agg exprs point to agg output
        for (int i = 0; i < selectList.getItems().size(); ++i) {
            if (!resultExprs.get(i).isBound(finalAggInfo.getOutputTupleId())) {
                throw new AnalysisException(
                  "select list expression not produced by aggregation output " + "(missing from " +
                    "GROUP BY clause?): " + selectList.getItems().get(
                    i).getExpr().toSql());
            }
        }
        if (orderByElements != null) {
            for (int i = 0; i < orderByElements.size(); ++i) {
                if (!sortInfo.getOrderingExprs().get(i).isBound(finalAggInfo.getOutputTupleId())) {
                    throw new AnalysisException(
                      "ORDER BY expression not produced by aggregation output " + "(missing from " +
                        "GROUP BY clause?): " + orderByElements.get(
                        i).getExpr().toSql());
                }

                if (sortInfo.getOrderingExprs().get(i).type.isHllType()) {
                    throw new AnalysisException(
                        "ORDER BY expression could not contain hll column.");
                }

                if (sortInfo.getOrderingExprs().get(i).type.isBitmapType()) {
                    throw new AnalysisException(
                            "ORDER BY expression could not contain bitmap column.");
                }
            }
        }
        if (havingPred != null) {
            if (!havingPred.isBound(finalAggInfo.getOutputTupleId())) {
                throw new AnalysisException(
                  "HAVING clause not produced by aggregation output " + "(missing from GROUP BY " +
                    "clause?): " + havingClause.toSql());
            }
        }
    }


    /**
     * Build smap count_distinct->multi_count_distinct sum_distinct->multi_count_distinct
     * assumes that select list and having clause have been analyzed.
     */
    private ExprSubstitutionMap createSumOrCountMultiDistinctSMap(
            ArrayList<FunctionCallExpr> aggExprs, Analyzer analyzer) throws AnalysisException {
        final List<FunctionCallExpr> distinctExprs = Lists.newArrayList();
        for (FunctionCallExpr aggExpr : aggExprs) {
             if (aggExpr.isDistinct()) {
                 distinctExprs.add(aggExpr);
             }
        }
        final ExprSubstitutionMap result = new ExprSubstitutionMap();
        final boolean hasMultiDistinct = AggregateInfo.estimateIfContainsMultiDistinct(distinctExprs);
        if (!hasMultiDistinct) {
            return result;
        }
        for (FunctionCallExpr inputExpr : distinctExprs) {
            Expr replaceExpr = null;
            final String functionName = inputExpr.getFnName().getFunction();
            if (functionName.equalsIgnoreCase("COUNT")) {
                final List<Expr> countInputExpr = Lists.newArrayList(inputExpr.getChild(0).clone(null));
                replaceExpr = new FunctionCallExpr("MULTI_DISTINCT_COUNT",
                        new FunctionParams(inputExpr.isDistinct(), countInputExpr));
            } else if (functionName.equalsIgnoreCase("SUM")) {
                final List<Expr> sumInputExprs = Lists.newArrayList(inputExpr.getChild(0).clone(null));
                replaceExpr = new FunctionCallExpr("MULTI_DISTINCT_SUM",
                        new FunctionParams(inputExpr.isDistinct(), sumInputExprs));
            } else if (functionName.equalsIgnoreCase("AVG")) {
                final List<Expr> sumInputExprs = Lists.newArrayList(inputExpr.getChild(0).clone(null));
                final List<Expr> countInputExpr = Lists.newArrayList(inputExpr.getChild(0).clone(null));
                final FunctionCallExpr sumExpr = new FunctionCallExpr("MULTI_DISTINCT_SUM",
                  new FunctionParams(inputExpr.isDistinct(), sumInputExprs));
                final FunctionCallExpr countExpr = new FunctionCallExpr("MULTI_DISTINCT_COUNT",
                    new FunctionParams(inputExpr.isDistinct(), countInputExpr));
                replaceExpr = new ArithmeticExpr(ArithmeticExpr.Operator.DIVIDE, sumExpr, countExpr);
            } else {
                throw new AnalysisException(inputExpr.getFnName() + " can't support multi distinct.");
            }

            replaceExpr.analyze(analyzer);
            result.put(inputExpr, replaceExpr);
        }
        if (LOG.isDebugEnabled()) LOG.debug("multi distinct smap: {}", result.debugString());
        return result;
    }

    /**
     * Create a map from COUNT([ALL]) -> zeroifnull(COUNT([ALL])) if
     * i) There is no GROUP-BY, and
     * ii) There are other distinct aggregates to be evaluated.
     * This transformation is necessary for COUNT to correctly return 0 for empty
     * input relations.
     */
    private ExprSubstitutionMap createCountAllMap(
            List<FunctionCallExpr> aggExprs, Analyzer analyzer)
            throws AnalysisException {
        ExprSubstitutionMap scalarCountAllMap = new ExprSubstitutionMap();

        if (groupingExprs != null && !groupingExprs.isEmpty()) {
            // There are grouping expressions, so no substitution needs to be done.
            return scalarCountAllMap;
        }

        com.google.common.base.Predicate<FunctionCallExpr> isNotDistinctPred =
                new com.google.common.base.Predicate<FunctionCallExpr>() {
                    public boolean apply(FunctionCallExpr expr) {
                        return !expr.isDistinct();
                    }
                };
        if (Iterables.all(aggExprs, isNotDistinctPred)) {
            // Only [ALL] aggs, so no substitution needs to be done.
            return scalarCountAllMap;
        }

        com.google.common.base.Predicate<FunctionCallExpr> isCountPred =
                new com.google.common.base.Predicate<FunctionCallExpr>() {
                    public boolean apply(FunctionCallExpr expr) {
                        return expr.getFnName().getFunction().equals("count");
                    }
                };

        Iterable<FunctionCallExpr> countAllAggs =
                Iterables.filter(aggExprs, Predicates.and(isCountPred, isNotDistinctPred));
        for (FunctionCallExpr countAllAgg: countAllAggs) {
            // TODO(zc)
            // Replace COUNT(ALL) with zeroifnull(COUNT(ALL))
            ArrayList<Expr> zeroIfNullParam = Lists.newArrayList(countAllAgg.clone(), new IntLiteral(0, Type.BIGINT));
            FunctionCallExpr zeroIfNull =
                    new FunctionCallExpr("ifnull", zeroIfNullParam);
            zeroIfNull.analyze(analyzer);
            scalarCountAllMap.put(countAllAgg, zeroIfNull);
        }

        return scalarCountAllMap;
    }

    /**
     * Create aggInfo for the given grouping and agg exprs.
     */
    private void createAggInfo(
            ArrayList<Expr> groupingExprs,
            ArrayList<FunctionCallExpr> aggExprs,
            Analyzer analyzer)
            throws AnalysisException {
        if (selectList.isDistinct()) {
            // Create aggInfo for SELECT DISTINCT ... stmt:
            // - all select list items turn into grouping exprs
            // - there are no aggregate exprs
            Preconditions.checkState(groupingExprs.isEmpty());
            Preconditions.checkState(aggExprs.isEmpty());
            aggInfo = AggregateInfo.create(Expr.cloneList(resultExprs), null, null, analyzer);
        } else {
            aggInfo = AggregateInfo.create(groupingExprs, aggExprs, null, analyzer);
        }
    }

    /**
     * If the select list contains AnalyticExprs, create AnalyticInfo and substitute
     * AnalyticExprs using the AnalyticInfo's smap.
     */
    private void createAnalyticInfo(Analyzer analyzer) throws AnalysisException {
        // collect AnalyticExprs from the SELECT and ORDER BY clauses
        ArrayList<Expr> analyticExprs = Lists.newArrayList();
        TreeNode.collect(resultExprs, AnalyticExpr.class, analyticExprs);
        if (sortInfo != null) {
            TreeNode.collect(sortInfo.getOrderingExprs(), AnalyticExpr.class, analyticExprs);
        }
        if (analyticExprs.isEmpty()) {
            return;
        }
        ExprSubstitutionMap rewriteSmap = new ExprSubstitutionMap();
        for (Expr expr: analyticExprs) {
            AnalyticExpr toRewrite = (AnalyticExpr)expr;
            Expr newExpr = AnalyticExpr.rewrite(toRewrite);
            if (newExpr != null) {
                newExpr.analyze(analyzer);
                if (!rewriteSmap.containsMappingFor(toRewrite)) {
                    rewriteSmap.put(toRewrite, newExpr);
                }
            }
        }
        if (rewriteSmap.size() > 0) {
            // Substitute the exprs with their rewritten versions.
            ArrayList<Expr> updatedAnalyticExprs =
                    Expr.substituteList(analyticExprs, rewriteSmap, analyzer, false);
            // This is to get rid the original exprs which have been rewritten.
            analyticExprs.clear();
            // Collect the new exprs introduced through the rewrite and the non-rewrite exprs.
            TreeNode.collect(updatedAnalyticExprs, AnalyticExpr.class, analyticExprs);
        }

        analyticInfo = AnalyticInfo.create(analyticExprs, analyzer);

        ExprSubstitutionMap smap = analyticInfo.getSmap();
        // If 'exprRewritten' is true, we have to compose the new smap with the existing one.
        if (rewriteSmap.size() > 0) {
            smap = ExprSubstitutionMap.compose(
                    rewriteSmap, analyticInfo.getSmap(), analyzer);
        }
        // change select list and ordering exprs to point to analytic output. We need
        // to reanalyze the exprs at this point.
        resultExprs = Expr.substituteList(resultExprs, smap, analyzer, false);
        if (LOG.isDebugEnabled()) {
            LOG.debug("post-analytic selectListExprs: " + Expr.debugString(resultExprs));
        }
        if (sortInfo != null) {
            sortInfo.substituteOrderingExprs(smap, analyzer);
            if (LOG.isDebugEnabled()) {
                LOG.debug("post-analytic orderingExprs: " +
                        Expr.debugString(sortInfo.getOrderingExprs()));
            }
        }
    }

    @Override
    public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
        Preconditions.checkState(isAnalyzed());
        selectList.rewriteExprs(rewriter, analyzer);
        for (TableRef ref: fromClause_) ref.rewriteExprs(rewriter, analyzer);
        if (whereClause != null) {
            whereClause = rewriter.rewrite(whereClause, analyzer);
            // Also rewrite exprs in the statements of subqueries.
            List<Subquery> subqueryExprs = Lists.newArrayList();
            whereClause.collect(Subquery.class, subqueryExprs);
            for (Subquery s: subqueryExprs) s.getStatement().rewriteExprs(rewriter);
        }
        if (havingClause != null) {
            havingClause = rewriter.rewrite(havingClause, analyzer);
        }
        if (groupingExprs != null) rewriter.rewriteList(groupingExprs, analyzer);
        if (orderByElements != null) {
            for (OrderByElement orderByElem: orderByElements) {
                orderByElem.setExpr(rewriter.rewrite(orderByElem.getExpr(), analyzer));
            }
        }
    }

    @Override
    public String toSql() {
        if (sqlString_ != null) {
            return sqlString_;
        }
        StringBuilder strBuilder = new StringBuilder();
        // Select list
        strBuilder.append("SELECT ");
        if (selectList.isDistinct()) {
            strBuilder.append("DISTINCT ");
        }
        for (int i = 0; i < resultExprs.size(); ++i) {
            // strBuilder.append(selectList.getItems().get(i).toSql());
            // strBuilder.append((i + 1 != selectList.getItems().size()) ? ", " : "");
            if (i != 0) {
                strBuilder.append(", ");
            }
            if (needToSql) {
                strBuilder.append(originalExpr.get(i).toSql());
            } else {
                strBuilder.append(resultExprs.get(i).toSql());
            }
            strBuilder.append(" AS ").append(SqlUtils.getIdentSql(colLabels.get(i)));
        }

        // From clause
        if (!fromClause_.isEmpty()) { strBuilder.append(fromClause_.toSql()); }

        // Where clause
        if (whereClause != null) {
            strBuilder.append(" WHERE ");
            strBuilder.append(whereClause.toSql());
        }
        // Group By clause
        if (groupingExprs != null) {
            strBuilder.append(" GROUP BY ");
            for (int i = 0; i < groupingExprs.size(); ++i) {
                strBuilder.append(groupingExprs.get(i).toSql());
                strBuilder.append((i + 1 != groupingExprs.size()) ? ", " : "");
            }
        }
        // Having clause
        if (havingClause != null) {
            strBuilder.append(" HAVING ");
            strBuilder.append(havingClause.toSql());
        }
        // Order By clause
        if (orderByElements != null) {
            strBuilder.append(" ORDER BY ");
            for (int i = 0; i < orderByElements.size(); ++i) {
                strBuilder.append(orderByElements.get(i).getExpr().toSql());
                if (sortInfo != null) {
                    strBuilder.append((sortInfo.getIsAscOrder().get(i)) ? " ASC" : " DESC");
                }
                strBuilder.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }
        // Limit clause.
        if (hasLimitClause()) {
            strBuilder.append(limitElement.toSql());
        }
        return strBuilder.toString();
    }

    @Override
    public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
        // If select statement has an aggregate, then the aggregate tuple id is materialized.
        // Otherwise, all referenced tables are materialized.
        if (evaluateOrderBy) {
            tupleIdList.add(sortInfo.getSortTupleDescriptor().getId());
        } else if (aggInfo != null) {
            // Return the tuple id produced in the final aggregation step.
            if (aggInfo.isDistinctAgg()) {
                tupleIdList.add(aggInfo.getSecondPhaseDistinctAggInfo().getOutputTupleId());
            } else {
                tupleIdList.add(aggInfo.getOutputTupleId());
            }
        } else {
            for (TableRef tblRef : fromClause_) {
                tupleIdList.addAll(tblRef.getMaterializedTupleIds());
            }
        }
        // We materialize the agg tuple or the table refs together with the analytic tuple.
        if (hasAnalyticInfo() && isEvaluateOrderBy()) {
            tupleIdList.add(analyticInfo.getOutputTupleId());
        }
    }

    @Override
    public void substituteSelectList(Analyzer analyzer, List<String> newColLabels)
            throws AnalysisException, UserException {
        // start out with table refs to establish aliases
        TableRef leftTblRef = null;  // the one to the left of tblRef
        for (int i = 0; i < fromClause_.size(); ++i) {
            // Resolve and replace non-InlineViewRef table refs with a BaseTableRef or ViewRef.
            TableRef tblRef = fromClause_.get(i);
            tblRef = analyzer.resolveTableRef(tblRef);
            Preconditions.checkNotNull(tblRef);
            fromClause_.set(i, tblRef);
            tblRef.setLeftTblRef(leftTblRef);
            tblRef.analyze(analyzer);
            leftTblRef = tblRef;
        }
        // populate selectListExprs, aliasSMap, and colNames
        for (SelectListItem item : selectList.getItems()) {
            if (item.isStar()) {
                TableName tblName = item.getTblName();
                if (tblName == null) {
                    expandStar(analyzer);
                } else {
                    expandStar(analyzer, tblName);
                }
            } else {
                // to make sure the sortinfo's AnalyticExpr and resultExprs's AnalyticExpr analytic once
                if(item.getExpr() instanceof AnalyticExpr){
                    item.getExpr().analyze(analyzer);
                }
                if (item.getAlias() != null) {
                    SlotRef aliasRef = new SlotRef(null, item.getAlias());
                    SlotRef newAliasRef = new SlotRef(null, newColLabels.get(resultExprs.size()));
                    newAliasRef.analysisDone();
                    aliasSMap.put(aliasRef, newAliasRef);
                }
                resultExprs.add(item.getExpr());
            }
        }
        // substitute group by
        if (groupingExprs != null) {
            substituteOrdinalsAliases(groupingExprs, "GROUP BY", analyzer);
        }
        // substitute having
        if (havingClause != null) {
            havingClause = havingClause.clone(aliasSMap);
        }
        // substitute order by
        if (orderByElements != null) {
            for (int i = 0; i < orderByElements.size(); ++i) {
                orderByElements = OrderByElement.substitute(orderByElements, aliasSMap, analyzer);
            }
        }

        colLabels.clear();
        colLabels.addAll(newColLabels);
    }

    public boolean hasWhereClause() { return whereClause != null; }
    public boolean hasAggInfo() { return aggInfo != null; }
    public boolean hasGroupByClause() { return groupingExprs != null; }
    /**
     * Check if the stmt returns a single row. This can happen
     * in the following cases:
     * 1. select stmt with a 'limit 1' clause
     * 2. select stmt with an aggregate function and no group by.
     * 3. select stmt with no from clause.
     *
     * This function may produce false negatives because the cardinality of the
     * result set also depends on the data a stmt is processing.
     */
    public boolean returnsSingleRow() {
        // limit 1 clause
        if (hasLimitClause() && getLimit() == 1) { return true; }
        // No from clause (base tables or inline views)
        if (fromClause_.isEmpty()) { return true; }
        // Aggregation with no group by and no DISTINCT
        if (hasAggInfo() && !hasGroupByClause() && !selectList.isDistinct()) { return true; }
        // In all other cases, return false.
        return false;
    }

    @Override
    public void collectTableRefs(List<TableRef> tblRefs) {
        for (TableRef tblRef: fromClause_) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
                inlineViewRef.getViewStmt().collectTableRefs(tblRefs);
            } else {
                tblRefs.add(tblRef);
            }
        }
    }

}
