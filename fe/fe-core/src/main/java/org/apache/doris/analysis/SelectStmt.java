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

import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.View;
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

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Representation of a single select block, including GROUP BY, ORDER BY and HAVING
 * clauses.
 */
public class SelectStmt extends QueryStmt {
    private final static Logger LOG = LogManager.getLogger(SelectStmt.class);
    private UUID id = UUID.randomUUID();

    // ///////////////////////////////////////
    // BEGIN: Members that need to be reset()

    protected SelectList selectList;
    private final ArrayList<String> colLabels; // lower case column labels
    protected final FromClause fromClause_;
    protected GroupByClause groupByClause;
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

    // if we have grouping extensions like cube or rollup or grouping sets
    private GroupingInfo groupingInfo;

    // having clause which has been analyzed
    // For example: select k1, sum(k2) a from t group by k1 having a>1;
    // this parameter: sum(t.k2) > 1
    private Expr havingClauseAfterAnaylzed;

    // END: Members that need to be reset()
    // ///////////////////////////////////////

    // SQL string of this SelectStmt before inline-view expression substitution.
    // Set in analyze().
    protected String sqlString_;

    // Table alias generator used during query rewriting.
    private TableAliasGenerator tableAliasGenerator = null;

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
            GroupByClause groupByClause,
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
        this.groupByClause = groupByClause;
        this.havingClause = havingPredicate;

        this.colLabels = Lists.newArrayList();
        this.havingPred = null;
        this.aggInfo = null;
        this.sortInfo = null;
        this.groupingInfo = null;
    }

    protected SelectStmt(SelectStmt other) {
        super(other);
        this.id = other.id;
        selectList = other.selectList.clone();
        fromClause_ = other.fromClause_.clone();
        whereClause = (other.whereClause != null) ? other.whereClause.clone() : null;
        groupByClause = (other.groupByClause != null) ? other.groupByClause.clone() : null;
        havingClause = (other.havingClause != null) ? other.havingClause.clone() : null;

        colLabels = Lists.newArrayList(other.colLabels);
        aggInfo = (other.aggInfo != null) ? other.aggInfo.clone() : null;
        analyticInfo = (other.analyticInfo != null) ? other.analyticInfo.clone() : null;
        sqlString_ = (other.sqlString_ != null) ? other.sqlString_ : null;
        baseTblSmap = other.baseTblSmap.clone();
        groupingInfo = null;
    }

    @Override
    public void reset() {
        super.reset();
        selectList.reset();
        colLabels.clear();
        fromClause_.reset();
        if (whereClause != null) {
            whereClause.reset();
        }
        if (groupByClause != null) {
            groupByClause.reset();
        }
        if (havingClause != null) {
            havingClause.reset();
        }
        havingClauseAfterAnaylzed = null;
        havingPred = null;
        aggInfo = null;
        analyticInfo = null;
        baseTblSmap.clear();
        groupingInfo = null;
    }

    @Override
    public QueryStmt clone() {
        return new SelectStmt(this);
    }

    public UUID getId() {
        return id;
    }

    /**
     * @return the original select list items from the query
     */
    public SelectList getSelectList() {
        return selectList;
    }

    public void setSelectList(SelectList selectList) {
        this.selectList = selectList;
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

    public Expr getHavingClauseAfterAnaylzed() {
        return havingClauseAfterAnaylzed;
    }

    public List<TableRef> getTableRefs() {
        return fromClause_.getTableRefs();
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(Expr whereClause) {
        this.whereClause = whereClause;
    }

    public AggregateInfo getAggInfo() {
        return aggInfo;
    }

    public GroupingInfo getGroupingInfo() {
        return groupingInfo;
    }

    public GroupByClause getGroupByClause() {
        return groupByClause;
    }

    public AnalyticInfo getAnalyticInfo() {
        return analyticInfo;
    }

    public boolean hasAnalyticInfo() {
        return analyticInfo != null;
    }

    public boolean hasHavingClause() {
        return havingClause != null;
    }

    public void removeHavingClause() {
        havingClause = null;
    }

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
    public void getTables(Analyzer analyzer, Map<Long, Table> tableMap, Set<String> parentViewNameSet) throws AnalysisException {
        getWithClauseTables(analyzer, tableMap, parentViewNameSet);
        for (TableRef tblRef : fromClause_) {
            if (tblRef instanceof InlineViewRef) {
                // Inline view reference
                QueryStmt inlineStmt = ((InlineViewRef) tblRef).getViewStmt();
                inlineStmt.getTables(analyzer, tableMap, parentViewNameSet);
            } else {
                String dbName = tblRef.getName().getDb();
                String tableName = tblRef.getName().getTbl();
                if (Strings.isNullOrEmpty(dbName)) {
                    dbName = analyzer.getDefaultDb();
                } else {
                    dbName = ClusterNamespace.getFullName(analyzer.getClusterName(), tblRef.getName().getDb());
                }
                if (isViewTableRef(tblRef.getName().toString(), parentViewNameSet)) {
                    continue;
                }
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
                }
                if (Strings.isNullOrEmpty(tableName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR);
                }
                Database db = analyzer.getCatalog().getDb(dbName);
                if (db == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
                }
                Table table = db.getTable(tableName);
                if (table == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                }

                // check auth
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                        tableName,
                        PrivPredicate.SELECT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                            ConnectContext.get().getQualifiedUser(),
                            ConnectContext.get().getRemoteIP(),
                            tblRef.getName().getTbl());
                }
                tableMap.put(table.getId(), table);
            }
        }
    }

    // if tableName in parentViewNameSetor tableName in withClause views
    // means this tableref is inlineview, no need check dbname again
    private boolean isViewTableRef(String tblName, Set<String> parentViewNameSet) {
        if (parentViewNameSet.contains(tblName)) {
            return true;
        }

        if (withClause_ != null) {
            List<View> views = withClause_.getViews();
            for (View view : views) {
                if (view.getName().equals(tblName)) {
                    return true;
                }
            }
        }

        return false;
    }

    // Column alias generator used during query rewriting.
    private ColumnAliasGenerator columnAliasGenerator = null;

    public ColumnAliasGenerator getColumnAliasGenerator() {
        if (columnAliasGenerator == null) {
            columnAliasGenerator = new ColumnAliasGenerator(colLabels, null);
        }
        return columnAliasGenerator;
    }

    public TableAliasGenerator getTableAliasGenerator() {
        if (tableAliasGenerator == null) {
            tableAliasGenerator = new TableAliasGenerator(analyzer, null);
        }
        return tableAliasGenerator;
    }

    public void setTableAliasGenerator(TableAliasGenerator tableAliasGenerator) {
        this.tableAliasGenerator = tableAliasGenerator;
    }

    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (isAnalyzed()) {
            return;
        }
        super.analyze(analyzer);
        fromClause_.setNeedToSql(needToSql);
        fromClause_.analyze(analyzer);

        // Generate !empty() predicates to filter out empty collections.
        // Skip this step when analyzing a WITH-clause because CollectionTableRefs
        // do not register collection slots in their parent in that context
        // (see CollectionTableRef.analyze()).
        if (!analyzer.isWithClause()) {
            registerIsNotEmptyPredicates(analyzer);
        }
        // populate selectListExprs, aliasSMap, groupingSmap and colNames
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
                if (!(item.getExpr() instanceof CaseExpr) &&
                        item.getExpr().contains(Predicates.instanceOf(Subquery.class))) {
                    throw new AnalysisException("Subquery is not supported in the select list.");
                }
                Expr expr = rewriteQueryExprByMvColumnExpr(item.getExpr(), analyzer);
                resultExprs.add(expr);
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
        if (groupByClause != null && groupByClause.isGroupByExtension()) {
            for (SelectListItem item : selectList.getItems()) {
                if (item.getExpr() instanceof FunctionCallExpr && item.getExpr().fn instanceof AggregateFunction) {
                    for (Expr expr: groupByClause.getGroupingExprs()) {
                        if (item.getExpr().contains(expr)) {
                            throw new AnalysisException("column: " + expr.toSql() + " cannot both in select list and "
                                    + "aggregate functions when using GROUPING SETS/CUBE/ROLLUP, please use union"
                                    + " instead.");
                        }
                    }
                }
            }
            groupingInfo = new GroupingInfo(analyzer, groupByClause.getGroupingType());
            groupingInfo.substituteGroupingFn(resultExprs, analyzer);
        } else {
            for (Expr expr : resultExprs) {
                if (checkGroupingFn(expr)) {
                    throw new AnalysisException(
                            "cannot use GROUPING functions without [grouping sets|rollup|cube] "
                                    + "clause or grouping sets only have one element.");
                }
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
        // do this before whereClause.analyze , some expr is not analyzed, this may cause some
        // function not work as expected such as equals;
        whereClauseRewrite();
        if (whereClause != null) {
            if (checkGroupingFn(whereClause)) {
                throw new AnalysisException("grouping operations are not allowed in WHERE.");
            }
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
        if (sortInfo != null && CollectionUtils.isNotEmpty(sortInfo.getOrderingExprs())) {
            if (groupingInfo != null) {
                groupingInfo.substituteGroupingFn(sortInfo.getOrderingExprs(), analyzer);
            }
        }
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("post-analysis " + aggInfo.debugString());
            }
        }
    }

    public List<TupleId> getTableRefIds() {
        List<TupleId> result = Lists.newArrayList();

        for (TableRef ref : fromClause_) {
            result.add(ref.getId());
        }

        return result;
    }

    public List<TupleId> getTableRefIdsWithoutInlineView() {
        List<TupleId> result = Lists.newArrayList();

        for (TableRef ref : fromClause_) {
            if (ref instanceof InlineViewRef) {
                continue;
            }
            result.add(ref.getId());
        }

        return result;
    }

    @Override
    public List<TupleId> collectTupleIds() {
        List<TupleId> result = Lists.newArrayList();
        resultExprs.stream().forEach(expr -> expr.getIds(result, null));
        result.addAll(getTableRefIds());
        if (whereClause != null) {
            whereClause.getIds(result, null);
        }
        if (havingClauseAfterAnaylzed != null) {
            havingClauseAfterAnaylzed.getIds(result, null);
        }
        return result;
    }

    private void whereClauseRewrite() {
        if (whereClause instanceof IntLiteral) {
            if (((IntLiteral) whereClause).getLongValue() == 0) {
                whereClause = new BoolLiteral(false);
            } else {
                whereClause = new BoolLiteral(true);
            }
        }
        Expr deDuplicatedWhere = deduplicateOrs(whereClause);
        if (deDuplicatedWhere != null) {
            whereClause = deDuplicatedWhere;
        }
    }

    /**
     * this function only process (a and b and c) or (d and e and f) like clause,
     * this function will extract this to [[a, b, c], [d, e, f]]
     */
    private List<List<Expr>> extractDuplicateOrs(CompoundPredicate expr) {
        List<List<Expr>> orExprs = new ArrayList<>();
        for (Expr child : expr.getChildren()) {
            if (child instanceof CompoundPredicate) {
                CompoundPredicate childCp = (CompoundPredicate) child;
                if (childCp.getOp() == CompoundPredicate.Operator.OR) {
                    orExprs.addAll(extractDuplicateOrs(childCp));
                    continue;
                } else if (childCp.getOp() == CompoundPredicate.Operator.AND) {
                    orExprs.add(flatAndExpr(child));
                    continue;
                }
            }
            orExprs.add(Arrays.asList(child));
        }
        return orExprs;
    }

    /**
     * This function attempts to apply the inverse OR distributive law:
     * ((A AND B) OR (A AND C))  =>  (A AND (B OR C))
     * That is, locate OR clauses in which every subclause contains an
     * identical term, and pull out the duplicated terms.
     */
    private Expr deduplicateOrs(Expr expr) {
        if (expr == null) {
            return null;
        } else if (expr instanceof CompoundPredicate && ((CompoundPredicate) expr).getOp() == CompoundPredicate.Operator.OR) {
            Expr rewritedExpr = processDuplicateOrs(extractDuplicateOrs((CompoundPredicate) expr));
            if (rewritedExpr != null) {
                return rewritedExpr;
            }
        } else {
            for (int i = 0; i < expr.getChildren().size(); i++) {
                Expr rewritedExpr = deduplicateOrs(expr.getChild(i));
                if (rewritedExpr != null) {
                    expr.setChild(i, rewritedExpr);
                }
            }
        }
        return expr;
    }

    /**
     * try to flat and , a and b and c => [a, b, c]
     */
    private List<Expr> flatAndExpr(Expr expr) {
        List<Expr> andExprs = new ArrayList<>();
        if (expr instanceof CompoundPredicate && ((CompoundPredicate) expr).getOp() == CompoundPredicate.Operator.AND) {
            andExprs.addAll(flatAndExpr(expr.getChild(0)));
            andExprs.addAll(flatAndExpr(expr.getChild(1)));
        } else {
            andExprs.add(expr);
        }
        return andExprs;
    }

    /**
     * the input is a list of list, the inner list is and connected exprs, the outer list is or connected
     * for example clause (a and b and c) or (a and e and f) after extractDuplicateOrs will be [[a, b, c], [a, e, f]]
     * this is the input of this function, first step is deduplicate [[a, b, c], [a, e, f]] => [[a], [b, c], [e, f]]
     * then rebuild the expr to a and ((b and c) or (e and f))
     */
    private Expr processDuplicateOrs(List<List<Expr>> exprs) {
        if (exprs.size() < 2) {
            return null;
        }
        // 1. remove duplicated elements [[a,a], [a, b], [a,b]] => [[a], [a,b]]
        Set<Set<Expr>> set = new LinkedHashSet<>();
        for (List<Expr> ex : exprs) {
            Set<Expr> es = new LinkedHashSet<>();
            es.addAll(ex);
            set.add(es);
        }
        List<List<Expr>> clearExprs = new ArrayList<>();
        for (Set<Expr> es : set) {
            List<Expr> el = new ArrayList<>();
            el.addAll(es);
            clearExprs.add(el);
        }
        if (clearExprs.size() == 1) {
            return makeCompound(clearExprs.get(0), CompoundPredicate.Operator.AND);
        }
        // 2. find duplicate cross the clause
        List<Expr> cloneExprs = new ArrayList<>(clearExprs.get(0));
        for (int i = 1; i < clearExprs.size(); ++i) {
            cloneExprs.retainAll(clearExprs.get(i));
        }
        List<Expr> temp = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(cloneExprs)) {
            temp.add(makeCompound(cloneExprs, CompoundPredicate.Operator.AND));
        }

        Expr result;
        boolean isReturnCommonFactorExpr = false;
        for (List<Expr> exprList : clearExprs) {
            exprList.removeAll(cloneExprs);
            if (exprList.size() == 0) {
                // For example, the sql is "where (a = 1) or (a = 1 and B = 2)"
                // if "(a = 1)" is extracted as a common factor expression, then the first expression "(a = 1)" has no expression
                // other than a common factor expression, and the second expression "(a = 1 and B = 2)" has an expression of "(B = 2)"
                //
                // In this case, the common factor expression ("a = 1") can be directly used to replace the whole CompoundOrPredicate.
                // In Fact, the common factor expression is actually the parent set of expression "(a = 1)" and expression "(a = 1 and B = 2)"
                //
                // exprList.size() == 0 means one child of CompoundOrPredicate has no expression other than a common factor expression.
                isReturnCommonFactorExpr = true;
                break;
            }
            temp.add(makeCompound(exprList, CompoundPredicate.Operator.AND));
        }
        if (isReturnCommonFactorExpr) {
            result = temp.get(0);
        } else {
            // rebuild CompoundPredicate if found duplicate predicate will build （predicate) and (.. or ..)  predicate in
            // step 1: will build (.. or ..)
            result = CollectionUtils.isNotEmpty(cloneExprs) ? new CompoundPredicate(CompoundPredicate.Operator.AND,
                    temp.get(0), makeCompound(temp.subList(1, temp.size()), CompoundPredicate.Operator.OR))
                    : makeCompound(temp, CompoundPredicate.Operator.OR);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("equal ors: " + result.toSql());
        }
        return result;
    }

    /**
     * Rebuild CompoundPredicate, [a, e, f] AND => a and e and f
     */
    private Expr makeCompound(List<Expr> exprs, CompoundPredicate.Operator op) {
        if (CollectionUtils.isEmpty(exprs)) {
            return null;
        }
        if (exprs.size() == 1) {
            return exprs.get(0);
        }
        CompoundPredicate result = new CompoundPredicate(op, exprs.get(0), exprs.get(1));
        for (int i = 2; i < exprs.size(); ++i) {
            result = new CompoundPredicate(op, result.clone(), exprs.get(i));
        }
        return result;
    }

    /**
     * Generates and registers !empty() predicates to filter out empty collections directly
     * in the parent scan of collection table refs. This is a performance optimization to
     * avoid the expensive processing of empty collections inside a subplan that would
     * yield an empty result set.
     * <p>
     * For correctness purposes, the predicates are generated in cases where we can ensure
     * that they will be assigned only to the parent scan, and no other plan node.
     * <p>
     * The conditions are as follows:
     * - collection table ref is relative and non-correlated
     * - collection table ref represents the rhs of an inner/cross/semi join
     * - collection table ref's parent tuple is not outer joined
     * <p>
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
        for (Expr e : unassigned) {
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
            if (havingPred != null) {
                havingConjuncts.add(havingPred);
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
            if (tblRef.getJoinOp() != JoinOperator.INNER_JOIN || tblRef.hasJoinHints()) {
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
        Collections.sort(candidates, (a, b) -> b.second.compareTo(a.second));

        for (Pair<TableRef, Long> candidate : candidates) {
            if (reorderTable(analyzer, candidate.first)) {
                // as long as one scheme success, we return this scheme immediately.
                // in this scheme, candidate.first will be consider to be the big table in star schema.
                // this scheme might not be fit for snowflake schema.
                return;
            }
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
                if (validTupleId.contains(tid)) {
                    // tid has allreday in the list of validTupleId, ignore it
                    continue;
                }
                TableRef candidateTableRef = tableRefMap.get(tid);
                if (candidateTableRef != null) {

                    // When sorting table according to the rows, you must ensure
                    // that all tables On-conjuncts referenced has been added or
                    // is being added.
                    Preconditions.checkState(tid == candidateTableRef.getId());
                    List<Expr> candidateEqJoinPredicates = analyzer.getEqJoinConjunctsExcludeAuxPredicates(tid);
                    List<TupleId> candidateTupleList = Lists.newArrayList();
                    Expr.getIds(candidateEqJoinPredicates, candidateTupleList, null);
                    int count = candidateTupleList.size();
                    for (TupleId tupleId : candidateTupleList) {
                        if (validTupleId.contains(tupleId) || tid == tupleId) {
                            count--;
                        }
                    }

                    if (count == 0) {
                        fromClause_.add(candidateTableRef);
                        validTupleId.add(tid);
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
            expandStar(new TableName(null, tableRef.getAlias()), tableRef.getDesc());
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
    private void expandStar(TableName tblName, TupleDescriptor desc) {
        for (Column col : desc.getTable().getBaseSchema()) {
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
            Expr ambiguousAlias = getFirstAmbiguousAlias(havingClause);
            if (ambiguousAlias != null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NON_UNIQ_ERROR, ambiguousAlias.toColumnLabel());
            }
            /*
             * The having clause need to be substitute by aliasSMap.
             * And it is analyzed after substitute.
             * For example:
             * Query: select k1 a, sum(k2) b from table group by k1 having a > 1;
             * Having clause: a > 1
             * aliasSMap: <a, table.k1> <b, sum(table.k2)>
             * After substitute: a > 1 changed to table.k1 > 1
             * Analyzer: check column and other subquery in having clause
             * having predicate: table.k1 > 1
             */
            /*
             * TODO(ml): support substitute outer column in correlated subquery
             * For example: select k1 key, sum(k1) sum_k1 from table a group by k1
             *              having k1 >
             *                     (select min(k1) from table b where a.key=b.k2);
             * TODO: the a.key should be replaced by a.k1 instead of unknown column 'key' in 'a'
             */
            havingClauseAfterAnaylzed = havingClause.substitute(aliasSMap, analyzer, false);
            havingClauseAfterAnaylzed = rewriteQueryExprByMvColumnExpr(havingClauseAfterAnaylzed, analyzer);
            havingClauseAfterAnaylzed.checkReturnsBool("HAVING clause", true);
            // can't contain analytic exprs
            Expr analyticExpr = havingClauseAfterAnaylzed.findFirstOf(AnalyticExpr.class);
            if (analyticExpr != null) {
                throw new AnalysisException(
                        "HAVING clause must not contain analytic expressions: "
                                + analyticExpr.toSql());
            }
        }

        if (groupByClause == null && !selectList.isDistinct()
                && !TreeNode.contains(resultExprs, Expr.isAggregatePredicate())
                && (havingClauseAfterAnaylzed == null || !havingClauseAfterAnaylzed.contains(Expr.isAggregatePredicate()))
                && (sortInfo == null || !TreeNode.contains(sortInfo.getOrderingExprs(),
                Expr.isAggregatePredicate()))) {
            // We're not computing aggregates but we still need to register the HAVING
            // clause which could, e.g., contain a constant expression evaluating to false.
            if (havingClauseAfterAnaylzed != null) {
                if (havingClauseAfterAnaylzed.contains(Subquery.class)) {
                    throw new AnalysisException("Only constant expr could be supported in having clause "
                            + "when no aggregation in stmt");
                }
                analyzer.registerConjuncts(havingClauseAfterAnaylzed, true);
            }
            return;
        }

        // If we're computing an aggregate, we must have a FROM clause.
        if (fromClause_.size() == 0) {
            throw new AnalysisException("Aggregation without a FROM clause is not allowed");
        }

        if (selectList.isDistinct() && groupByClause == null) {
            List<Expr> aggregateExpr = Lists.newArrayList();
            TreeNode.collect(resultExprs, Expr.isAggregatePredicate(), aggregateExpr);
            if (aggregateExpr.size() == resultExprs.size()) {
                selectList.setIsDistinct(false);
            }
        }

        if (selectList.isDistinct()
                && (groupByClause != null
                || TreeNode.contains(resultExprs, Expr.isAggregatePredicate())
                || (havingClauseAfterAnaylzed != null && havingClauseAfterAnaylzed.contains(Expr.isAggregatePredicate())))) {
            throw new AnalysisException("cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
        }

        // disallow '*' and explicit GROUP BY (we can't group by '*', and if you need to
        // name all star-expanded cols in the group by clause you might as well do it
        // in the select list)
        if (groupByClause != null ||
                TreeNode.contains(resultExprs, Expr.isAggregatePredicate())) {
            for (SelectListItem item : selectList.getItems()) {
                if (item.isStar()) {
                    throw new AnalysisException(
                            "cannot combine '*' in select list with GROUP BY: " + item.toSql());
                }
            }
        }

        // Collect the aggregate expressions from the SELECT, HAVING and ORDER BY clauses
        // of this statement.
        ArrayList<FunctionCallExpr> aggExprs = Lists.newArrayList();
        TreeNode.collect(resultExprs, Expr.isAggregatePredicate(), aggExprs);
        if (havingClauseAfterAnaylzed != null) {
            havingClauseAfterAnaylzed.collect(Expr.isAggregatePredicate(), aggExprs);
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

        List<TupleId> groupingByTupleIds = new ArrayList<>();
        if (groupByClause != null) {
            // must do it before copying for createAggInfo()
            if (groupingInfo != null) {
                groupingByTupleIds.add(groupingInfo.getVirtualTuple().getId());
            }
            groupByClause.genGroupingExprs();
            if (groupingInfo != null) {
                GroupByClause.GroupingType groupingType = groupByClause.getGroupingType();
                if ((groupingType == GroupByClause.GroupingType.GROUPING_SETS && CollectionUtils
                        .isNotEmpty(groupByClause.getGroupingSetList()))
                        || groupingType == GroupByClause.GroupingType.CUBE
                        || groupingType == GroupByClause.GroupingType.ROLLUP) {

                }
                groupingInfo.buildRepeat(groupByClause.getGroupingExprs(), groupByClause.getGroupingSetList());
            }
            substituteOrdinalsAliases(groupByClause.getGroupingExprs(), "GROUP BY", analyzer);
            groupByClause.analyze(analyzer);
            createAggInfo(groupByClause.getGroupingExprs(), aggExprs, analyzer);
        } else {
            createAggInfo( new ArrayList<>(), aggExprs, analyzer);
        }

        // combine avg smap with the one that produces the final agg output
        AggregateInfo finalAggInfo =
                aggInfo.getSecondPhaseDistinctAggInfo() != null
                        ? aggInfo.getSecondPhaseDistinctAggInfo()
                        : aggInfo;
        groupingByTupleIds.add(finalAggInfo.getOutputTupleId());
        ExprSubstitutionMap combinedSmap = ExprSubstitutionMap.compose(countAllMap, finalAggInfo.getOutputSmap(), analyzer);
        // change select list, having and ordering exprs to point to agg output. We need
        // to reanalyze the exprs at this point.
        if (LOG.isDebugEnabled()) {
            LOG.debug("combined smap: " + combinedSmap.debugString());
            LOG.debug("desctbl: " + analyzer.getDescTbl().debugString());
            LOG.debug("resultexprs: " + Expr.debugString(resultExprs));
        }

        if (havingClauseAfterAnaylzed != null) {
            // forbidden correlated subquery in having clause
            List<Subquery> subqueryInHaving = Lists.newArrayList();
            havingClauseAfterAnaylzed.collect(Subquery.class, subqueryInHaving);
            for (Subquery subquery : subqueryInHaving) {
                if (subquery.isCorrelatedPredicate(getTableRefIds())) {
                    throw new AnalysisException("The correlated having clause is not supported");
                }
            }
        }

        /*
         * All of columns of result and having clause are replaced by new slot ref which is bound by top tuple of agg info.
         * For example:
         * ResultExprs: SlotRef(k1), FunctionCall(sum(SlotRef(k2)))
         * Having predicate: FunctionCall(sum(SlotRef(k2))) > subquery
         * CombinedSMap: <SlotRef(k1) tuple 0, SlotRef(k1) of tuple 3>,
         *               <FunctionCall(SlotRef(k2)) tuple 0, SlotRef(sum(k2)) of tuple 3>
         *
         * After rewritten:
         * ResultExprs: SlotRef(k1) of tuple 3, SlotRef(sum(k2)) of tuple 3
         * Having predicate: SlotRef(sum(k2)) of tuple 3 > subquery
         */
        resultExprs = Expr.substituteList(resultExprs, combinedSmap, analyzer, false);
        if (LOG.isDebugEnabled()) {
            LOG.debug("post-agg selectListExprs: " + Expr.debugString(resultExprs));
        }
        if (havingClauseAfterAnaylzed != null) {
            havingPred = havingClauseAfterAnaylzed.substitute(combinedSmap, analyzer, false);
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
            if (!resultExprs.get(i).isBoundByTupleIds(groupingByTupleIds)) {
                throw new AnalysisException(
                        "select list expression not produced by aggregation output " + "(missing from " +
                                "GROUP BY clause?): " + selectList.getItems().get(i).getExpr().toSql());
            }
        }
        if (orderByElements != null) {
            for (int i = 0; i < orderByElements.size(); ++i) {
                if (!sortInfo.getOrderingExprs().get(i).isBoundByTupleIds(groupingByTupleIds)) {
                    throw new AnalysisException(
                            "ORDER BY expression not produced by aggregation output " + "(missing from " +
                                    "GROUP BY clause?): " + orderByElements.get(i).getExpr().toSql());
                }

                if (sortInfo.getOrderingExprs().get(i).type.isHllType()) {
                    throw new AnalysisException("ORDER BY expression could not contain hll column.");
                }

                if (sortInfo.getOrderingExprs().get(i).type.isBitmapType()) {
                    throw new AnalysisException("ORDER BY expression could not contain bitmap column.");
                }
            }
        }
        if (havingPred != null) {
            if (!havingPred.isBoundByTupleIds(groupingByTupleIds)) {
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
            if (functionName.equalsIgnoreCase(FunctionSet.COUNT)) {
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("multi distinct smap: {}", result.debugString());
        }
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

        if (groupByClause != null && !groupByClause.isEmpty()) {
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
                        return expr.getFnName().getFunction().equals(FunctionSet.COUNT);
                    }
                };

        Iterable<FunctionCallExpr> countAllAggs =
                Iterables.filter(aggExprs, Predicates.and(isCountPred, isNotDistinctPred));
        for (FunctionCallExpr countAllAgg : countAllAggs) {
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
        for (Expr expr : analyticExprs) {
            AnalyticExpr toRewrite = (AnalyticExpr) expr;
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
            // Collect the new exprs introduced through the equal and the non-equal exprs.
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
        rewriteSelectList(rewriter);
        for (TableRef ref : fromClause_) {
            ref.rewriteExprs(rewriter, analyzer);
        }
        // Also equal exprs in the statements of subqueries.
        List<Subquery> subqueryExprs = Lists.newArrayList();
        if (whereClause != null) {
            whereClause = rewriter.rewrite(whereClause, analyzer);
            whereClause.collect(Subquery.class, subqueryExprs);

        }
        if (havingClause != null) {
            havingClause = rewriter.rewrite(havingClause, analyzer);
            havingClauseAfterAnaylzed.collect(Subquery.class, subqueryExprs);
        }
        for (Subquery subquery : subqueryExprs) {
            subquery.getStatement().rewriteExprs(rewriter);
        }
        if (groupByClause != null) {
            ArrayList<Expr> groupingExprs = groupByClause.getGroupingExprs();
            if (groupingExprs != null) {
                rewriter.rewriteList(groupingExprs, analyzer);
            }
            List<Expr> oriGroupingExprs = groupByClause.getOriGroupingExprs();
            if (oriGroupingExprs != null) {
                rewriter.rewriteList(oriGroupingExprs, analyzer);
            }
        }
        if (orderByElements != null) {
            for (OrderByElement orderByElem : orderByElements) {
                orderByElem.setExpr(rewriter.rewrite(orderByElem.getExpr(), analyzer));
            }
        }
    }

    private void rewriteSelectList(ExprRewriter rewriter) throws AnalysisException {
        for (SelectListItem item : selectList.getItems()) {
            if (item.getExpr() instanceof CaseExpr && item.getExpr().contains(Predicates.instanceOf(Subquery.class))) {
                rewriteSubquery(item.getExpr(), analyzer);
            }
        }
        selectList.rewriteExprs(rewriter, analyzer);
    }

    /** equal subquery in case when to an inline view
     *  subquery in case when statement like
     *
     * SELECT CASE
     *         WHEN (
     *             SELECT COUNT(*) / 2
     *             FROM t
     *         ) > k4 THEN (
     *             SELECT AVG(k4)
     *             FROM t
     *         )
     *         ELSE (
     *             SELECT SUM(k4)
     *             FROM t
     *         )
     *     END AS kk4
     * FROM t;
     * this statement will be equal to
     *
     * SELECT CASE
     *         WHEN t1.a > k4 THEN t2.a
     *         ELSE t3.a
     *     END AS kk4
     * FROM t, (
     *         SELECT COUNT(*) / 2 AS a
     *         FROM t
     *     ) t1,  (
     *         SELECT AVG(k4) AS a
     *         FROM t
     *     ) t2,  (
     *         SELECT SUM(k4) AS a
     *         FROM t
     *     ) t3;
     */
    private Expr rewriteSubquery(Expr expr, Analyzer analyzer)
            throws AnalysisException {
        if (expr instanceof Subquery) {
            if (!(((Subquery) expr).getStatement() instanceof SelectStmt)) {
                throw new AnalysisException("Only support select subquery in case-when clause.");
            }
            if (expr.isCorrelatedPredicate(getTableRefIds())) {
                throw new AnalysisException("The correlated subquery in case-when clause is not supported");
            }
            SelectStmt subquery = (SelectStmt) ((Subquery) expr).getStatement();
            if (subquery.resultExprs.size() != 1 || !subquery.returnsSingleRow()) {
                throw new AnalysisException("Subquery in case-when must return scala type");
            }
            subquery.reset();
            subquery.setAssertNumRowsElement(1, AssertNumRowsElement.Assertion.EQ);
            String alias = getTableAliasGenerator().getNextAlias();
            String colAlias = getColumnAliasGenerator().getNextAlias();
            InlineViewRef inlineViewRef = new InlineViewRef(alias, subquery, Arrays.asList(colAlias));
            try {
                inlineViewRef.analyze(analyzer);
            } catch (UserException e) {
                throw new AnalysisException(e.getMessage());
            }
            fromClause_.add(inlineViewRef);
            expr = new SlotRef(inlineViewRef.getAliasAsName(), colAlias);
        } else if (CollectionUtils.isNotEmpty(expr.getChildren())) {
            for (int i = 0; i < expr.getChildren().size(); ++i) {
                expr.setChild(i, rewriteSubquery(expr.getChild(i), analyzer));
            }
        }
        return expr;
    }

    @Override
    public String toSql() {
        if (sqlString_ != null) {
            return sqlString_;
        }
        StringBuilder strBuilder = new StringBuilder();
        if (withClause_ != null) {
            strBuilder.append(withClause_.toSql());
            strBuilder.append(" ");
        }

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
        if (!fromClause_.isEmpty()) {
            strBuilder.append(fromClause_.toSql());
        }

        // Where clause
        if (whereClause != null) {
            strBuilder.append(" WHERE ");
            strBuilder.append(whereClause.toSql());
        }
        // Group By clause
        if (groupByClause != null) {
            strBuilder.append(" GROUP BY ");
            strBuilder.append(groupByClause.toSql());
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

        if (hasOutFileClause()) {
            strBuilder.append(outFileClause.toSql());
        }
        return strBuilder.toString();
    }

    /**
     * If the select statement has a sort/top that is evaluated, then the sort tuple
     * is materialized. Else, if there is aggregation then the aggregate tuple id is
     * materialized. Otherwise, all referenced tables are materialized as long as they are
     * not semi-joined. If there are analytics and no sort, then the returned tuple
     * ids also include the logical analytic output tuple.
     */
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
        // Fixme(kks): get tuple id from analyticInfo is wrong, should get from AnalyticEvalNode
        // We materialize the agg tuple or the table refs together with the analytic tuple.
        if (hasAnalyticInfo() && isEvaluateOrderBy()) {
            tupleIdList.add(analyticInfo.getOutputTupleId());
        }
    }

    @Override
    public void substituteSelectList(Analyzer analyzer, List<String> newColLabels)
            throws AnalysisException, UserException {
        // analyze with clause
        if (hasWithClause()) {
            withClause_.analyze(analyzer);
        }
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
                if (item.getExpr() instanceof AnalyticExpr) {
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
        if (groupByClause != null) {
            substituteOrdinalsAliases(groupByClause.getGroupingExprs(), "GROUP BY", analyzer);
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

    public boolean hasWhereClause() {
        return whereClause != null;
    }

    public boolean hasAggInfo() {
        return aggInfo != null;
    }

    public boolean hasGroupByClause() {
        return groupByClause != null;
    }

    /**
     * Check if the stmt returns a single row. This can happen
     * in the following cases:
     * 1. select stmt with a 'limit 1' clause
     * 2. select stmt with an aggregate function and no group by.
     * 3. select stmt with no from clause.
     * <p>
     * This function may produce false negatives because the cardinality of the
     * result set also depends on the data a stmt is processing.
     */
    public boolean returnsSingleRow() {
        // limit 1 clause
        if (hasLimitClause() && getLimit() == 1) {
            return true;
        }
        // No from clause (base tables or inline views)
        if (fromClause_.isEmpty()) {
            return true;
        }
        // Aggregation with no group by and no DISTINCT
        if (hasAggInfo() && !hasGroupByClause() && !selectList.isDistinct()) {
            return true;
        }
        // In all other cases, return false.
        return false;
    }

    @Override
    public void collectTableRefs(List<TableRef> tblRefs) {
        for (TableRef tblRef : fromClause_) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
                inlineViewRef.getViewStmt().collectTableRefs(tblRefs);
            } else {
                tblRefs.add(tblRef);
            }
        }
    }

    private boolean checkGroupingFn(Expr expr) {
        if (expr instanceof GroupingFunctionCallExpr) {
            return true;
        } else if (expr.getChildren() != null && expr.getChildren().size() > 0) {
            for (Expr child : expr.getChildren()) {
                if (checkGroupingFn(child)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof SelectStmt)) {
            return false;
        }
        return this.id.equals(((SelectStmt) obj).id);
    }
}
