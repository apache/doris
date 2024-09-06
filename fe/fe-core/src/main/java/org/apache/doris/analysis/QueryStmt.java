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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/QueryStmt.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.rewrite.mvrewrite.CountDistinctToBitmapOrHLLRule;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract base class for any statement that returns results
 * via a list of result expressions, for example a
 * SelectStmt or UnionStmt. Also maintains a map of expression substitutions
 * for replacing expressions from ORDER BY or GROUP BY clauses with
 * their corresponding result expressions.
 * Used for sharing members/methods and some of the analysis code, in particular the
 * analysis of the ORDER BY and LIMIT clauses.
 */
@Deprecated
public abstract class QueryStmt extends StatementBase implements Queriable, NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(QueryStmt.class);

    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    protected WithClause withClause;

    protected ArrayList<OrderByElement> orderByElements;
    // Limit element could not be null, the default limit element is NO_LIMIT
    protected LimitElement limitElement;
    // This is a internal element which is used to query plan.
    // It will not change the origin stmt and present in toSql.
    protected AssertNumRowsElement assertNumRowsElement;

    /**
     * For a select statment:
     * List of executable exprs in select clause (star-expanded, ordinals and
     * aliases substituted, agg output substituted
     * For a union statement:
     * List of slotrefs into the tuple materialized by the union.
     */
    protected ArrayList<Expr> resultExprs = Lists.newArrayList();

    // For a select statement: select list exprs resolved to base tbl refs
    // For a union statement: same as resultExprs
    /**
     * For inline view, Doris will generate an extra layer of tuple (virtual) during semantic analysis.
     * But if the expr in the outer select stmt involves columns in the inline view,
     * it can only be mapped to this layer of tuple (virtual) at the beginning (@resultExprs).
     * If you want to really associate with the column in the inlineview,
     * you need to substitute it with baseTblSmap to get the correct expr (@baseTblResultExprs).
     * resultExprs + baseTblSmap = baseTblResultExprs
     * For example:
     * select c1 from (select k1 c1 from table group by k1) tmp;
     * tuple 0: table, tuple1: group by tuple2: tmp
     * resultExprs: c1 belongs to tuple2(tmp)
     * baseTblResultExprs: c1 belongs to tuple1(group by)
     */
    protected ArrayList<Expr> baseTblResultExprs = Lists.newArrayList();

    /**
     * Map of expression substitutions for replacing aliases
     * in "order by" or "group by" clauses with their corresponding result expr.
     */
    protected final ExprSubstitutionMap aliasSMap;

    protected final ExprSubstitutionMap mvSMap;

    /**
     * Select list item alias does not have to be unique.
     * This list contains all the non-unique aliases. For example,
     *  select int_col a, string_col a from alltypessmall;
     * Both columns are using the same alias "a".
     */
    protected final ArrayList<Expr> ambiguousAliasList;

    protected SortInfo sortInfo;

    // evaluateOrderBy_ is true if there is an order by clause that must be evaluated.
    // False for nested query stmts with an order-by clause without offset/limit.
    // sortInfo_ is still generated and used in analysis to ensure that the order-by clause
    // is well-formed.
    protected boolean evaluateOrderBy;

    protected boolean needToSql = false;

    // used by hll
    protected boolean fromInsert = false;

    // order by elements which has been analyzed
    // For example: select k1 a from t order by a;
    // this parameter: order by t.k1
    protected List<OrderByElement> orderByElementsAfterAnalyzed;

    /////////////////////////////////////////
    // END: Members that need to be reset()

    // represent the "INTO OUTFILE" clause
    protected OutFileClause outFileClause;

    /**
     * If the query stmt belongs to CreateMaterializedViewStmt,
     * such as
     * `CREATE MATERIALIZED VIEW mv AS SELECT bitmap_union(to_bitmap(k1)) from table`
     * query stmt will not be rewrite by MVRewriter.
     * The `bitmap_union(to_bitmap(k1))` is the definition of the mv column rather then a expr.
     * So `forbiddenMVRewrite` will be set to true to protect the definition of the mv column from being overwritten.
     * <p>
     * In other query case, `forbiddenMVRewrite` is always false.
     */
    private boolean forbiddenMVRewrite = false;

    /**
     * If the tuple id in `disableMVRewriteTupleIds`, the expr which belongs to this tuple will not be MVRewritten.
     * Initially this set is an empty set.
     * When the scan node is unable to match any index in selecting the materialized view,
     *   the tuple is added to this set.
     * The query will be re-executed, and this tuple will not be mv rewritten.
     * For example:
     * TableA: (k1 int, k2 int, k3 int)
     * MV: (k1 int, mv_bitmap_union_k2 bitmap bitmap_union)
     * Query: select k3, bitmap_union(to_bitmap(k2)) from TableA
     * First analyze: MV rewriter enable and this set is empty
     *     select k3, bitmap_union(mv_bitmap_union_k2) from TableA
     * SingleNodePlanner: could not select any index for TableA
     *     Add table to disableMVRewriteTupleIds.
     * `disableMVRewriteTupleIds` = {TableA}
     * Re-executed:
     * Second analyze: MV rewrite disable in table and use origin stmt.
     *     select k3, bitmap_union(to_bitmap(k2)) from TableA
     * SingleNodePlanner: base index selected
     */
    private Set<TupleId> disableTuplesMVRewriter = Sets.newHashSet();

    protected boolean toSQLWithSelectList;
    protected boolean isPointQuery;
    protected boolean toSQLWithHint;

    QueryStmt(ArrayList<OrderByElement> orderByElements, LimitElement limitElement) {
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
        this.aliasSMap = new ExprSubstitutionMap();
        this.mvSMap = new ExprSubstitutionMap();
        this.ambiguousAliasList = Lists.newArrayList();
        this.sortInfo = null;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (isAnalyzed()) {
            return;
        }
        super.analyze(analyzer);
        analyzeLimit(analyzer);
        if (hasWithClause()) {
            withClause.analyze(analyzer);
        }
    }

    private void analyzeLimit(Analyzer analyzer) throws AnalysisException {
        limitElement.analyze(analyzer);
    }

    /**
     * Returns a list containing all the materialized tuple ids that this stmt is
     * correlated with (i.e., those tuple ids from outer query blocks that TableRefs
     * inside this stmt are rooted at).
     *
     * Throws if this stmt contains an illegal mix of un/correlated table refs.
     * A statement is illegal if it contains a TableRef correlated with a parent query
     * block as well as a table ref with an absolute path (e.g. a BaseTabeRef). Such a
     * statement would generate a Subplan containing a base table scan (very expensive),
     * and should therefore be avoided.
     *
     * In other words, the following cases are legal:
     * (1) only uncorrelated table refs
     * (2) only correlated table refs
     * (3) a mix of correlated table refs and table refs rooted at those refs
     *     (the statement is 'self-contained' with respect to correlation)
     */
    public List<TupleId> getCorrelatedTupleIds(Analyzer analyzer) throws AnalysisException {
        // Correlated tuple ids of this stmt.
        List<TupleId> correlatedTupleIds = Lists.newArrayList();
        // First correlated and absolute table refs. Used for error detection/reporting.
        // We pick the first ones for simplicity. Choosing arbitrary ones is equally valid.
        TableRef correlatedRef = null;
        TableRef absoluteRef = null;
        // Materialized tuple ids of the table refs checked so far.
        Set<TupleId> tblRefIds = Sets.newHashSet();

        List<TableRef> tblRefs = Lists.newArrayList();
        collectTableRefs(tblRefs);
        for (TableRef tblRef : tblRefs) {
            if (absoluteRef == null && !tblRef.isRelative()) {
                absoluteRef = tblRef;
            }
            /*if (tblRef.isCorrelated()) {
             *
             *   // Check if the correlated table ref is rooted at a tuple descriptor from within
             *   // this query stmt. If so, the correlation is contained within this stmt
             *   // and the table ref does not conflict with absolute refs.
             *   CollectionTableRef t = (CollectionTableRef) tblRef;
             *   Preconditions.checkState(t.getResolvedPath().isRootedAtTuple());
             *   // This check relies on tblRefs being in depth-first order.
             *   if (!tblRefIds.contains(t.getResolvedPath().getRootDesc().getId())) {
             *       if (correlatedRef == null) correlatedRef = tblRef;
             *       correlatedTupleIds.add(t.getResolvedPath().getRootDesc().getId());
             *   }
             *
            }*/
            if (correlatedRef != null && absoluteRef != null) {
                throw new AnalysisException(String.format(
                        "Nested query is illegal because it contains a table reference '%s' "
                                + "correlated with an outer block as well as an uncorrelated one '%s':\n%s",
                        correlatedRef.tableRefToSql(), absoluteRef.tableRefToSql(), toSql()));
            }
            tblRefIds.add(tblRef.getId());
        }
        return correlatedTupleIds;
    }

    public boolean isEvaluateOrderBy() {
        return evaluateOrderBy;
    }

    public ArrayList<Expr> getBaseTblResultExprs() {
        return baseTblResultExprs;
    }

    public void setNeedToSql(boolean needToSql) {
        this.needToSql = needToSql;
    }

    public boolean isDisableTuplesMVRewriter(Expr expr) {
        if (disableTuplesMVRewriter.isEmpty()) {
            return false;
        }
        return expr.isBoundByTupleIds(disableTuplesMVRewriter.stream().collect(Collectors.toList()));
    }

    protected Expr rewriteQueryExprByMvColumnExpr(Expr expr, Analyzer analyzer) throws AnalysisException {
        if (analyzer == null || analyzer.getMVExprRewriter() == null) {
            return expr;
        }
        ExprRewriter rewriter;
        if (forbiddenMVRewrite) {
            rewriter = new ExprRewriter(Lists.newArrayList(CountDistinctToBitmapOrHLLRule.INSTANCE),
                    Lists.newArrayList());
        } else {
            rewriter = analyzer.getMVExprRewriter();
            rewriter.reset();
            rewriter.setInfoMVRewriter(disableTuplesMVRewriter, mvSMap, aliasSMap);
        }
        rewriter.setUpBottom();

        Expr result = rewriter.rewrite(expr, analyzer);
        return result;
    }

    /**
     * Creates sortInfo by resolving aliases and ordinals in the orderingExprs.
     * If the query stmt is an inline view/union operand, then order-by with no
     * limit with offset is not allowed, since that requires a sort and merging-exchange,
     * and subsequent query execution would occur on a single machine.
     * Sets evaluateOrderBy_ to false for ignored order-by w/o limit/offset in nested
     * queries.
     */
    protected void createSortInfo(Analyzer analyzer) throws AnalysisException {
        // not computing order by
        if (orderByElements == null) {
            evaluateOrderBy = false;
            return;
        }

        ArrayList<Expr> orderingExprs = Lists.newArrayList();
        ArrayList<Boolean> isAscOrder = Lists.newArrayList();
        ArrayList<Boolean> nullsFirstParams = Lists.newArrayList();

        // extract exprs
        for (OrderByElement orderByElement : orderByElements) {
            // create copies, we don't want to modify the original parse node, in case
            // we need to print it
            orderingExprs.add(orderByElement.getExpr().clone());
            isAscOrder.add(Boolean.valueOf(orderByElement.getIsAsc()));
            nullsFirstParams.add(orderByElement.getNullsFirstParam());
        }
        substituteOrdinalsAliases(orderingExprs, "ORDER BY", analyzer, true);

        // save the order by element after analyzed
        orderByElementsAfterAnalyzed = Lists.newArrayList();
        for (int i = 0; i < orderByElements.size(); i++) {
            // equal count distinct
            orderingExprs.set(i, rewriteQueryExprByMvColumnExpr(orderingExprs.get(i), analyzer));
            OrderByElement orderByElement = new OrderByElement(orderingExprs.get(i), isAscOrder.get(i),
                    nullsFirstParams.get(i));
            orderByElementsAfterAnalyzed.add(orderByElement);
        }

        if (!analyzer.isRootAnalyzer() && hasOffset() && !hasLimit()) {
            throw new AnalysisException("Order-by with offset without limit not supported"
                    + " in nested queries.");
        }

        for (Expr expr : orderingExprs) {
            if (expr.getType().isOnlyMetricType()) {
                throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
            }
        }

        sortInfo = new SortInfo(orderingExprs, isAscOrder, nullsFirstParams);
        // order by w/o limit and offset in inline views, set operands and insert statements
        // are ignored.
        if (!hasLimit() && !hasOffset() && (!analyzer.isRootAnalyzer() || fromInsert)) {
            evaluateOrderBy = false;
            if (LOG.isDebugEnabled()) {
                // Return a warning that the order by was ignored.
                StringBuilder strBuilder = new StringBuilder();
                strBuilder.append("Ignoring ORDER BY clause without LIMIT or OFFSET: ");
                strBuilder.append("ORDER BY ");
                strBuilder.append(orderByElements.get(0).toSql());
                for (int i = 1; i < orderByElements.size(); ++i) {
                    strBuilder.append(", ").append(orderByElements.get(i).toSql());
                }
                strBuilder.append(".\nAn ORDER BY appearing in a view, subquery, union operand, ");
                strBuilder.append("or an insert/ctas statement has no effect on the query result ");
                strBuilder.append("unless a LIMIT and/or OFFSET is used in conjunction ");
                strBuilder.append("with the ORDER BY.");
                if (LOG.isDebugEnabled()) {
                    LOG.debug(strBuilder.toString());
                }
            }
        } else {
            evaluateOrderBy = true;
        }
    }

    /**
     * Create a tuple descriptor for the single tuple that is materialized, sorted and
     * output by the exec node implementing the sort. Done by materializing slot refs in
     * the order-by and result expressions. Those SlotRefs in the ordering and result exprs
     * are substituted with SlotRefs into the new tuple. This simplifies sorting logic for
     * total (no limit) sorts.
     * Done after analyzeAggregation() since ordering and result exprs may refer to the
     * outputs of aggregation.
     */
    protected void createSortTupleInfo(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(evaluateOrderBy);

        for (Expr orderingExpr : sortInfo.getOrderingExprs()) {
            if (orderingExpr.getType().isComplexType()) {
                throw new AnalysisException(String.format(
                        "ORDER BY expression '%s' with "
                                + "complex type '%s' is not supported.",
                        orderingExpr.toSql(), orderingExpr.getType().toSql()));
            }
        }

        ExprSubstitutionMap smap = sortInfo.createSortTupleInfo(resultExprs, analyzer);

        for (int i = 0; i < smap.size(); ++i) {
            if (!(smap.getLhs().get(i) instanceof SlotRef)
                    || !(smap.getRhs().get(i) instanceof SlotRef)) {
                continue;
            }
            // TODO(zc)
            // SlotRef inputSlotRef = (SlotRef) smap.getLhs().get(i);
            // SlotRef outputSlotRef = (SlotRef) smap.getRhs().get(i);
            // if (hasLimit()) {
            //     analyzer.registerValueTransfer(
            //             inputSlotRef.getSlotId(), outputSlotRef.getSlotId());
            // } else {
            //     analyzer.createAuxEquivPredicate(outputSlotRef, inputSlotRef);
            // }
        }

        substituteResultExprs(smap, analyzer);
    }

    /**
     * Return the first expr in exprs that is a non-unique alias. Return null if none of
     * exprs is an ambiguous alias.
     */
    protected Expr getFirstAmbiguousAlias(List<Expr> exprs) {
        for (Expr exp : exprs) {
            if (ambiguousAliasList.contains(exp)) {
                return exp;
            }
        }
        return null;
    }

    protected Expr getFirstAmbiguousAlias(Expr expr) {
        return expr.findEqual(ambiguousAliasList);
    }

    /**
     * Substitute exprs of the form "<number>"  with the corresponding
     * expressions and any alias references in aliasSmap_.
     * Modifies exprs list in-place.
     */
    protected void substituteOrdinalsAliases(List<Expr> exprs, String errorPrefix,
                                             Analyzer analyzer, boolean aliasFirst) throws AnalysisException {
        Expr ambiguousAlias = getFirstAmbiguousAlias(exprs);
        if (ambiguousAlias != null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NON_UNIQ_ERROR, ambiguousAlias.toColumnLabel());
        }

        ListIterator<Expr> i = exprs.listIterator();
        while (i.hasNext()) {
            Expr expr = i.next();
            // We can substitute either by ordinal or by alias.
            // If we substitute by ordinal, we should not replace any aliases, since
            // the new expression was copied from the select clause context, where
            // alias substitution is not performed in the same way.
            Expr substituteExpr = trySubstituteOrdinal(expr, errorPrefix, analyzer);
            if (substituteExpr == null) {
                if (aliasFirst) {
                    substituteExpr = expr.trySubstitute(aliasSMap, analyzer, false);
                } else {
                    try {
                        // use col name from tableRefs first
                        substituteExpr = expr.clone();
                        substituteExpr.analyze(analyzer);
                    } catch (AnalysisException ex) {
                        if (ConnectContext.get() != null) {
                            ConnectContext.get().getState().reset();
                        }
                        // then consider alias name
                        substituteExpr = expr.trySubstitute(aliasSMap, analyzer, false);
                    }
                }
            }
            i.set(substituteExpr);
        }
    }

    // Attempt to replace an expression of form "<number>" with the corresponding
    // select list items.  Return null if not an ordinal expression.
    private Expr trySubstituteOrdinal(Expr expr, String errorPrefix,
                                      Analyzer analyzer) throws AnalysisException {
        if (!(expr instanceof IntLiteral)) {
            return null;
        }
        expr.analyze(analyzer);
        if (!expr.getType().isIntegerType()) {
            return null;
        }
        long pos = ((IntLiteral) expr).getLongValue();
        if (pos < 1) {
            throw new AnalysisException(
                    errorPrefix + ": ordinal must be >= 1: " + expr.toSql());
        }
        if (pos > resultExprs.size()) {
            throw new AnalysisException(
                    errorPrefix + ": ordinal exceeds number of items in select list: " + expr.toSql());
        }

        // Create copy to protect against accidentally shared state.
        return resultExprs.get((int) pos - 1).clone();
    }

    public void getWithClauseTables(Analyzer analyzer, boolean expandView, Map<Long, TableIf> tableMap,
            Set<String> parentViewNameSet) throws AnalysisException {
        if (withClause != null) {
            withClause.getTables(analyzer, expandView, tableMap, parentViewNameSet);
        }
    }

    public void getWithClauseTableRefs(Analyzer analyzer, List<TableRef> tblRefs, Set<String> parentViewNameSet) {
        if (withClause != null) {
            withClause.getTableRefs(analyzer, tblRefs, parentViewNameSet);
        }
    }

    /**
     * collect all exprs of a QueryStmt to a map
     * @param exprMap
     */
    public void collectExprs(Map<String, Expr> exprMap) {
    }

    /**
     * put all rewritten exprs back to the ori QueryStmt
     * @param rewrittenExprMap
     */
    public void putBackExprs(Map<String, Expr> rewrittenExprMap) {
    }


    @Override
    public void foldConstant(ExprRewriter rewriter, TQueryOptions tQueryOptions) throws AnalysisException {
        Preconditions.checkState(isAnalyzed());
        Map<String, Expr> exprMap = new HashMap<>();
        collectExprs(exprMap);
        rewriter.rewriteConstant(exprMap, analyzer, tQueryOptions);
        if (rewriter.changed()) {
            putBackExprs(exprMap);
        }

    }

    @Override
    public void rewriteElementAtToSlot(ExprRewriter rewriter, TQueryOptions tQueryOptions) throws AnalysisException {
    }


    /**
     * register expr_id of expr and its children, if not set
     * @param expr
     */
    public void registerExprId(Expr expr) {
        if (expr.getId() == null) {
            analyzer.registerExprId(expr);
        }
        for (Expr child : expr.getChildren()) {
            registerExprId(child);
        }
    }

    /**
     * check whether expr and it's children contain alias
     * @param expr expr to be checked
     * @return true if contains, otherwise false
     */
    public boolean containAlias(Expr expr) {
        for (Expr child : expr.getChildren()) {
            if (containAlias(child)) {
                return true;
            }
        }

        if (null != aliasSMap.get(expr)) {
            return true;
        }
        return false;
    }

    public Expr getExprFromAliasSMapDirect(Expr expr) throws AnalysisException {
        return expr.trySubstitute(aliasSMap, analyzer, false);
    }


    public Expr getExprFromAliasSMap(Expr expr) throws AnalysisException {
        if (!analyzer.getContext().getSessionVariable().isGroupByAndHavingUseAliasFirst()) {
            return expr;
        }
        return getExprFromAliasSMapDirect(expr);
    }

    // get tables used by this query.
    // Set<String> parentViewNameSet contain parent stmt view name
    // to make sure query like "with tmp as (select * from db1.table1) " +
    //                "select a.siteid, b.citycode, a.siteid from (select siteid, citycode from tmp) a " +
    //                "left join (select siteid, citycode from tmp) b on a.siteid = b.siteid;";
    // tmp in child stmt "(select siteid, citycode from tmp)" do not contain with_Clause
    // so need to check is view name by parentViewNameSet.
    // issue link: https://github.com/apache/doris/issues/4598
    public abstract void getTables(Analyzer analyzer, boolean expandView, Map<Long, TableIf> tables,
            Set<String> parentViewNameSet) throws AnalysisException;

    // get TableRefs in this query, including physical TableRefs of this statement and
    // nested statements of inline views and with_Clause.
    public abstract void getTableRefs(Analyzer analyzer, List<TableRef> tblRefs, Set<String> parentViewNameSet);

    /**
     * UnionStmt and SelectStmt have different implementations.
     */
    public abstract ArrayList<String> getColLabels();

    public abstract ArrayList<List<String>> getSubColPath();

    /**
     * Returns the materialized tuple ids of the output of this stmt.
     * Used in case this stmt is part of an @InlineViewRef,
     * since we need to know the materialized tupls ids of a TableRef.
     */
    public abstract void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList);

    /**
     * Returns all physical (non-inline-view) TableRefs of this statement and the nested
     * statements of inline views. The returned TableRefs are in depth-first order.
     */
    public abstract void collectTableRefs(List<TableRef> tblRefs);

    abstract List<TupleId> collectTupleIds();

    public ArrayList<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public List<OrderByElement> getOrderByElementsAfterAnalyzed() {
        return orderByElementsAfterAnalyzed;
    }

    public void removeOrderByElements() {
        orderByElements = null;
    }

    public void setWithClause(WithClause withClause) {
        this.withClause = withClause;
    }

    public boolean hasWithClause() {
        return withClause != null;
    }

    public WithClause getWithClause() {
        return withClause;
    }

    public boolean hasOrderByClause() {
        return orderByElements != null;
    }

    public boolean hasLimit() {
        return limitElement != null && limitElement.hasLimit();
    }

    public boolean hasOffset() {
        return limitElement != null && limitElement.hasOffset();
    }

    public long getLimit() {
        return limitElement.getLimit();
    }

    public void setLimit(long limit) {
        Preconditions.checkState(limit >= 0);
        long newLimit = hasLimitClause() ? Math.min(limit, getLimit()) : limit;
        long offset = hasLimitClause() ? getOffset() : 0;
        limitElement = new LimitElement(offset, newLimit);
    }

    public void removeLimitElement() {
        limitElement = LimitElement.NO_LIMIT;
    }

    public long getOffset() {
        return limitElement.getOffset();
    }

    public void setAssertNumRowsElement(int desiredNumOfRows, AssertNumRowsElement.Assertion assertion) {
        this.assertNumRowsElement = new AssertNumRowsElement(desiredNumOfRows, toSql(), assertion);
    }

    public AssertNumRowsElement getAssertNumRowsElement() {
        return assertNumRowsElement;
    }

    public void setIsExplain(ExplainOptions options) {
        this.explainOptions = options;
    }

    public boolean isExplain() {
        return this.explainOptions != null;
    }

    public boolean hasLimitClause() {
        return limitElement.hasLimit();
    }

    public SortInfo getSortInfo() {
        return sortInfo;
    }

    public boolean evaluateOrderBy() {
        return evaluateOrderBy;
    }

    public ArrayList<Expr> getResultExprs() {
        return resultExprs;
    }

    /**
     * Substitutes the result expressions with smap. Preserves the original types of
     * those expressions during the substitution.
     */
    public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
        resultExprs = Expr.substituteList(resultExprs, smap, analyzer, true);
    }

    public boolean isForbiddenMVRewrite() {
        return forbiddenMVRewrite;
    }

    public void forbiddenMVRewrite() {
        this.forbiddenMVRewrite = true;
    }

    public void updateDisableTuplesMVRewriter(TupleId tupleId) {
        disableTuplesMVRewriter.add(tupleId);
    }

    public void updateDisableTuplesMVRewriter(Set<TupleId> tupleIds) {
        disableTuplesMVRewriter.addAll(tupleIds);
    }

    public Set<TupleId> getDisableTuplesMVRewriter() {
        return disableTuplesMVRewriter;
    }

    /**
     * Mark all slots that need to be materialized for the execution of this stmt.
     * This excludes slots referenced in resultExprs (it depends on the consumer of
     * the output of the stmt whether they'll be accessed) and single-table predicates
     * (the PlanNode that materializes that tuple can decide whether evaluating those
     * predicates requires slot materialization).
     * This is called prior to plan tree generation and allows tuple-materializing
     * PlanNodes to compute their tuple's mem layout.
     */
    public abstract void materializeRequiredSlots(Analyzer analyzer) throws AnalysisException;

    /**
     * Mark slots referenced in exprs as materialized.
     */
    protected void materializeSlots(Analyzer analyzer, List<Expr> exprs) {
        List<SlotId> slotIds = Lists.newArrayList();
        for (Expr e : exprs) {
            e.getIds(null, slotIds);
        }
        analyzer.getDescTbl().markSlotsMaterialized(slotIds);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    public ArrayList<OrderByElement> cloneOrderByElements() {
        if (orderByElements == null) {
            return null;
        }
        ArrayList<OrderByElement> result =
                Lists.newArrayListWithCapacity(orderByElements.size());
        for (OrderByElement o : orderByElements) {
            result.add(o.clone());
        }
        return result;
    }

    public WithClause cloneWithClause() {
        return withClause != null ? withClause.clone() : null;
    }

    public OutFileClause cloneOutfileCluse() {
        return outFileClause != null ? outFileClause.clone() : null;
    }

    public String toDigest() {
        return "";
    }

    /**
     * C'tor for cloning.
     */
    protected QueryStmt(QueryStmt other) {
        super(other);
        withClause = other.cloneWithClause();
        outFileClause = other.cloneOutfileCluse();
        orderByElements = other.cloneOrderByElements();
        limitElement = other.limitElement.clone();
        resultExprs = Expr.cloneList(other.resultExprs);
        baseTblResultExprs = Expr.cloneList(other.baseTblResultExprs);
        aliasSMap = other.aliasSMap.clone();
        mvSMap = other.mvSMap.clone();
        ambiguousAliasList = Expr.cloneList(other.ambiguousAliasList);
        sortInfo = (other.sortInfo != null) ? other.sortInfo.clone() : null;
        analyzer = other.analyzer;
        evaluateOrderBy = other.evaluateOrderBy;
        disableTuplesMVRewriter = other.disableTuplesMVRewriter;
    }

    @Override
    public void reset() {
        super.reset();
        if (orderByElements != null) {
            for (OrderByElement o : orderByElements) {
                o.getExpr().reset();
            }
        }
        limitElement.reset();
        resultExprs.clear();
        baseTblResultExprs.clear();
        aliasSMap.clear();
        ambiguousAliasList.clear();
        orderByElementsAfterAnalyzed = null;
        sortInfo = null;
        evaluateOrderBy = false;
        fromInsert = false;
    }

    // DORIS-7361, Reset selectList to keep clean
    public abstract void resetSelectList();

    public void setFromInsert(boolean value) {
        this.fromInsert = value;
    }

    public boolean isFromInsert() {
        return fromInsert;
    }

    @Override
    public abstract QueryStmt clone();

    public abstract void substituteSelectList(Analyzer analyzer, List<String> newColLabels)
            throws AnalysisException, UserException;

    public void setOutFileClause(OutFileClause outFileClause) {
        this.outFileClause = outFileClause;
    }

    public OutFileClause getOutFileClause() {
        return outFileClause;
    }

    public boolean hasOutFileClause() {
        return outFileClause != null;
    }

    public String toSqlWithSelectList() {
        toSQLWithSelectList = true;
        return toSql();
    }

    public boolean isPointQuery() {
        return isPointQuery;
    }

    public String toSqlWithHint() {
        toSQLWithHint = true;
        return toSql();
    }

    public void setToSQLWithHint(boolean enableSqlSqlWithHint) {
        this.toSQLWithHint = enableSqlSqlWithHint;
    }
}
