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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

/**
 * Abstract base class for any statement that returns results
 * via a list of result expressions, for example a
 * SelectStmt or UnionStmt. Also maintains a map of expression substitutions
 * for replacing expressions from ORDER BY or GROUP BY clauses with
 * their corresponding result expressions.
 * Used for sharing members/methods and some of the analysis code, in particular the
 * analysis of the ORDER BY and LIMIT clauses.
 */
public abstract class QueryStmt extends StatementBase {
    private final static Logger LOG = LogManager.getLogger(QueryStmt.class);

    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    protected WithClause withClause_;

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
    protected ArrayList<Expr> baseTblResultExprs = Lists.newArrayList();

    /**
     * Map of expression substitutions for replacing aliases
     * in "order by" or "group by" clauses with their corresponding result expr.
     */
    protected final ExprSubstitutionMap aliasSMap;

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

    QueryStmt(ArrayList<OrderByElement> orderByElements, LimitElement limitElement) {
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
        this.aliasSMap = new ExprSubstitutionMap();
        this.ambiguousAliasList = Lists.newArrayList();
        this.sortInfo = null;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (isAnalyzed()) return;
        super.analyze(analyzer);
        analyzeLimit(analyzer);
        if (hasWithClause()) withClause_.analyze(analyzer);
        if (hasOutFileClause()) outFileClause.analyze(analyzer);
    }

    private void analyzeLimit(Analyzer analyzer) throws AnalysisException {
        // TODO chenhao
        if (limitElement.getOffset() > 0 && !hasOrderByClause()) {
          throw new AnalysisException("OFFSET requires an ORDER BY clause: " +
                  limitElement.toSql().trim());
        }
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
    public List<TupleId> getCorrelatedTupleIds(Analyzer analyzer)
            throws AnalysisException {
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
        for (TableRef tblRef: tblRefs) {
            if (absoluteRef == null && !tblRef.isRelative()) absoluteRef = tblRef;
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
                        "Nested query is illegal because it contains a table reference '%s' " +
                                "correlated with an outer block as well as an uncorrelated one '%s':\n%s",
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


    // for bitmap type, we rewrite count distinct to bitmap_union_count
    // for hll type, we rewrite count distinct to hll_union_agg
    protected Expr rewriteCountDistinctForBitmapOrHLL(Expr expr, Analyzer analyzer) {
        if (ConnectContext.get() == null || !ConnectContext.get().getSessionVariable().isRewriteCountDistinct()) {
            return expr;
        }

        for (int i = 0; i < expr.getChildren().size(); ++i) {
            expr.setChild(i, rewriteCountDistinctForBitmapOrHLL(expr.getChild(i), analyzer));
        }

        if (!(expr instanceof FunctionCallExpr)) {
            return expr;
        }

        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        if (functionCallExpr.isCountDistinctBitmapOrHLL()) {
            FunctionParams newParams = new FunctionParams(false, functionCallExpr.getParams().exprs());
            if (expr.getChild(0).type.isBitmapType()) {
                FunctionCallExpr bitmapExpr = new FunctionCallExpr(FunctionSet.BITMAP_UNION_COUNT, newParams);
                bitmapExpr.analyzeNoThrow(analyzer);
                return bitmapExpr;
            } else  {
                FunctionCallExpr hllExpr = new FunctionCallExpr("hll_union_agg", newParams);
                hllExpr.analyzeNoThrow(analyzer);
                return hllExpr;
            }
        }
        return expr;
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
        substituteOrdinalsAliases(orderingExprs, "ORDER BY", analyzer);

        // save the order by element after analyzed
        orderByElementsAfterAnalyzed = Lists.newArrayList();
        for (int i = 0; i < orderByElements.size(); i++) {
            // rewrite count distinct
            orderingExprs.set(i, rewriteCountDistinctForBitmapOrHLL(orderingExprs.get(i), analyzer));
            OrderByElement orderByElement = new OrderByElement(orderingExprs.get(i), isAscOrder.get(i),
                    nullsFirstParams.get(i));
            orderByElementsAfterAnalyzed.add(orderByElement);
        }

        if (!analyzer.isRootAnalyzer() && hasOffset() && !hasLimit()) {
            throw new AnalysisException("Order-by with offset without limit not supported" +
                    " in nested queries.");
        }

        sortInfo = new SortInfo(orderingExprs, isAscOrder, nullsFirstParams);
        // order by w/o limit and offset in inline views, set operands and insert statements
        // are ignored.
        if (!hasLimit() && !hasOffset() && !analyzer.isRootAnalyzer()) {
            evaluateOrderBy = false;
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
            LOG.info(strBuilder.toString());
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
        for (Expr exp: exprs) {
            if (ambiguousAliasList.contains(exp)) return exp;
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
                                             Analyzer analyzer) throws AnalysisException {
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
                substituteExpr = expr.trySubstitute(aliasSMap, analyzer, false);
            }
            i.set(substituteExpr);
        }
    }

    // Attempt to replace an expression of form "<number>" with the corresponding
    // select list items.  Return null if not an ordinal expression.
    private Expr trySubstituteOrdinal(Expr expr, String errorPrefix,
                                      Analyzer analyzer) throws AnalysisException {
        if (!(expr instanceof IntLiteral)) return null;
        expr.analyze(analyzer);
        if (!expr.getType().isIntegerType()) return null;
        long pos = ((IntLiteral) expr).getLongValue();
        if (pos < 1) {
            throw new AnalysisException(
                    errorPrefix + ": ordinal must be >= 1: " + expr.toSql());
        }
        if (pos > resultExprs.size()) {
            throw new AnalysisException(
                    errorPrefix + ": ordinal exceeds number of items in select list: "
                            + expr.toSql());
        }

        // Create copy to protect against accidentally shared state.
        return resultExprs.get((int) pos - 1).clone();
    }

    public void getWithClauseDbs(Analyzer analyzer, Map<String, Database> dbs) throws AnalysisException {
        if (withClause_ != null) {
            withClause_.getDbs(analyzer, dbs);
        }
    }

    // get database used by this query.
    public abstract void getDbs(Analyzer analyzer, Map<String, Database> dbs) throws AnalysisException;

    /**
     * UnionStmt and SelectStmt have different implementations.
     */
    public abstract ArrayList<String> getColLabels();

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

    public void setWithClause(WithClause withClause) { this.withClause_ = withClause; }
    public boolean hasWithClause() { return withClause_ != null; }
    public WithClause getWithClause() { return withClause_; }
    public boolean hasOrderByClause() {
        return orderByElements != null;
    }
    public boolean hasLimit() { return limitElement != null && limitElement.hasLimit(); }
    public boolean hasOffset() { return limitElement != null && limitElement.hasOffset(); }

    public long getLimit() {
        return limitElement.getLimit();
    }

    public void setLimit(long limit) throws AnalysisException {
        Preconditions.checkState(limit >= 0);
        long newLimit = hasLimitClause() ? Math.min(limit, getLimit()) : limit;
        limitElement = new LimitElement(newLimit);
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

    public void setIsExplain(boolean isExplain) {
        this.isExplain = isExplain;
    }

    public boolean isExplain() {
        return isExplain;
    }

    public boolean hasLimitClause() {
        return limitElement.hasLimit();
    }

    public SortInfo getSortInfo() {
        return sortInfo;
    }
    public boolean evaluateOrderBy() { return evaluateOrderBy; }
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
        for (Expr e: exprs) {
            e.getIds(null, slotIds);
        }
        analyzer.getDescTbl().markSlotsMaterialized(slotIds);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    public ArrayList<OrderByElement> cloneOrderByElements() {
        if (orderByElements == null) return null;
        ArrayList<OrderByElement> result =
                Lists.newArrayListWithCapacity(orderByElements.size());
        for (OrderByElement o: orderByElements) result.add(o.clone());
        return result;
    }

    public WithClause cloneWithClause() {
        return withClause_ != null ? withClause_.clone() : null;
    }

    public OutFileClause cloneOutfileCluse() {
        return outFileClause != null ? outFileClause.clone() : null;
    }

    /**
     * C'tor for cloning.
     */
    protected QueryStmt(QueryStmt other) {
        super(other);
        withClause_ = other.cloneWithClause();
        outFileClause = other.cloneOutfileCluse();
        orderByElements = other.cloneOrderByElements();
        limitElement = other.limitElement.clone();
        resultExprs = Expr.cloneList(other.resultExprs);
        baseTblResultExprs = Expr.cloneList(other.baseTblResultExprs);
        aliasSMap = other.aliasSMap.clone();
        ambiguousAliasList = Expr.cloneList(other.ambiguousAliasList);
        sortInfo = (other.sortInfo != null) ? other.sortInfo.clone() : null;
        analyzer = other.analyzer;
        evaluateOrderBy = other.evaluateOrderBy;
    }

    @Override
    public void reset() {
        super.reset();
        if (orderByElements != null) {
            for (OrderByElement o : orderByElements)
                o.getExpr().reset();
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

    public void setFromInsert(boolean value) {
        this.fromInsert = value;
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
}
