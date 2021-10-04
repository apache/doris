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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.TableAliasGenerator;
import org.apache.doris.common.UserException;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Class representing a statement rewriter. A statement rewriter performs subquery
 * unnesting on an analyzed parse tree.
 */
public class StmtRewriter {
    private static final Logger LOG = LoggerFactory.getLogger(StmtRewriter.class);

    /**
     * Rewrite the statement of an analysis result. The unanalyzed rewritten
     * statement is returned.
     */
    public static StatementBase rewrite(Analyzer analyzer, StatementBase parsedStmt)
            throws AnalysisException {
        if (parsedStmt instanceof QueryStmt) {
            QueryStmt analyzedStmt = (QueryStmt) parsedStmt;
            Preconditions.checkNotNull(analyzedStmt.analyzer);
            return rewriteQueryStatement(analyzedStmt, analyzer);
        } else if (parsedStmt instanceof InsertStmt) {
            final InsertStmt insertStmt = (InsertStmt)parsedStmt;
            final QueryStmt analyzedStmt = (QueryStmt)insertStmt.getQueryStmt();
            Preconditions.checkNotNull(analyzedStmt.analyzer);
            QueryStmt rewrittenQueryStmt = rewriteQueryStatement(analyzedStmt, analyzer);
            insertStmt.setQueryStmt(rewrittenQueryStmt);
        } else {
            throw new AnalysisException("Unsupported statement containing subqueries: "
                    + parsedStmt.toSql());
        }
        return parsedStmt;
    }

  /**
   *  Calls the appropriate equal method based on the specific type of query stmt. See
   *  rewriteSelectStatement() and rewriteUnionStatement() documentation.
   */
    public static QueryStmt rewriteQueryStatement(QueryStmt stmt, Analyzer analyzer)
            throws AnalysisException {
        Preconditions.checkNotNull(stmt);
        if (stmt instanceof SelectStmt) {
            return rewriteSelectStatement((SelectStmt) stmt, analyzer);
        } else if (stmt instanceof SetOperationStmt) {
            rewriteUnionStatement((SetOperationStmt) stmt, analyzer);
        } else {
            throw new AnalysisException("Subqueries not supported for "
                    + stmt.getClass().getSimpleName() + " statements");
        }
        return stmt;
    }

    private static SelectStmt rewriteSelectStatement(SelectStmt stmt, Analyzer analyzer)
            throws AnalysisException {
        SelectStmt result = stmt;
        // Rewrite all the subqueries in the FROM clause.
        for (TableRef tblRef: result.fromClause_) {
            if (!(tblRef instanceof InlineViewRef)) continue;
            InlineViewRef inlineViewRef = (InlineViewRef)tblRef;
            QueryStmt rewrittenQueryStmt = rewriteQueryStatement(inlineViewRef.getViewStmt(),
                    inlineViewRef.getAnalyzer());
            inlineViewRef.setViewStmt(rewrittenQueryStmt);
        }
        // Rewrite all the subqueries in the WHERE clause.
        if (result.hasWhereClause()) {
            // Push negation to leaf operands.
            result.whereClause = Expr.pushNegationToOperands(result.whereClause);
            // Check if we can equal the subqueries in the WHERE clause. OR predicates with
            // subqueries are not supported.
            if (hasSubqueryInDisjunction(result.whereClause)) {
                throw new AnalysisException("Subqueries in OR predicates are not supported: "
                        + result.whereClause.toSql());
            }
            rewriteWhereClauseSubqueries(result, analyzer);
        }
        // Rewrite all subquery in the having clause
        if (result.getHavingClauseAfterAnaylzed() != null
                && result.getHavingClauseAfterAnaylzed().getSubquery() != null) {
            result = rewriteHavingClauseSubqueries(result, analyzer);
        }
        result.sqlString_ = null;
        if (LOG.isDebugEnabled()) {
            LOG.debug("rewritten stmt: " + result.toSql());
        }
        return result;
    }

    /**
     * Rewrite having subquery.
     * Step1: equal having subquery to where subquery
     * Step2: equal where subquery
     * <p>
     * For example:
     * select cs_item_sk, sum(cs_sales_price) from catalog_sales a group by cs_item_sk
     * having sum(cs_sales_price) >
     *        (select min(cs_sales_price) from catalog_sales b where a.cs_item_sk = b.cs_item_sk);
     * <p>
     * Step1: equal having subquery to where subquery
     * Outer query is changed to inline view in rewritten query
     * Inline view of outer query:
     *     from (select cs_item_sk, sum(cs_sales_price) sum_cs_sales_price from catalog_sales group by cs_item_sk) a
     * Rewritten subquery of expr:
     *     where a.sum_cs_sales_price >
     *           (select min(cs_sales_price) from catalog_sales b where a.cs_item_sk = b.cs_item_sk)
     * Rewritten query:
     *     select cs_item_sk, a.sum_cs_sales_price from
     *     (select cs_item_sk, sum(cs_sales_price) sum_cs_sales_price from catalog_sales group by cs_item_sk) a
     *     where a.sum_cs_sales_price >
     *           (select min(cs_sales_price) from catalog_sales b where a.cs_item_sk = b.cs_item_sk)
     * <p>
     * Step2: equal where subquery
     * Inline view of subquery:
     *     from (select b.cs_item_sk, min(cs_sales_price) from catalog_sales b group by cs_item_sk) c
     * Rewritten correlated predicate:
     *     where c.cs_item_sk = a.cs_item_sk and a.sum_cs_sales_price > c.min(cs_sales_price)
     * The final stmt:
     * select a.cs_item_sk, a.sum_cs_sales_price from
     *     (select cs_item_sk, sum(cs_sales_price) sum_cs_sales_price from catalog_sales group by cs_item_sk) a
     *     join
     *     (select b.cs_item_sk, min(b.cs_sales_price) min_cs_sales_price from catalog_sales b group by b.cs_item_sk) c
     * where c.cs_item_sk = a.cs_item_sk and a.sum_cs_sales_price > c.min_cs_sales_price;
     *
     * @param stmt
     * @param analyzer
     */
    private static SelectStmt rewriteHavingClauseSubqueries(SelectStmt stmt, Analyzer analyzer) throws AnalysisException {
        // prepare parameters
        SelectList selectList = stmt.getSelectList();
        List<String> columnLabels = stmt.getColLabels();
        Expr havingClause = stmt.getHavingClauseAfterAnaylzed();
        List<FunctionCallExpr> aggregateExprs = stmt.getAggInfo().getAggregateExprs();
        Preconditions.checkState(havingClause != null);
        Preconditions.checkState(havingClause.getSubquery() != null);
        List<OrderByElement> orderByElements = stmt.getOrderByElementsAfterAnalyzed();
        LimitElement limitElement = new LimitElement(stmt.getOffset(), stmt.getLimit());
        TableAliasGenerator tableAliasGenerator = stmt.getTableAliasGenerator();

        /*
         * The outer query is changed to inline view without having predicate
         * For example:
         * Query: select cs_item_sk, sum(cs_sales_price) from catalog_sales a group by cs_item_sk having ...;
         * Inline view:
         *     from (select cs_item_sk $ColumnA, sum(cs_sales_price) $ColumnB from catalog_sales a group by cs_item_sk) $TableA
         *
         * Add missing aggregation columns in select list
         * For example:
         * Query: select cs_item_sk from catalog_sales a group by cs_item_sk having sum(cs_sales_price) > 1
         * SelectList: select cs_item_sk
         * AggregateExprs:  sum(cs_sales_price)
         * Add missing aggregation columns: select cs_item_sk, sum(cs_sales_price)
         * Inline view:
         *     from (select cs_item_sk $ColumnA, sum(cs_sales_price) $ColumnB from catalog_sales a group by
         *     cs_item_sk) $TableA
         */
        SelectStmt inlineViewQuery = (SelectStmt) stmt.clone();
        inlineViewQuery.reset();
        // the having, order by and limit should be move to outer query
        inlineViewQuery.removeHavingClause();
        inlineViewQuery.removeOrderByElements();
        inlineViewQuery.removeLimitElement();
        // add missing aggregation columns
        SelectList selectListOfInlineViewQuery = addMissingAggregationColumns(selectList, aggregateExprs);
        inlineViewQuery.setSelectList(selectListOfInlineViewQuery);
        // add a new alias for all of columns in subquery
        List<String> colAliasOfInlineView = Lists.newArrayList();
        List<Expr> leftExprList = Lists.newArrayList();
        for (int i = 0; i < selectListOfInlineViewQuery.getItems().size(); ++i) {
            leftExprList.add(selectListOfInlineViewQuery.getItems().get(i).getExpr().clone());
            colAliasOfInlineView.add(inlineViewQuery.getColumnAliasGenerator().getNextAlias());
        }
        InlineViewRef inlineViewRef = new InlineViewRef(tableAliasGenerator.getNextAlias(), inlineViewQuery,
                colAliasOfInlineView);
        try {
            inlineViewRef.analyze(analyzer);
        } catch (UserException e) {
            throw new AnalysisException(e.getMessage());
        }
        LOG.debug("Outer query is changed to {}", inlineViewRef.tableRefToSql());

        /*
         * Columns which belong to outer query can substitute for output columns of inline view
         * For example:
         * Having clause: sum(cs_sales_price) >
         *                   (select min(cs_sales_price) from catalog_sales b where a.cs_item_sk = b.cs_item_sk);
         * Order by: sum(cs_sales_price), a.cs_item_sk
         * Select list: cs_item_sk, sum(cs_sales_price)
         * Columns which belong to outer query: sum(cs_sales_price), a.cs_item_sk
         * SMap: <cs_item_sk $ColumnA> <sum(cs_sales_price) $ColumnB>
         * After substitute
         * Having clause: $ColumnB >
         *                   (select min(cs_sales_price) from catalog_sales b where $ColumnA = b.cs_item_sk)
         * Order by: $ColumnB, $ColumnA
         * Select list: $ColumnA cs_item_sk, $ColumnB sum(cs_sales_price)
         */
        /*
         * Prepare select list of new query.
         * Generate a new select item for each original columns in select list
         */
        ExprSubstitutionMap smap = new ExprSubstitutionMap();
        List<SelectListItem> inlineViewItems = inlineViewQuery.getSelectList().getItems();
        for (int i = 0; i < inlineViewItems.size(); i++) {
            Expr leftExpr = leftExprList.get(i);
            Expr rightExpr = new SlotRef(inlineViewRef.getAliasAsName(), colAliasOfInlineView.get(i));
            rightExpr.analyze(analyzer);
            smap.put(leftExpr, rightExpr);
        }
        havingClause.reset();
        Expr newWherePredicate = havingClause.substitute(smap, analyzer, false);
        LOG.debug("Having predicate is changed to " + newWherePredicate.toSql());
        ArrayList<OrderByElement> newOrderByElements = null;
        if (orderByElements != null) {
            newOrderByElements = Lists.newArrayList();
            for (OrderByElement orderByElement : orderByElements) {
                OrderByElement newOrderByElement = new OrderByElement(orderByElement.getExpr().reset().substitute(smap),
                        orderByElement.getIsAsc(), orderByElement.getNullsFirstParam());
                newOrderByElements.add(newOrderByElement);
                LOG.debug("Order by element is changed to " + newOrderByElement.toSql());
            }
        }
        List<SelectListItem> newSelectItems = Lists.newArrayList();
        for (int i = 0; i < selectList.getItems().size(); i++) {
            SelectListItem newItem = new SelectListItem(selectList.getItems().get(i).getExpr().reset().substitute(smap),
                    columnLabels.get(i));
            newSelectItems.add(newItem);
            LOG.debug("New select item is changed to "+ newItem.toSql());
        }
        SelectList newSelectList = new SelectList(newSelectItems, selectList.isDistinct());

        // construct rewritten query
        List<TableRef> newTableRefList = Lists.newArrayList();
        newTableRefList.add(inlineViewRef);
        FromClause newFromClause = new FromClause(newTableRefList);
        SelectStmt result = new SelectStmt(newSelectList, newFromClause, newWherePredicate, null, null,
                newOrderByElements, limitElement);
        result.setTableAliasGenerator(tableAliasGenerator);
        try {
            result.analyze(analyzer);
        } catch (UserException e) {
            throw new AnalysisException(e.getMessage());
        }
        LOG.info("New stmt {} is constructed after rewritten subquery of having clause.", result.toSql());

        // equal where subquery
        result = rewriteSelectStatement(result, analyzer);
        LOG.debug("The final stmt is " + result.toSql());
        return result;
    }

    /**
     * Add missing aggregation columns
     *
     * @param selectList:     select a, sum(v1)
     * @param aggregateExprs: sum(v1), sum(v2)
     * @return select a, sum(v1), sum(v2)
     */
    private static SelectList addMissingAggregationColumns(SelectList selectList,
            List<FunctionCallExpr> aggregateExprs) {
        SelectList result = selectList.clone();
        for (FunctionCallExpr functionCallExpr : aggregateExprs) {
            boolean columnExists = false;
            for (SelectListItem selectListItem : selectList.getItems()) {
                if (selectListItem.getExpr().equals(functionCallExpr)) {
                    columnExists = true;
                    break;
                }
            }
            if (!columnExists) {
                SelectListItem selectListItem = new SelectListItem(functionCallExpr.clone().reset(), null);
                result.addItem(selectListItem);
            }
        }
        return result;
    }

    /**
     * Rewrite all operands in a UNION. The conditions that apply to SelectStmt rewriting
     * also apply here.
     */
    private static void rewriteUnionStatement(SetOperationStmt stmt, Analyzer analyzer)
            throws AnalysisException {
        for (SetOperationStmt.SetOperand operand: stmt.getOperands()) {
            Preconditions.checkState(operand.getQueryStmt() instanceof SelectStmt);
            QueryStmt rewrittenQueryStmt = StmtRewriter.rewriteSelectStatement(
                    (SelectStmt)operand.getQueryStmt(), operand.getAnalyzer());
            operand.setQueryStmt(rewrittenQueryStmt);
        }
    }

    /**
     * Returns true if the Expr tree rooted at 'expr' has at least one subquery
     * that participates in a disjunction.
     */
    private static boolean hasSubqueryInDisjunction(Expr expr) {
        if (!(expr instanceof CompoundPredicate)) {
            return false;
        };
        if (Expr.IS_OR_PREDICATE.apply(expr)) {
            return expr.contains(Subquery.class);
        }
        for (Expr child : expr.getChildren()) {
            if (hasSubqueryInDisjunction(child)) {
                return true;
            }
        }
        return false;
    }
    /**
     * Rewrite all subqueries of a stmt's WHERE clause. Initially, all the
     * conjuncts containing subqueries are extracted from the WHERE clause and are
     * replaced with true BoolLiterals. Subsequently, each extracted conjunct is
     * merged into its parent select block by converting it into a join.
     * Conjuncts with subqueries that themselves contain conjuncts with subqueries are
     * recursively rewritten in a bottom up fashion.
     *
     * The following example illustrates the bottom up rewriting of nested queries.
     * Suppose we have the following three level nested query Q0:
     *
     * SELECT *
     * FROM T1                                            : Q0
     * WHERE T1.a IN (SELECT a
     *                FROM T2 WHERE T2.b IN (SELECT b
     *                                       FROM T3))
     * AND T1.c < 10;
     *
     * This query will be rewritten as follows. Initially, the IN predicate
     * T1.a IN (SELECT a FROM T2 WHERE T2.b IN (SELECT b FROM T3)) is extracted
     * from the top level block (Q0) since it contains a subquery and is
     * replaced by a true BoolLiteral, resulting in the following query Q1:
     *
     * SELECT * FROM T1 WHERE TRUE : Q1
     *
     * Since the stmt in the extracted predicate contains a conjunct with a subquery,
     * it is also rewritten. As before, rewriting stmt SELECT a FROM T2
     * WHERE T2.b IN (SELECT b FROM T3) works by first extracting the conjunct that
     * contains the subquery (T2.b IN (SELECT b FROM T3)) and substituting it with
     * a true BoolLiteral, producing the following stmt Q2:
     *
     * SELECT a FROM T2 WHERE TRUE : Q2
     *
     * The predicate T2.b IN (SELECT b FROM T3) is then merged with Q2,
     * producing the following unnested query Q3:
     *
     * SELECT a FROM T2 LEFT SEMI JOIN (SELECT b FROM T3) $a$1 ON T2.b = $a$1.b : Q3
     *
     * The extracted IN predicate becomes:
     *
     * T1.a IN (SELECT a FROM T2 LEFT SEMI JOIN (SELECT b FROM T3) $a$1 ON T2.b = $a$1.b)
     *
     * Finally, the rewritten IN predicate is merged with query block Q1,
     * producing the following unnested query (WHERE clauses that contain only
     * conjunctions of true BoolLiterals are eliminated):
     *
     * SELECT *
     * FROM T1 LEFT SEMI JOIN (SELECT a
     *                         FROM T2 LEFT SEMI JOIN (SELECT b FROM T3) $a$1
     *                         ON T2.b = $a$1.b) $a$1
     * ON $a$1.a = T1.a
     * WHERE T1.c < 10;
     *
     */
    private static void rewriteWhereClauseSubqueries(
            SelectStmt stmt, Analyzer analyzer)
            throws AnalysisException {
        int numTableRefs = stmt.fromClause_.size();
        ArrayList<Expr> exprsWithSubqueries = Lists.newArrayList();
        ExprSubstitutionMap smap = new ExprSubstitutionMap();
        // Check if all the conjuncts in the WHERE clause that contain subqueries
        // can currently be rewritten as a join.
        for (Expr conjunct : stmt.whereClause.getConjuncts()) {
            List<Subquery> subqueries = Lists.newArrayList();
            conjunct.collectAll(Predicates.instanceOf(Subquery.class), subqueries);
            if (subqueries.size() == 0) {
                continue;
            }
            if (subqueries.size() > 1) {
                throw new AnalysisException("Multiple subqueries are not supported in "
                        + "expression: " + conjunct.toSql());
            }
            if (!(conjunct instanceof InPredicate)
                    && !(conjunct instanceof ExistsPredicate)
                    && !(conjunct instanceof BinaryPredicate)
                    && !conjunct.contains(Expr.IS_SCALAR_SUBQUERY)) {
                throw new AnalysisException("Non-scalar subquery is not supported in "
                        + "expression: "
                        + conjunct.toSql());
            }

            if (conjunct instanceof ExistsPredicate) {
                // Check if we can determine the result of an ExistsPredicate during analysis.
                // If so, replace the predicate with a BoolLiteral predicate and remove it from
                // the list of predicates to be rewritten.
                BoolLiteral boolLiteral = replaceExistsPredicate((ExistsPredicate) conjunct);
                if (boolLiteral != null) {
                    boolLiteral.analyze(analyzer);
                    smap.put(conjunct, boolLiteral);
                    continue;
                }
            }

            // Replace all the supported exprs with subqueries with true BoolLiterals
            // using an smap.
            BoolLiteral boolLiteral = new BoolLiteral(true);
            boolLiteral.analyze(analyzer);
            smap.put(conjunct, boolLiteral);
            exprsWithSubqueries.add(conjunct);
        }
        stmt.whereClause = stmt.whereClause.substitute(smap, analyzer, false);

        boolean hasNewVisibleTuple = false;
        // Recursively equal all the exprs that contain subqueries and merge them
        // with 'stmt'.
        for (Expr expr : exprsWithSubqueries) {
            if (mergeExpr(stmt, rewriteExpr(expr, analyzer), analyzer)) {
                hasNewVisibleTuple = true;
            }
        }
        if (canEliminate(stmt.whereClause)) {
            stmt.whereClause = null;
        }
        if (hasNewVisibleTuple) {
            replaceUnqualifiedStarItems(stmt, numTableRefs);
        }
    }


    /**
     * Replace an ExistsPredicate that contains a subquery with a BoolLiteral if we
     * can determine its result without evaluating it. Return null if the result of the
     * ExistsPredicate can only be determined at run-time.
     */
    private static BoolLiteral replaceExistsPredicate(ExistsPredicate predicate) {
        Subquery subquery = predicate.getSubquery();
        Preconditions.checkNotNull(subquery);
        SelectStmt subqueryStmt = (SelectStmt) subquery.getStatement();
        BoolLiteral boolLiteral = null;
        if (subqueryStmt.getAnalyzer().hasEmptyResultSet()) {
            boolLiteral = new BoolLiteral(predicate.isNotExists());
        } else if (subqueryStmt.hasAggInfo() && subqueryStmt.getAggInfo().hasAggregateExprs()
                && !subqueryStmt.hasAnalyticInfo() && subqueryStmt.getHavingPred() == null) {
            boolLiteral = new BoolLiteral(!predicate.isNotExists());
        }
        return boolLiteral;
    }

    /**
     * Return true if the Expr tree rooted at 'expr' can be safely
     * eliminated, i.e. it only consists of conjunctions of true BoolLiterals.
     */
    private static boolean canEliminate(Expr expr) {
        for (Expr conjunct : expr.getConjuncts()) {
            if (!Expr.IS_TRUE_LITERAL.apply(conjunct)) {
                return false;
            }
        }
        return true;
    }

    /*
     * Modifies in place an expr that contains a subquery by rewriting its
     * subquery stmt. The modified analyzed expr is returned.
     */
    private static Expr rewriteExpr(Expr expr, Analyzer analyzer)
    throws AnalysisException {
        // Extract the subquery and equal it.
        Subquery subquery = expr.getSubquery();
        Preconditions.checkNotNull(subquery);
        QueryStmt rewrittenStmt = rewriteSelectStatement((SelectStmt) subquery.getStatement(), subquery.getAnalyzer());
        // Create a new Subquery with the rewritten stmt and use a substitution map
        // to replace the original subquery from the expr.

        rewrittenStmt = rewrittenStmt.clone();
        rewrittenStmt.reset();
        Subquery newSubquery = new Subquery(rewrittenStmt);
        newSubquery.analyze(analyzer);
        
        ExprSubstitutionMap smap = new ExprSubstitutionMap();
        smap.put(subquery, newSubquery);
        return expr.substitute(smap, analyzer, false);
    }

    private static void canRewriteScalarFunction(Expr expr, Expr conjunct)
            throws AnalysisException {
        if (expr.getSubquery().isScalarSubquery()) {
            if (conjunct instanceof BinaryPredicate
                    && ((BinaryPredicate) conjunct).getOp() == BinaryPredicate.Operator.EQ) {
                return;
            }
            throw new AnalysisException("scalar subquery's correlatedPredicates's operator must be EQ");
        }
    }

    /**
     * origin stmt: select * from t1 where t1=(select k1 from t2);
     *
     * @param stmt     select * from t1 where true;
     * @param expr     t1=(select k1 from t2). The expr has already be rewritten.
     * @param analyzer
     * @return
     * @throws AnalysisException
     */
    private static boolean mergeExpr(SelectStmt stmt, Expr expr,
                                     Analyzer analyzer) throws AnalysisException {
        // LOG.warn("dhc mergeExpr stmt={} expr={}", stmt, expr);
        LOG.debug("SUBQUERY mergeExpr stmt={} expr={}", stmt.toSql(), expr.toSql());
        Preconditions.checkNotNull(expr);
        Preconditions.checkNotNull(analyzer);
        Preconditions.checkState(expr.getSubquery().getAnalyzer() != null,
            "subquery must be analyze address=" + System.identityHashCode(expr.getSubquery()));
        boolean updateSelectList = false;

        SelectStmt subqueryStmt = (SelectStmt) expr.getSubquery().getStatement();
        // Create a new inline view from the subquery stmt. The inline view will be added
        // to the stmt's table refs later. Explicitly set the inline view's column labels
        // to eliminate any chance that column aliases from the parent query could reference
        // select items from the inline view after the equal.
        List<String> colLabels = Lists.newArrayList();
        // add a new alias for all of columns in subquery
        for (int i = 0; i < subqueryStmt.getColLabels().size(); ++i) {
            colLabels.add(subqueryStmt.getColumnAliasGenerator().getNextAlias());
        }
        // (select k1 $a from t2) $b
        InlineViewRef inlineView = new InlineViewRef(
            stmt.getTableAliasGenerator().getNextAlias(), subqueryStmt, colLabels);

        // Extract all correlated predicates from the subquery.
        List<Expr> onClauseConjuncts = extractCorrelatedPredicates(subqueryStmt);
        if (!onClauseConjuncts.isEmpty()) {
            canRewriteCorrelatedSubquery(expr, onClauseConjuncts);
            // For correlated subqueries that are eligible for equal by transforming
            // into a join, a LIMIT clause has no effect on the results, so we can
            // safely remove it.
            // subqueryStmt.limitElement = null;
            subqueryStmt.limitElement = new LimitElement();
        }

        // Update the subquery's select list and/or its GROUP BY clause by adding
        // exprs from the extracted correlated predicates.
        boolean updateGroupBy = expr.getSubquery().isScalarSubquery()
                || (expr instanceof ExistsPredicate && subqueryStmt.hasAggInfo());
        // boolean updateGroupBy = false;
        List<Expr> lhsExprs = Lists.newArrayList();
        List<Expr> rhsExprs = Lists.newArrayList();

        for (Expr conjunct : onClauseConjuncts) {
            canRewriteScalarFunction(expr, conjunct);
            updateInlineView(inlineView, conjunct, stmt.getTableRefIds(),
                lhsExprs, rhsExprs, updateGroupBy);
        }

        /**
         * Situation: The expr is a uncorrelated subquery for outer stmt.
         * Rewrite: Add a limit 1 for subquery.
         * origin stmt: select * from t1 where exists (select * from table2);
         * expr: exists (select * from table2)
         * outer stmt: select * from t1
         * onClauseConjuncts: empty.
         */
        if (expr instanceof ExistsPredicate && onClauseConjuncts.isEmpty()) {
            // For uncorrelated subqueries, we limit the number of rows returned by the
            // subquery.
            subqueryStmt.setLimit(1);
        }

        // Analyzing the inline view trigger reanalysis of the subquery's select statement.
        // However the statement is already analyzed and since statement analysis is not
        // idempotent, the analysis needs to be reset (by a call to clone()).
        // inlineView = (InlineViewRef) inlineView.clone();
        inlineView.reset();
        try {
            inlineView.analyze(analyzer);
        } catch (UserException e) {
            throw new AnalysisException(e.getMessage());
        }
        inlineView.setLeftTblRef(stmt.fromClause_.get(stmt.fromClause_.size() - 1));

        stmt.fromClause_.add(inlineView);
        JoinOperator joinOp = JoinOperator.LEFT_SEMI_JOIN;

        // Create a join conjunct from the expr that contains a subquery.
        Expr joinConjunct = createJoinConjunct(expr, inlineView, analyzer,
            !onClauseConjuncts.isEmpty());
        if (joinConjunct != null) {
            SelectListItem firstItem =
              ((SelectStmt) inlineView.getViewStmt()).getSelectList().getItems().get(0);
            if (!onClauseConjuncts.isEmpty()
                    && firstItem.getExpr().contains(Expr.NON_NULL_EMPTY_AGG)) {
                // Correlated subqueries with an aggregate function that returns non-null on
                // an empty input are rewritten using a LEFT OUTER JOIN because we
                // need to ensure that there is one agg value for every tuple of 'stmt'
                // (parent select block), even for those tuples of 'stmt' that get rejected
                // by the subquery due to some predicate. The new join conjunct is added to
                // stmt's WHERE clause because it needs to be applied to the result of the
                // LEFT OUTER JOIN (both matched and unmatched tuples).
                stmt.whereClause =
                    CompoundPredicate.createConjunction(joinConjunct, stmt.whereClause);
                joinConjunct = null;
                joinOp = JoinOperator.LEFT_OUTER_JOIN;
                updateSelectList = true;
            }

            if (joinConjunct != null) {
                onClauseConjuncts.add(joinConjunct);
            }
        }

        // Create the ON clause from the extracted correlated predicates.
        Expr onClausePredicate =
            CompoundPredicate.createConjunctivePredicate(onClauseConjuncts);
        if (onClausePredicate == null) {
            Preconditions.checkState(expr instanceof ExistsPredicate);
            if (((ExistsPredicate) expr).isNotExists()) {
                throw new AnalysisException("Unsupported uncorrelated NOT EXISTS subquery: "
                        + subqueryStmt.toSql());
            }
            // We don't have an ON clause predicate to create an equi-join. Rewrite the
            // subquery using a CROSS JOIN.
            // TODO This is very expensive. Remove it when we implement independent
            // subquery evaluation.
            inlineView.setJoinOp(JoinOperator.CROSS_JOIN);
            LOG.warn("uncorrelated subquery rewritten using a cross join");
            // Indicate that new visible tuples may be added in stmt's select list.
            return true;
        }

        // Create an smap from the original select-list exprs of the select list to
        // the corresponding inline-view columns.
        ExprSubstitutionMap smap = new ExprSubstitutionMap();
        Preconditions.checkState(lhsExprs.size() == rhsExprs.size());
        for (int i = 0; i < lhsExprs.size(); ++i) {
            Expr lhsExpr = lhsExprs.get(i);
            Expr rhsExpr = rhsExprs.get(i);
            rhsExpr.analyze(analyzer);
            smap.put(lhsExpr, rhsExpr);
        }

        onClausePredicate = onClausePredicate.substitute(smap, analyzer, false);

        // Check for references to ancestor query blocks (cycles in the dependency
        // graph of query blocks are not supported).
        if (!onClausePredicate.isBoundByTupleIds(stmt.getTableRefIds())) {
            throw new AnalysisException("Unsupported correlated subquery: "
                    + subqueryStmt.toSql());
        }

        // Check if we have a valid ON clause for an equi-join.
        boolean hasEqJoinPred = false;
        for (Expr conjunct : onClausePredicate.getConjuncts()) {
            if (!(conjunct instanceof BinaryPredicate)) continue;
            BinaryPredicate.Operator operator = ((BinaryPredicate) conjunct).getOp();
            if (!operator.isEquivalence()) continue;
            List<TupleId> lhsTupleIds = Lists.newArrayList();
            conjunct.getChild(0).getIds(lhsTupleIds, null);
            if (lhsTupleIds.isEmpty()) {
                continue;
            }
            List<TupleId> rhsTupleIds = Lists.newArrayList();
            conjunct.getChild(1).getIds(rhsTupleIds, null);
            if (rhsTupleIds.isEmpty()) {
                continue;
            }
            // Check if columns from the outer query block (stmt) appear in both sides
            // of the binary predicate.
            if ((lhsTupleIds.contains(inlineView.getDesc().getId()) && lhsTupleIds.size() > 1)
                    || (rhsTupleIds.contains(inlineView.getDesc().getId())
                    && rhsTupleIds.size() > 1)) {
                continue;
            }
            hasEqJoinPred = true;
            break;
        }

        if (!hasEqJoinPred && !inlineView.isCorrelated()) {
            // TODO: Remove this when independent subquery evaluation is implemented.
            // TODO: Requires support for non-equi joins.
            boolean hasGroupBy = ((SelectStmt) inlineView.getViewStmt()).hasGroupByClause();
            // boolean hasGroupBy = false;
            if (!expr.getSubquery().returnsScalarColumn()) {
                throw new AnalysisException("Unsupported predicate with subquery: "
                        + expr.toSql());
            }

            // TODO: Requires support for null-aware anti-join mode in nested-loop joins
            if (expr.getSubquery().isScalarSubquery() && expr instanceof InPredicate
                    && ((InPredicate) expr).isNotIn()) {
                throw new AnalysisException("Unsupported NOT IN predicate with subquery: " +
                        expr.toSql());
            }

            // We can equal the aggregate subquery using a cross join. All conjuncts
            // that were extracted from the subquery are added to stmt's WHERE clause.
            stmt.whereClause =
                CompoundPredicate.createConjunction(onClausePredicate, stmt.whereClause);
            inlineView.setJoinOp(JoinOperator.CROSS_JOIN);
            // Indicate that the CROSS JOIN may add a new visible tuple to stmt's
            // select list (if the latter contains an unqualified star item '*')
            return true;
        }

        // We have a valid equi-join conjunct.
        if (expr instanceof InPredicate
                    && ((InPredicate) expr).isNotIn()
                    || expr instanceof ExistsPredicate
                    && ((ExistsPredicate) expr).isNotExists()) {
            // For the case of a NOT IN with an eq join conjunct, replace the join
            // conjunct with a conjunct that uses the null-matching eq operator.
            if (expr instanceof InPredicate) {
                // joinOp = JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
                joinOp = JoinOperator.LEFT_ANTI_JOIN;
                List<TupleId> tIds = Lists.newArrayList();
                joinConjunct.getIds(tIds, null);
                if (tIds.size() <= 1 || !tIds.contains(inlineView.getDesc().getId())) {
                    throw new AnalysisException("Unsupported NOT IN predicate with subquery: "
                            + expr.toSql());
                }
                // Replace the EQ operator in the generated join conjunct with a
                // null-matching EQ operator.
                for (Expr conjunct : onClausePredicate.getConjuncts()) {
                    if (conjunct.equals(joinConjunct)) {
                        Preconditions.checkState(conjunct instanceof BinaryPredicate);
                        Preconditions.checkState(((BinaryPredicate) conjunct).getOp()
                                == BinaryPredicate.Operator.EQ);
                        // ((BinaryPredicate)conjunct).setOp(BinaryPredicate.Operator.NULL_MATCHING_EQ);
                        break;
                    }
                }
            } else {
                joinOp = JoinOperator.LEFT_ANTI_JOIN;
            }
        }

        inlineView.setJoinOp(joinOp);
        inlineView.setOnClause(onClausePredicate);
        return updateSelectList;
    }

    /**
     * Replace all unqualified star exprs ('*') from stmt's select list with qualified
     * ones, i.e. tbl_1.*,...,tbl_n.*, where tbl_1,...,tbl_n are the visible tablerefs
     * in stmt. 'tableIndx' indicates the maximum tableRef ordinal to consider when
     * replacing an unqualified star item.
     */
    private static void replaceUnqualifiedStarItems(SelectStmt stmt, int tableIdx) {
        Preconditions.checkState(tableIdx < stmt.fromClause_.size());
        ArrayList<SelectListItem> newItems = Lists.newArrayList();
        for (int i = 0; i < stmt.selectList.getItems().size(); ++i) {
            SelectListItem item = stmt.selectList.getItems().get(i);
            if (!item.isStar() || item.getTblName() != null) {
                newItems.add(item);
                continue;
            }
            // '*' needs to be replaced by tbl1.*,...,tbln.*, where
            // tbl1,...,tbln are the visible tableRefs in stmt.
            for (int j = 0; j < tableIdx; ++j) {
                TableRef tableRef = stmt.fromClause_.get(j);
                if (tableRef.getJoinOp() == JoinOperator.LEFT_SEMI_JOIN ||
                        tableRef.getJoinOp() == JoinOperator.LEFT_ANTI_JOIN) {
                    continue;
                }
                newItems.add(SelectListItem.createStarItem(tableRef.getAliasAsName()));
            }
        }
        Preconditions.checkState(!newItems.isEmpty());
        boolean isDistinct = stmt.selectList.isDistinct();
        stmt.selectList = new SelectList(newItems, isDistinct);
    }

    /**
     * Return true if the expr tree rooted at 'root' contains a correlated
     * predicate.
     */
    private static boolean containsCorrelatedPredicate(Expr root, List<TupleId> tupleIds) {
        if (isCorrelatedPredicate(root, tupleIds)) {
            return true;
        }
        for (Expr child : root.getChildren()) {
            if (containsCorrelatedPredicate(child, tupleIds)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if 'expr' is a correlated predicate. A predicate is
     * correlated if at least one of its SlotRefs belongs to an ancestor
     * query block (i.e. is not bound by the given 'tupleIds').
     */
    private static boolean isCorrelatedPredicate(Expr expr, List<TupleId> tupleIds) {
        return (expr instanceof BinaryPredicate || expr instanceof SlotRef) && !expr.isBoundByTupleIds(tupleIds);
    }

    /**
     * Extract all correlated predicates of a subquery.
     *
     * TODO Handle correlated predicates in a HAVING clause.
     */
    private static ArrayList<Expr> extractCorrelatedPredicates(SelectStmt subqueryStmt)
            throws AnalysisException {
        List<TupleId> subqueryTupleIds = subqueryStmt.getTableRefIds();
        ArrayList<Expr> correlatedPredicates = Lists.newArrayList();

        if (subqueryStmt.hasWhereClause()) {
            if (!canExtractCorrelatedPredicates(subqueryStmt.getWhereClause(),
                subqueryTupleIds)) {
                throw new AnalysisException("Disjunctions with correlated predicates "
                        + "are not supported: " + subqueryStmt.getWhereClause().toSql());
            }
            // Extract the correlated predicates from the subquery's WHERE clause and
            // replace them with true BoolLiterals.
            Expr newWhereClause = extractCorrelatedPredicates(subqueryStmt.getWhereClause(),
                subqueryTupleIds, correlatedPredicates);
            if (canEliminate(newWhereClause)) {
                newWhereClause = null;
            }
            subqueryStmt.setWhereClause(newWhereClause);
        }

        // Process all correlated predicates from subquery's ON clauses.
        for (TableRef tableRef : subqueryStmt.getTableRefs()) {
            if (tableRef.getOnClause() == null) {
                continue;
            }

            ArrayList<Expr> onClauseCorrelatedPreds = Lists.newArrayList();
            Expr newOnClause = extractCorrelatedPredicates(tableRef.getOnClause(),
                subqueryTupleIds, onClauseCorrelatedPreds);
            if (onClauseCorrelatedPreds.isEmpty()) {
                continue;
            }

            correlatedPredicates.addAll(onClauseCorrelatedPreds);
            if (canEliminate(newOnClause)) {
                // After the extraction of correlated predicates from an ON clause,
                // the latter may only contain conjunctions of True BoolLiterals. In
                // this case, we can eliminate the ON clause and set the join type to
                // CROSS JOIN.
                tableRef.setJoinOp(JoinOperator.CROSS_JOIN);
                tableRef.setOnClause(null);
            } else {
                tableRef.setOnClause(newOnClause);
            }
        }
        return correlatedPredicates;
    }

    /**
     * Extract all correlated predicates from the expr tree rooted at 'root' and
     * replace them with true BoolLiterals. The modified expr tree is returned
     * and the extracted correlated predicates are added to 'matches'.
     */
    private static Expr extractCorrelatedPredicates(Expr root, List<TupleId> tupleIds,
        ArrayList<Expr> matches) {
        if (isCorrelatedPredicate(root, tupleIds)) {
            matches.add(root);
            return new BoolLiteral(true);
        }
        for (int i = 0; i < root.getChildren().size(); ++i) {
            root.getChildren().set(i, extractCorrelatedPredicates(root.getChild(i), tupleIds,
                matches));
        }
        return root;
    }

    /**
     * Returns true if we can extract the correlated predicates from 'expr'. A
     * correlated predicate cannot be extracted if it is part of a disjunction.
     */
    private static boolean canExtractCorrelatedPredicates(Expr expr,
            List<TupleId> subqueryTupleIds) {
        if (!(expr instanceof CompoundPredicate)) {
            return true;
        }
        if (Expr.IS_OR_PREDICATE.apply(expr)) {
            return !containsCorrelatedPredicate(expr, subqueryTupleIds);
        }
        for (Expr child : expr.getChildren()) {
            if (!canExtractCorrelatedPredicates(child, subqueryTupleIds)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if an expr containing a correlated subquery is eligible for equal by
     * tranforming into a join. 'correlatedPredicates' contains the correlated
     * predicates identified in the subquery. Throws an AnalysisException if 'expr'
     * is not eligible for equal.
     * TODO: Merge all the equal eligibility tests into a single function.
     */
    private static void canRewriteCorrelatedSubquery(
            Expr expr, List<Expr> correlatedPredicates)
            throws AnalysisException {
        Preconditions.checkNotNull(expr);
        Preconditions.checkState(expr.contains(Subquery.class));
        SelectStmt stmt = (SelectStmt) expr.getSubquery().getStatement();
        Preconditions.checkNotNull(stmt);
        // Only specified function could be supported in correlated subquery of binary predicate.
        if (expr instanceof BinaryPredicate) {
            if (stmt.getSelectList().getItems().size() != 1) {
                throw new AnalysisException("The subquery only support one item in select clause");
            }
            SelectListItem item = stmt.getSelectList().getItems().get(0);
            if (!item.getExpr().contains(Expr.CORRELATED_SUBQUERY_SUPPORT_AGG_FN)) {
                throw new AnalysisException("The select item in correlated subquery of binary predicate should only "
                                                    + "be sum, min, max, avg and count. Current subquery:"
                                                    + stmt.toSql());
            }
        }
        // Grouping and/or aggregation (including analytic functions) is forbidden in correlated subquery of in
        // predicate.
        if (expr instanceof InPredicate && (stmt.hasAggInfo() || stmt.hasAnalyticInfo())) {
            LOG.warn("canRewriteCorrelatedSubquery fail, expr={} subquery={}", expr.toSql(), stmt.toSql());
            throw new AnalysisException("Unsupported correlated subquery with grouping "
                    + "and/or aggregation: "
                    + stmt.toSql());
        }

        final com.google.common.base.Predicate<Expr> isSingleSlotRef =
                new com.google.common.base.Predicate<Expr>() {
                    @Override
                    public boolean apply(Expr arg) { return arg.unwrapSlotRef(false) != null; }
                };

        // A HAVING clause is only allowed on correlated EXISTS subqueries with
        // correlated binary predicates of the form Slot = Slot (see IMPALA-2734)
        // TODO Handle binary predicates with IS NOT DISTINCT op
        if (expr instanceof ExistsPredicate && stmt.hasHavingClause()
                && !correlatedPredicates.isEmpty()
                && (!stmt.hasAggInfo()
                || !Iterables.all(correlatedPredicates,
                Predicates.or(Expr.IS_EQ_BINARY_PREDICATE, isSingleSlotRef)))) {
            throw new AnalysisException("Unsupported correlated EXISTS subquery with a " +
                    "HAVING clause: " + stmt.toSql());
        }

        // The following correlated subqueries with a limit clause are supported:
        // 1. EXISTS subqueries
        // 2. Scalar subqueries with aggregation
        if (stmt.hasLimitClause()
                && (!(expr instanceof BinaryPredicate) || !stmt.hasAggInfo()
                || stmt.selectList.isDistinct())
                && !(expr instanceof ExistsPredicate)) {
            throw new AnalysisException("Unsupported correlated subquery with a "
                    + "LIMIT clause: " + stmt.toSql());
        }
    }

    /**
     * Update the subquery within an inline view by expanding its select list with exprs
     * from a correlated predicate 'expr' that will be 'moved' to an ON clause in the
     * subquery's parent query block. We need to make sure that every expr extracted from
     * the subquery references an item in the subquery's select list. If 'updateGroupBy'
     * is true, the exprs extracted from 'expr' are also added in stmt's GROUP BY clause.
     * Throws an AnalysisException if we need to update the GROUP BY clause but
     * both the lhs and rhs of 'expr' reference a tuple of the subquery stmt.
     */
    private static void updateInlineView(
            InlineViewRef inlineView, Expr expr, List<TupleId> parentQueryTids,
            List<Expr> lhsExprs, List<Expr> rhsExprs, boolean updateGroupBy)
            throws AnalysisException {
        SelectStmt stmt = (SelectStmt) inlineView.getViewStmt();
        List<TupleId> subqueryTblIds = stmt.getTableRefIds();
        ArrayList<Expr> groupByExprs = null;
        if (updateGroupBy) {
            groupByExprs = Lists.newArrayList();
        }

        List<SelectListItem> items = stmt.selectList.getItems();
        // Collect all the SlotRefs from 'expr' and identify those that are bound by
        // subquery tuple ids.
        ArrayList<Expr> slotRefs = Lists.newArrayList();
        expr.collectAll(Predicates.instanceOf(SlotRef.class), slotRefs);
        List<Expr> exprsBoundBySubqueryTids = Lists.newArrayList();
        for (Expr slotRef : slotRefs) {
            if (slotRef.isBoundByTupleIds(subqueryTblIds)) {
                exprsBoundBySubqueryTids.add(slotRef);
            }
        }
        // The correlated predicate only references slots from a parent block,
        // no need to update the subquery's select or group by list.
        if (exprsBoundBySubqueryTids.isEmpty()) {
            return;
        }
        if (updateGroupBy) {
            Preconditions.checkState(expr instanceof BinaryPredicate);
            Expr exprBoundBySubqueryTids = null;
            if (exprsBoundBySubqueryTids.size() > 1) {
                // If the predicate contains multiple SlotRefs bound by subquery tuple
                // ids, they must all be on the same side of that predicate.
                if (expr.getChild(0).isBoundByTupleIds(subqueryTblIds)
                        && expr.getChild(1).isBoundByTupleIds(parentQueryTids)) {
                    exprBoundBySubqueryTids = expr.getChild(0);
                } else if (expr.getChild(0).isBoundByTupleIds(parentQueryTids)
                        && expr.getChild(1).isBoundByTupleIds(subqueryTblIds)) {
                    exprBoundBySubqueryTids = expr.getChild(1);
                } else {
                    throw new AnalysisException("All subquery columns "
                            + "that participate in a predicate must be on the same side of "
                            + "that predicate: " + expr.toSql());
                }
            } else {
                Preconditions.checkState(exprsBoundBySubqueryTids.size() == 1);
                exprBoundBySubqueryTids = exprsBoundBySubqueryTids.get(0);
            }
            exprsBoundBySubqueryTids.clear();
            exprsBoundBySubqueryTids.add(exprBoundBySubqueryTids);
        }

        // Add the exprs bound by subquery tuple ids to the select list and
        // register it for substitution. We use a temporary substitution map
        // because we cannot at this point analyze the new select list expr. Once
        // the new inline view is analyzed, the entries from this map will be
        // added to an ExprSubstitutionMap.
        for (Expr boundExpr : exprsBoundBySubqueryTids) {
            String colAlias = stmt.getColumnAliasGenerator().getNextAlias();
            items.add(new SelectListItem(boundExpr, null));
            inlineView.getExplicitColLabels().add(colAlias);
            lhsExprs.add(boundExpr);
            rhsExprs.add(new SlotRef(inlineView.getAliasAsName(), colAlias));
            if (groupByExprs != null) {
                groupByExprs.add(boundExpr);
            }
        }

        // Update the subquery's select list.
        boolean isDistinct = stmt.selectList.isDistinct();
        Preconditions.checkState(!isDistinct);
        stmt.selectList = new SelectList(items, isDistinct);
        // Update subquery's GROUP BY clause
        if (groupByExprs != null && !groupByExprs.isEmpty()) {
            if (stmt.hasGroupByClause()) {
                stmt.groupByClause.getGroupingExprs().addAll(groupByExprs);
                stmt.groupByClause.getOriGroupingExprs().addAll(groupByExprs);
            } else {
                stmt.groupByClause = new GroupByClause(groupByExprs, GroupByClause.GroupingType.GROUP_BY);
            }
        }
    }

    /**
     * Converts an expr containing a subquery into an analyzed conjunct to be
     * used in a join. The conversion is performed in place by replacing the
     * subquery with the first expr from the select list of 'inlineView'.
     * If 'isCorrelated' is true and the first expr from the inline view contains
     * an aggregate function that returns non-null on an empty input,
     * the aggregate function is wrapped into a 'zeroifnull' function.
     */
    private static Expr createJoinConjunct(Expr exprWithSubquery, InlineViewRef inlineView,
            Analyzer analyzer, boolean isCorrelated) throws AnalysisException {
        Preconditions.checkNotNull(exprWithSubquery);
        Preconditions.checkNotNull(inlineView);
        Preconditions.checkState(exprWithSubquery.contains(Subquery.class));
        if (exprWithSubquery instanceof ExistsPredicate) {
            return null;
        }
        // Create a SlotRef from the first item of inlineView's select list
        SlotRef slotRef = new SlotRef(new TableName(null, inlineView.getAlias()),
                inlineView.getColLabels().get(0));
        slotRef.analyze(analyzer);
        Expr subquerySubstitute = slotRef;
        if (exprWithSubquery instanceof InPredicate) {
            BinaryPredicate pred = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                    exprWithSubquery.getChild(0), slotRef);
            pred.analyze(analyzer);
            return pred;
        }

        Subquery subquery = exprWithSubquery.getSubquery();
        ExprSubstitutionMap smap = new ExprSubstitutionMap();
        SelectListItem item =
                ((SelectStmt) inlineView.getViewStmt()).getSelectList().getItems().get(0);
        if (isCorrelated && item.getExpr().contains(Expr.NON_NULL_EMPTY_AGG)) {
            // TODO: Add support for multiple agg functions that return non-null on an
            // empty input, by wrapping them with zeroifnull functions before the inline
            // view is analyzed.
            if (!Expr.NON_NULL_EMPTY_AGG.apply(item.getExpr())
                    && (!(item.getExpr() instanceof CastExpr)
                    || !Expr.NON_NULL_EMPTY_AGG.apply(item.getExpr().getChild(0)))) {
                throw new AnalysisException("Aggregate function that returns non-null on "
                        + "an empty input cannot be used in an expression in a "
                        + "correlated subquery's select list: " + subquery.toSql());
            }

            List<Expr> aggFns = Lists.newArrayList();
            item.getExpr().collectAll(Expr.NON_NULL_EMPTY_AGG, aggFns);
            // TODO Generalize this by making the aggregate functions aware of the
            // literal expr that they return on empty input, e.g. max returns a
            // NullLiteral whereas count returns a NumericLiteral.
            if (((FunctionCallExpr) aggFns.get(0)).getFn().getReturnType().isNumericType()) {
                FunctionCallExpr zeroIfNull = new FunctionCallExpr("ifnull",
                Lists.newArrayList((Expr) slotRef, new IntLiteral(0, Type.BIGINT)));
                zeroIfNull.analyze(analyzer);
                subquerySubstitute = zeroIfNull;
            } else if (((FunctionCallExpr) aggFns.get(0)).getFn().getReturnType().isStringType()) {
                List<Expr> params = Lists.newArrayList();
                params.add(slotRef);
                params.add(new StringLiteral(""));
                FunctionCallExpr ifnull = new FunctionCallExpr("ifnull", params);
                ifnull.analyze(analyzer);
                subquerySubstitute = ifnull;
            } else {
                throw new AnalysisException("Unsupported aggregate function used in "
                        + "a correlated subquery's select list: " + subquery.toSql());
            }
        }
        smap.put(subquery, subquerySubstitute);
        return exprWithSubquery.substitute(smap, analyzer, false);
    }
}

