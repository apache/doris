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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.rewrite.ExprRewriter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Representation of a set ops with its list of operands, and optional order by and limit.
 * A set ops materializes its results, and its resultExprs are SlotRefs into a new
 * materialized tuple.
 * During analysis, the operands are normalized (separated into a single sequence of
 * DISTINCT followed by a single sequence of ALL operands) and unnested to the extent
 * possible. This also creates the AggregationInfo for DISTINCT operands.
 *
 * Use of resultExprs vs. baseTblResultExprs:
 * We consistently use/cast the resultExprs of set operands because the final expr
 * substitution happens during planning. The only place where baseTblResultExprs are
 * used is in materializeRequiredSlots() because that is called before plan generation
 * and we need to mark the slots of resolved exprs as materialized.
 */
public class SetOperationStmt extends QueryStmt {
    private final static Logger LOG = LogManager.getLogger(SetOperationStmt.class);

    public enum Operation {
        UNION,
        INTERSECT,
        EXCEPT
    }

    public enum Qualifier {
        ALL,
        DISTINCT
    }

    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    // before analysis, this contains the list of set operands derived verbatim
    // from the query;
    // after analysis, this contains all of distinctOperands followed by allOperands
    private final List<SetOperand> operands;

    // filled during analyze(); contains all operands that need to go through
    // distinct aggregation
    protected final List<SetOperand> distinctOperands_ = Lists.newArrayList();

    // filled during analyze(); contains all operands that can be aggregated with
    // a simple merge without duplicate elimination (also needs to merge the output
    // of the DISTINCT operands)
    protected final List<SetOperand> allOperands_ = Lists.newArrayList();

    private AggregateInfo distinctAggInfo;  // only set if we have DISTINCT ops

    private boolean hasDistinct = false;

    // Single tuple materialized by the set operation. Set in analyze().
    private TupleId tupleId;

    // set prior to unnesting
    private String toSqlString;

    // true if any of the operands_ references an AnalyticExpr
    private boolean hasAnalyticExprs_ = false;

    // List of output expressions produced by the set operation without the ORDER BY portion
    // (if any). Same as resultExprs_ if there is no ORDER BY.
    private List<Expr> setOpsResultExprs_ = Lists.newArrayList();

    // END: Members that need to be reset()
    /////////////////////////////////////////

    public SetOperationStmt(
            List<SetOperand> operands,
            ArrayList<OrderByElement> orderByElements,
            LimitElement limitElement) {
        super(orderByElements, limitElement);
        this.operands = operands;
    }

    /**
     * C'tor for cloning.
     */
    protected SetOperationStmt(SetOperationStmt other) {
        super(other.cloneOrderByElements(),
                (other.limitElement == null) ? null : other.limitElement.clone());
        operands = Lists.newArrayList();
        if (analyzer != null) {
            for (SetOperand o: other.distinctOperands_) distinctOperands_.add(o.clone());
            for (SetOperand o: other.allOperands_) allOperands_.add(o.clone());
            operands.addAll(distinctOperands_);
            operands.addAll(allOperands_);
        } else {
            for (SetOperand operand: other.operands) operands.add(operand.clone());
        }
        analyzer = other.analyzer;
        distinctAggInfo =
                (other.distinctAggInfo != null) ? other.distinctAggInfo.clone() : null;
        tupleId = other.tupleId;
        toSqlString = (other.toSqlString != null) ? new String(other.toSqlString) : null;
        hasAnalyticExprs_ = other.hasAnalyticExprs_;
        withClause_ = (other.withClause_ != null) ? other.withClause_.clone() : null;
        setOpsResultExprs_ = Expr.cloneList(other.setOpsResultExprs_);
    }

    @Override
    public SetOperationStmt clone() { return new SetOperationStmt(this); }

    /**
     * Undoes all changes made by analyze() except distinct propagation and unnesting.
     * After analysis, operands_ contains the list of unnested operands with qualifiers
     * adjusted to reflect distinct propagation. Every operand in that list is reset().
     * The distinctOperands_ and allOperands_ are cleared because they are redundant
     * with operands_.
     */
    @Override
    public void reset() {
        super.reset();
        for (SetOperand op: operands) op.reset();
        distinctOperands_.clear();
        allOperands_.clear();
        distinctAggInfo = null;
        tupleId = null;
        toSqlString = null;
        hasAnalyticExprs_ = false;
        setOpsResultExprs_.clear();
    }

    public List<SetOperand> getOperands() { return operands; }
    public List<SetOperand> getDistinctOperands() { return distinctOperands_; }
    public boolean hasDistinctOps() { return !distinctOperands_.isEmpty(); }
    public List<SetOperand> getAllOperands() { return allOperands_; }
    public boolean hasAllOps() { return !allOperands_.isEmpty(); }
    public AggregateInfo getDistinctAggInfo() { return distinctAggInfo; }
    public boolean hasAnalyticExprs() { return hasAnalyticExprs_; }
    public TupleId getTupleId() { return tupleId; }

    public void removeAllOperands() {
        operands.removeAll(allOperands_);
        allOperands_.clear();
    }

    public List<Expr> getSetOpsResultExprs() { return setOpsResultExprs_; }

    @Override
    public void getDbs(Analyzer analyzer, Map<String, Database> dbs, Set<String> parentViewNameSet) throws AnalysisException {
        getWithClauseDbs(analyzer, dbs, parentViewNameSet);
        for (SetOperand op : operands) {
            op.getQueryStmt().getDbs(analyzer, dbs, parentViewNameSet);
        }
    }

    /**
     * Propagates DISTINCT from left to right, and checks that all
     * set operands are set compatible, adding implicit casts if necessary.
     */
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (isAnalyzed()) return;
        super.analyze(analyzer);
        Preconditions.checkState(operands.size() > 0);

        // the first operand's operation usually null
        if (operands.get(0).operation == null && operands.size() > 1) {
            operands.get(0).setOperation(operands.get(1).getOperation());
        }

        // Propagates DISTINCT from left to right,
        propagateDistinct();

        // Analyze all operands and make sure they return an equal number of exprs.
        analyzeOperands(analyzer);

        // Remember the SQL string before unnesting operands.
        if (needToSql) {
            toSqlString = toSql();
        }

        // Unnest the operands before casting the result exprs. Unnesting may add
        // additional entries to operands_ and the result exprs of those unnested
        // operands must also be cast properly.
        unnestOperands(analyzer);

        // Compute hasAnalyticExprs_
        hasAnalyticExprs_ = false;
        for (SetOperand op: operands) {
            if (op.hasAnalyticExprs()) {
                hasAnalyticExprs_ = true;
                break;
            }
        }

        // Collect all result expr lists and cast the exprs as necessary.
        List<List<Expr>> resultExprLists = Lists.newArrayList();
        for (SetOperand op: operands) {
            resultExprLists.add(op.getQueryStmt().getResultExprs());
        }
        analyzer.castToSetOpsCompatibleTypes(resultExprLists);

        // Create tuple descriptor materialized by this SetOperationStmt, its resultExprs, and
        // its sortInfo if necessary.
        createMetadata(analyzer);
        createSortInfo(analyzer);

        // Create unnested operands' smaps.
        for (SetOperand operand: operands) setOperandSmap(operand, analyzer);

        // Create distinctAggInfo, if necessary.
        if (!distinctOperands_.isEmpty()) {
            // Aggregate produces exactly the same tuple as the original setOp stmt.
            ArrayList<Expr> groupingExprs = Expr.cloneList(resultExprs);
            try {
                distinctAggInfo = AggregateInfo.create(
                        groupingExprs, null, analyzer.getDescTbl().getTupleDesc(tupleId), analyzer);
            } catch (AnalysisException e) {
                // Should never happen.
                throw new IllegalStateException("Error creating agg info in SetOperationStmt.analyze()", e);
            }
        }

        setOpsResultExprs_ = Expr.cloneList(resultExprs);
        if (evaluateOrderBy) createSortTupleInfo(analyzer);
        baseTblResultExprs = resultExprs;
    }

    /**
     * Analyzes all operands and checks that they return an equal number of exprs.
     * Throws an AnalysisException if that is not the case, or if analyzing
     * an operand fails.
     */
    private void analyzeOperands(Analyzer analyzer) throws AnalysisException, UserException {
        for (int i = 0; i < operands.size(); ++i) {
            operands.get(i).analyze(analyzer);
            QueryStmt firstQuery = operands.get(0).getQueryStmt();
            List<Expr> firstExprs = operands.get(0).getQueryStmt().getResultExprs();
            QueryStmt query = operands.get(i).getQueryStmt();
            List<Expr> exprs = query.getResultExprs();
            if (firstExprs.size() != exprs.size()) {
                throw new AnalysisException("Operands have unequal number of columns:\n" +
                        "'" + queryStmtToSql(firstQuery) + "' has " +
                        firstExprs.size() + " column(s)\n" +
                        "'" + queryStmtToSql(query) + "' has " + exprs.size() + " column(s)");
            }
        }
    }

    /**
     * Fill distinct-/allOperands and performs possible unnesting of SetOperationStmt
     * operands in the process.
     */
    private void unnestOperands(Analyzer analyzer) throws AnalysisException {
        if (operands.size() == 1) {
            // ValuesStmt for a single row.
            allOperands_.add(operands.get(0));
            return;
        }
        // find index of first ALL operand
        int firstAllIdx = operands.size();
        for (int i = 1; i < operands.size(); ++i) {
            SetOperand operand = operands.get(i);
            if (operand.getQualifier() == Qualifier.ALL) {
                firstAllIdx = (i == 1 ? 0 : i);
                break;
            }
        }
        // operands[0] is always implicitly ALL, so operands[1] can't be the
        // first one
        Preconditions.checkState(firstAllIdx != 1);

        // unnest DISTINCT operands
        Preconditions.checkState(distinctOperands_.isEmpty());
        for (int i = 0; i < firstAllIdx; ++i) {
            unnestOperand(distinctOperands_, Qualifier.DISTINCT, operands.get(i));
        }

        // unnest ALL operands
        Preconditions.checkState(allOperands_.isEmpty());
        for (int i = firstAllIdx; i < operands.size(); ++i) {
            unnestOperand(allOperands_, Qualifier.ALL, operands.get(i));
        }

        for (SetOperand op: distinctOperands_) op.setQualifier(Qualifier.DISTINCT);
        for (SetOperand op: allOperands_) op.setQualifier(Qualifier.ALL);

        operands.clear();
        operands.addAll(distinctOperands_);
        operands.addAll(allOperands_);
    }

    /**
     * Add a single operand to the target list; if the operand itself is a SetOperationStmt, apply
     * unnesting to the extent possible (possibly modifying 'operand' in the process).
     */
    private void unnestOperand(
            List<SetOperand> target, Qualifier targetQualifier, SetOperand operand) {
        Preconditions.checkState(operand.isAnalyzed());
        QueryStmt queryStmt = operand.getQueryStmt();
        if (queryStmt instanceof SelectStmt) {
            target.add(operand);
            return;
        }

        Preconditions.checkState(queryStmt instanceof SetOperationStmt);
        SetOperationStmt setOperationStmt = (SetOperationStmt) queryStmt;
        boolean mixed = false;
        if (operand.getOperation() != null) {
            for (int i = 1; i < setOperationStmt.operands.size(); ++i) {
                if (operand.getOperation() != setOperationStmt.operands.get(i).getOperation()) {
                    mixed = true;
                    break;
                }
            }
        }
        if (setOperationStmt.hasLimit() || setOperationStmt.hasOffset() || mixed) {
            // we must preserve the nested SetOps
            target.add(operand);
        } else if (targetQualifier == Qualifier.DISTINCT || !setOperationStmt.hasDistinctOps()) {
            // there is no limit in the nested SetOps and we can absorb all of its
            // operands as-is
            target.addAll(setOperationStmt.getDistinctOperands());
            target.addAll(setOperationStmt.getAllOperands());
        } else {
            // the nested SetOps contains some Distinct ops and we're accumulating
            // into our All ops; unnest only the All ops and leave the rest in place
            target.addAll(setOperationStmt.getAllOperands());
            setOperationStmt.removeAllOperands();
            target.add(operand);
        }
    }

    /**
     * Sets the smap for the given operand. It maps from the output slots this SetOps's
     * tuple to the corresponding result exprs of the operand.
     */
    private void setOperandSmap(SetOperand operand, Analyzer analyzer) {
        TupleDescriptor tupleDesc = analyzer.getDescTbl().getTupleDesc(tupleId);
        // operands' smaps were already set in the operands' analyze()
        operand.getSmap().clear();
        List<Expr> resultExprs = operand.getQueryStmt().getResultExprs();
        Preconditions.checkState(resultExprs.size() == tupleDesc.getSlots().size());
        for (int i = 0; i < tupleDesc.getSlots().size(); ++i) {
            SlotDescriptor outputSlot = tupleDesc.getSlots().get(i);
            // Map to the original (uncast) result expr of the operand.
            Expr origExpr = resultExprs.get(i).unwrapExpr(true).clone();
            operand.getSmap().put(new SlotRef(outputSlot), origExpr);
        }
    }

    /**
     * String representation of queryStmt used in reporting errors.
     * Allow subclasses to override this.
     */
    protected String queryStmtToSql(QueryStmt queryStmt) {
        return queryStmt.toSql();
    }

    /**
     * Propagates DISTINCT (if present) from right to left.
     * Implied associativity:
     * A UNION ALL B UNION DISTINCT C = (A UNION ALL B) UNION DISTINCT C
     * = A UNION DISTINCT B UNION DISTINCT C
     */
    private void propagateDistinct() {
        int firstDistinctPos = -1;
        for (int i = operands.size() - 1; i > 0; --i) {
            SetOperand operand = operands.get(i);
            if (firstDistinctPos != -1) {
                // There is a DISTINCT somewhere to the right.
                operand.setQualifier(Qualifier.DISTINCT);
            } else if (operand.getQualifier() == Qualifier.DISTINCT) {
                firstDistinctPos = i;
            }
        }
    }

    /**
     * Create a descriptor for the tuple materialized by the setOps.
     * Set resultExprs to be slot refs into that tuple.
     * Also fills the substitution map, such that "order by" can properly resolve
     * column references from the result of the setOps.
     */
    private void createMetadata(Analyzer analyzer) throws AnalysisException {
        // Create tuple descriptor for materialized tuple created by the setOps.
        TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor("SetOps");
        tupleDesc.setIsMaterialized(true);
        tupleId = tupleDesc.getId();
        if (LOG.isTraceEnabled()) {
            LOG.trace("SetOperationStmt.createMetadata: tupleId=" + tupleId.toString());
        }

        // One slot per expr in the select blocks. Use first select block as representative.
        List<Expr> firstSelectExprs = operands.get(0).getQueryStmt().getResultExprs();

        // TODO(zc) Column stats
        /*
        // Compute column stats for the materialized slots from the source exprs.
        List<ColumnStats> columnStats = Lists.newArrayList();
        for (int i = 0; i < operands_.size(); ++i) {
            List<Expr> selectExprs = operands_.get(i).getQueryStmt().getResultExprs();
            for (int j = 0; j < selectExprs.size(); ++j) {
                ColumnStats statsToAdd = ColumnStats.fromExpr(selectExprs.get(j));
                if (i == 0) {
                    columnStats.add(statsToAdd);
                } else {
                    columnStats.get(j).add(statsToAdd);
                }
            }
        }
        */

        // Create tuple descriptor and slots.
        for (int i = 0; i < firstSelectExprs.size(); ++i) {
            Expr expr = firstSelectExprs.get(i);
            SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
            slotDesc.setLabel(getColLabels().get(i));
            slotDesc.setType(expr.getType());
            // TODO(zc)
            // slotDesc.setStats(columnStats.get(i));
            SlotRef outputSlotRef = new SlotRef(slotDesc);
            resultExprs.add(outputSlotRef);

            // Add to aliasSMap so that column refs in "order by" can be resolved.
            if (orderByElements != null) {
                SlotRef aliasRef = new SlotRef(null, getColLabels().get(i));
                if (aliasSMap.containsMappingFor(aliasRef)) {
                    ambiguousAliasList.add(aliasRef);
                } else {
                    aliasSMap.put(aliasRef, outputSlotRef);
                }
            }

            boolean isNullable = false;
            // register single-directional value transfers from output slot
            // to operands' result exprs (if those happen to be slotrefs);
            // don't do that if the operand computes analytic exprs
            // (see Planner.createInlineViewPlan() for the reasoning)
            for (SetOperand op: operands) {
                Expr resultExpr = op.getQueryStmt().getResultExprs().get(i);
                slotDesc.addSourceExpr(resultExpr);
                SlotRef slotRef = resultExpr.unwrapSlotRef(false);
                if (slotRef == null || slotRef.getDesc().getIsNullable()
                        || analyzer.isOuterJoined(slotRef.getDesc().getParent().getId())) isNullable = true;
                if (op.hasAnalyticExprs()) continue;
                slotRef = resultExpr.unwrapSlotRef(true);
                if (slotRef == null) continue;
                // analyzer.registerValueTransfer(outputSlotRef.getSlotId(), slotRef.getSlotId());
            }
            // If all the child slots are not nullable, then the SetOps output slot should not
            // be nullable as well.
            slotDesc.setIsNullable(isNullable);
        }
        baseTblResultExprs = resultExprs;
    }

    /**
     * Marks the baseTblResultExprs of its operands as materialized, based on
     * which of the output slots have been marked.
     * Calls materializeRequiredSlots() on the operands themselves.
     */
    @Override
    public void materializeRequiredSlots(Analyzer analyzer) throws AnalysisException {
        TupleDescriptor tupleDesc = analyzer.getDescTbl().getTupleDesc(tupleId);
        // to keep things simple we materialize all grouping exprs = output slots,
        // regardless of what's being referenced externally
        if (!distinctOperands_.isEmpty()) tupleDesc.materializeSlots();

        if (evaluateOrderBy) sortInfo.materializeRequiredSlots(analyzer, null);

        // collect operands' result exprs
        List<SlotDescriptor> outputSlots = tupleDesc.getSlots();
        List<Expr> exprs = Lists.newArrayList();
        for (int i = 0; i < outputSlots.size(); ++i) {
            SlotDescriptor slotDesc = outputSlots.get(i);
            if (!slotDesc.isMaterialized()) continue;
            for (SetOperand op: operands) {
                exprs.add(op.getQueryStmt().getBaseTblResultExprs().get(i));
            }
            if (distinctAggInfo != null) {
                // also mark the corresponding slot in the distinct agg tuple as being
                // materialized
                distinctAggInfo.getOutputTupleDesc().getSlots().get(i).setIsMaterialized(true);
            }
        }
        materializeSlots(analyzer, exprs);

        for (SetOperand op: operands) {
            op.getQueryStmt().materializeRequiredSlots(analyzer);
        }
    }

    @Override
    public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
        for (SetOperand op: operands) op.getQueryStmt().rewriteExprs(rewriter);
        if (orderByElements != null) {
            for (OrderByElement orderByElem: orderByElements) {
                orderByElem.setExpr(rewriter.rewrite(orderByElem.getExpr(), analyzer));
            }
        }
    }

    @Override
    public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
        // Return the sort tuple if there is an evaluated order by.
        if (evaluateOrderBy) {
            tupleIdList.add(sortInfo.getSortTupleDescriptor().getId());
        } else {
            tupleIdList.add(tupleId);
        }
    }

    @Override
    public void collectTableRefs(List<TableRef> tblRefs) {
        for (SetOperand op: operands) op.getQueryStmt().collectTableRefs(tblRefs);
    }

    @Override
    public List<TupleId> collectTupleIds() {
        List<TupleId> result = Lists.newArrayList();
        for (SetOperand op: operands) result.addAll(op.getQueryStmt().collectTupleIds());
        return result;
    }

    @Override
    public String toSql() {
        if (toSqlString != null) {
            return toSqlString;
        }
        StringBuilder strBuilder = new StringBuilder();
        Preconditions.checkState(operands.size() > 0);
        strBuilder.append(operands.get(0).getQueryStmt().toSql());
        for (int i = 1; i < operands.size() - 1; ++i) {
            strBuilder.append(
              " " + operands.get(i).getOperation().toString() + " "
                      + ((operands.get(i).getQualifier() == Qualifier.ALL) ? "ALL " : ""));
            if (operands.get(i).getQueryStmt() instanceof SetOperationStmt) {
                strBuilder.append("(");
            }
            strBuilder.append(operands.get(i).getQueryStmt().toSql());
            if (operands.get(i).getQueryStmt() instanceof SetOperationStmt) {
                strBuilder.append(")");
            }
        }
        // Determine whether we need parenthesis around the last Set operand.
        SetOperand lastOperand = operands.get(operands.size() - 1);
        QueryStmt lastQueryStmt = lastOperand.getQueryStmt();
        strBuilder.append(" " + lastOperand.getOperation().toString() + " "
                + ((lastOperand.getQualifier() == Qualifier.ALL) ? "ALL " : ""));
        if (lastQueryStmt instanceof SetOperationStmt || ((hasOrderByClause() || hasLimitClause()) &&
                !lastQueryStmt.hasLimitClause() &&
                !lastQueryStmt.hasOrderByClause())) {
            strBuilder.append("(");
            strBuilder.append(lastQueryStmt.toSql());
            strBuilder.append(")");
        } else {
            strBuilder.append(lastQueryStmt.toSql());
        }
        // Order By clause
        if (hasOrderByClause()) {
            strBuilder.append(" ORDER BY ");
            for (int i = 0; i < orderByElements.size(); ++i) {
                strBuilder.append(orderByElements.get(i).getExpr().toSql());
                strBuilder.append(orderByElements.get(i).getIsAsc() ? " ASC" : " DESC");
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
    public ArrayList<String> getColLabels() {
        Preconditions.checkState(operands.size() > 0);
        return operands.get(0).getQueryStmt().getColLabels();
    }

    @Override
    public void setNeedToSql(boolean needToSql) {
        super.setNeedToSql(needToSql);
        for (SetOperand operand : operands) {
            operand.getQueryStmt().setNeedToSql(needToSql);
        }
    }

    @Override
    public void substituteSelectList(Analyzer analyzer, List<String> newColLabels)
            throws AnalysisException, UserException {
        QueryStmt firstQuery = operands.get(0).getQueryStmt();
        firstQuery.substituteSelectList(analyzer, newColLabels);
        // substitute order by
        if (orderByElements != null) {
            orderByElements = OrderByElement.substitute(orderByElements, firstQuery.aliasSMap, analyzer);
        }
    }

    /**
     * Represents an operand to a SetOperand. It consists of a query statement and its left
     * all/distinct qualifier (null for the first operand).
     */
    public static class SetOperand {
        // Operand indicate this SetOperand is union/intersect/except
        private Operation operation;

        // Effective qualifier. Should not be reset() to preserve changes made during
        // distinct propagation and unnesting that are needed after rewriting Subqueries.
        private Qualifier qualifier_;

        // ///////////////////////////////////////
        // BEGIN: Members that need to be reset()

        private QueryStmt queryStmt;

        // Analyzer used for this operand. Set in analyze().
        // We must preserve the conjuncts registered in the analyzer for partition pruning.
        private Analyzer analyzer;

        // Map from SetOperationStmt's result slots to our resultExprs. Used during plan generation.
        private final ExprSubstitutionMap smap_;

        // END: Members that need to be reset()
        // ///////////////////////////////////////

        public SetOperand(QueryStmt queryStmt, Operation operation, Qualifier qualifier) {
            this.queryStmt = queryStmt;
            this.operation = operation;
            qualifier_ = qualifier;
            smap_ = new ExprSubstitutionMap();
        }

        public void analyze(Analyzer parent) throws AnalysisException, UserException {
            if (isAnalyzed()) {
                return;
            }
            // union statement support const expr, so not need to equal
            if (operation != Operation.UNION && queryStmt instanceof SelectStmt
                    && ((SelectStmt) queryStmt).fromClause_.isEmpty()) {
                // equal select 1 to select * from (select 1) __DORIS_DUAL__ , because when using select 1 it will be
                // transformed to a union node, select 1 is a literal, it doesn't have a tuple but will produce a slot,
                // this will cause be core dump
                QueryStmt inlineQuery = queryStmt.clone();
                Map<String, Integer> map = new HashMap<>();
                // rename select 2,2 to select 2 as 2_1, 2 as 2_2 to avoid duplicated column in inline view
                for (int i = 0; i < ((SelectStmt) inlineQuery).selectList.getItems().size(); ++i) {
                    SelectListItem item = ((SelectStmt) inlineQuery).selectList.getItems().get(i);
                    String col = item.toColumnLabel();
                    Integer count = map.get(col);
                    count = (count == null) ? 1 : count + 1;
                    map.put(col, count);
                    if (count > 1) {
                        ((SelectStmt) inlineQuery).selectList.getItems()
                                .set(i, new SelectListItem(item.getExpr(), col + "_" + count.toString()));
                    }
                }
                ((SelectStmt) queryStmt).fromClause_.add(new InlineViewRef("__DORIS_DUAL__", inlineQuery));
                List<SelectListItem> slist = ((SelectStmt) queryStmt).selectList.getItems();
                slist.clear();
                slist.add(SelectListItem.createStarItem(null));
            }
            // Oracle and ms-SQLServer do not support INTERSECT ALL and EXCEPT ALL, postgres support it,
            // but it is very ambiguous
            if (qualifier_ == Qualifier.ALL && (operation == Operation.EXCEPT || operation == Operation.INTERSECT)) {
                throw new AnalysisException("INTERSECT and EXCEPT does not support ALL qualifier.");
            }
            analyzer = new Analyzer(parent);
            queryStmt.analyze(analyzer);
        }

        public boolean isAnalyzed() { return analyzer != null; }
        public QueryStmt getQueryStmt() { return queryStmt; }
        public Qualifier getQualifier() { return qualifier_; }
        public Operation getOperation() {
            return operation;
        }
        // Used for propagating DISTINCT.
        public void setQualifier(Qualifier qualifier) { qualifier_ = qualifier; }

        public void setOperation(Operation operation) {
            this.operation =operation;
        }
        public void setQueryStmt(QueryStmt queryStmt) {
            this.queryStmt = queryStmt;
        }
        public Analyzer getAnalyzer() { return analyzer; }
        public ExprSubstitutionMap getSmap() { return smap_; }

        public boolean hasAnalyticExprs() {
            if (queryStmt instanceof SelectStmt) {
                return ((SelectStmt) queryStmt).hasAnalyticInfo();
            } else {
                Preconditions.checkState(queryStmt instanceof SetOperationStmt);
                return ((SetOperationStmt) queryStmt).hasAnalyticExprs();
            }
        }

        /**
         * C'tor for cloning.
         */
        private SetOperand(SetOperand other) {
            queryStmt = other.queryStmt.clone();
            this.operation = other.operation;
            qualifier_ = other.qualifier_;
            analyzer = other.analyzer;
            smap_ = other.smap_.clone();
        }

        public void reset() {
            queryStmt.reset();
            analyzer = null;
            smap_.clear();
        }

        @Override
        public SetOperand clone() {
            return new SetOperand(this);
        }
    }
}
