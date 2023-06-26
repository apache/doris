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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/InlineViewRef.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.InlineView;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.thrift.TNullSide;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An inline view is a query statement with an alias. Inline views can be parsed directly
 * from a query string or represent a reference to a local or catalog view.
 */
public class InlineViewRef extends TableRef {
    private static final Logger LOG = LogManager.getLogger(InlineViewRef.class);

    // Catalog or local view that is referenced.
    // Null for inline views parsed directly from a query string.
    private final View view;

    // If not null, these will serve as the column labels for the inline view. This provides
    // a layer of separation between column labels visible from outside the inline view
    // and column labels used in the query definition. Either all or none of the column
    // labels must be overridden.
    private List<String> explicitColLabels;
    private List<List<String>> explicitSubColLabels;

    // ///////////////////////////////////////
    // BEGIN: Members that need to be reset()

    // The select or union statement of the inline view
    private QueryStmt queryStmt;

    // queryStmt has its own analysis context
    private Analyzer inlineViewAnalyzer;

    // list of tuple ids materialized by queryStmt
    private final ArrayList<TupleId> materializedTupleIds = Lists.newArrayList();

    // Map inline view's output slots to the corresponding resultExpr of queryStmt.
    protected final ExprSubstitutionMap sMap;

    // Map inline view's output slots to the corresponding baseTblResultExpr of queryStmt.
    protected final ExprSubstitutionMap baseTblSmap;

    // When parsing a ddl of hive view, it does not contains any catalog info,
    // so we need to record it in Analyzer
    // otherwise some error will occurs when resolving TableRef later.
    protected String externalCtl;

    // END: Members that need to be reset()
    // ///////////////////////////////////////

    /**
     * C'tor for creating inline views parsed directly from the a query string.
     */
    public InlineViewRef(String alias, QueryStmt queryStmt) {
        super(null, alias);
        this.queryStmt = queryStmt;
        this.view = null;
        sMap = new ExprSubstitutionMap();
        baseTblSmap = new ExprSubstitutionMap();
    }

    public InlineViewRef(String alias, QueryStmt queryStmt, List<String> colLabels) {
        this(alias, queryStmt);
        explicitColLabels = Lists.newArrayList(colLabels);
        LOG.debug("inline view explicitColLabels {}", explicitColLabels);
    }

    /**
     * C'tor for creating inline views that replace a local or catalog view ref.
     */
    public InlineViewRef(View view, TableRef origTblRef) {
        super(origTblRef.getName(), origTblRef.getExplicitAlias());
        queryStmt = view.getQueryStmt().clone();
        if (view.isLocalView()) {
            queryStmt.reset();
        }
        this.view = view;
        sMap = new ExprSubstitutionMap();
        baseTblSmap = new ExprSubstitutionMap();
        setJoinAttrs(origTblRef);
        explicitColLabels = view.getColLabels();
        // Set implicit aliases if no explicit one was given.
        if (hasExplicitAlias()) {
            return;
        }
        // TODO(zc)
        // view_.getTableName().toString().toLowerCase(), view.getName().toLowerCase()
        if (view.isLocalView()) {
            aliases = new String[]{view.getName()};
        } else {
            aliases = new String[]{name.toString(), view.getName()};
        }
        if (origTblRef.getLateralViewRefs() != null) {
            lateralViewRefs = (ArrayList<LateralViewRef>) origTblRef.getLateralViewRefs().clone();
        }
    }

    protected InlineViewRef(InlineViewRef other) {
        super(other);
        queryStmt = other.queryStmt.clone();
        view = other.view;
        inlineViewAnalyzer = other.inlineViewAnalyzer;
        if (other.explicitColLabels != null) {
            explicitColLabels = Lists.newArrayList(other.explicitColLabels);
        }
        materializedTupleIds.addAll(other.materializedTupleIds);
        sMap = other.sMap.clone();
        baseTblSmap = other.baseTblSmap.clone();
    }

    public List<String> getExplicitColLabels() {
        return explicitColLabels;
    }

    public List<String> getColLabels() {
        if (explicitColLabels != null) {
            return explicitColLabels;
        }
        return queryStmt.getColLabels();
    }

    public List<List<String>> getSubColLabels() {
        if (explicitSubColLabels != null) {
            return explicitSubColLabels;
        }
        return queryStmt.getSubColLabels();
    }

    @Override
    public void reset() {
        super.reset();
        queryStmt.reset();
        inlineViewAnalyzer = null;
        materializedTupleIds.clear();
        sMap.clear();
        baseTblSmap.clear();
    }

    @Override
    public TableRef clone() {
        return new InlineViewRef(this);
    }

    public void setNeedToSql(boolean needToSql) {
        queryStmt.setNeedToSql(needToSql);
    }

    /**
     * Analyzes the inline view query block in a child analyzer of 'analyzer', creates
     * a new tuple descriptor for the inline view and registers auxiliary eq predicates
     * between the slots of that descriptor and the select list exprs of the inline view;
     * then performs join clause analysis.
     */
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (isAnalyzed) {
            return;
        }

        if (view == null && !hasExplicitAlias()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DERIVED_MUST_HAVE_ALIAS);
        }

        // Analyze the inline view query statement with its own analyzer
        inlineViewAnalyzer = new Analyzer(analyzer);
        inlineViewAnalyzer.setInlineView(true);
        if (hasExplicitAlias) {
            inlineViewAnalyzer.setExplicitViewAlias(aliases[0]);
        }
        queryStmt.analyze(inlineViewAnalyzer);
        correlatedTupleIds.addAll(queryStmt.getCorrelatedTupleIds(inlineViewAnalyzer));

        queryStmt.getMaterializedTupleIds(materializedTupleIds);
        if (view != null && !hasExplicitAlias() && !view.isLocalView()) {
            name = analyzer.getFqTableName(name);
            aliases = new String[] { name.toString(), view.getName() };
        }
        //TODO(chenhao16): fix TableName in Db.Table style
        // name.analyze(analyzer);
        desc = analyzer.registerTableRef(this);
        isAnalyzed = true;  // true now that we have assigned desc

        // For constant selects we materialize its exprs into a tuple.
        if (materializedTupleIds.isEmpty()) {
            Preconditions.checkState(queryStmt instanceof SelectStmt);
            Preconditions.checkState(((SelectStmt) queryStmt).getTableRefs().isEmpty());
            desc.setIsMaterialized(true);
            materializedTupleIds.add(desc.getId());
        }
        // create sMap and baseTblSmap and register auxiliary eq predicates between our
        // tuple descriptor's slots and our *unresolved* select list exprs;
        // we create these auxiliary predicates so that the analyzer can compute the value
        // transfer graph through this inline view correctly (ie, predicates can get
        // propagated through the view);
        // if the view stmt contains analytic functions, we cannot propagate predicates
        // into the view, unless the predicates are compatible with the analytic
        // function's partition by clause, because those extra filters
        // would alter the results of the analytic functions (see IMPALA-1243)
        // TODO: relax this a bit by allowing propagation out of the inline view (but
        // not into it)
        List<SlotDescriptor> slots = analyzer.changeSlotToNullableOfOuterJoinedTuples();
        LOG.debug("inline view query {}", queryStmt.toSql());
        for (int i = 0; i < getColLabels().size(); ++i) {
            String colName = getColLabels().get(i);
            LOG.debug("inline view register {}", colName);
            SlotDescriptor slotDesc = analyzer.registerColumnRef(getAliasAsName(),
                                            colName, getSubColLabels().get(i));
            Expr colExpr = queryStmt.getResultExprs().get(i);
            slotDesc.setSourceExpr(colExpr);
            slotDesc.setIsNullable(slotDesc.getIsNullable() || colExpr.isNullable());
            SlotRef slotRef = new SlotRef(slotDesc);
            sMap.put(slotRef, colExpr);
            baseTblSmap.put(slotRef, queryStmt.getBaseTblResultExprs().get(i));
            if (createAuxPredicate(colExpr)) {
                analyzer.createAuxEquivPredicate(new SlotRef(slotDesc), colExpr.clone());
            }
        }
        analyzer.changeSlotsToNotNullable(slots);
        if (LOG.isDebugEnabled()) {
            LOG.debug("inline view " + getUniqueAlias() + " smap: " + sMap.debugString());
            LOG.debug("inline view " + getUniqueAlias() + " baseTblSmap: " + baseTblSmap.debugString());
        }

        // analyzeLateralViewRefs
        analyzeLateralViewRef(analyzer);

        // Now do the remaining join analysis
        // In general, we should do analyze join before do RegisterColumnRef. However, We cannot move analyze join
        // before generate sMap and baseTblSmap, because generate sMap and baseTblSmap will register all column refs
        // in the inline view. If inline view is on right side of left semi join, exception will be thrown.
        // Instead, we do a little trick in RegisterColumnRef to avoid this problem.
        analyzeJoin(analyzer);
    }

    /**
     * Checks if an auxiliary predicate should be created for an expr. Returns False if the
     * inline view has a SELECT stmt with analytic functions and the expr is not in the
     * common partition exprs of all the analytic functions computed by this inline view.
     */
    public boolean createAuxPredicate(Expr e) {
        if (!(queryStmt instanceof SelectStmt)
                || !((SelectStmt) queryStmt).hasAnalyticInfo()) {
            return true;
        }
        AnalyticInfo analyticInfo = ((SelectStmt) queryStmt).getAnalyticInfo();
        return analyticInfo.getCommonPartitionExprs().contains(e);
    }

    /**
     * Create a non-materialized tuple descriptor in descTbl for this inline view.
     * This method is called from the analyzer when registering this inline view.
     */
    @Override
    public TupleDescriptor createTupleDescriptor(Analyzer analyzer) throws AnalysisException {
        // Create a fake catalog table for the inline view
        int numColLabels = getColLabels().size();
        Preconditions.checkState(numColLabels > 0);
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        List<Column> columnList = Lists.newArrayList();
        for (int i = 0; i < numColLabels; ++i) {
            // inline view select statement has been analyzed. Col label should be filled.
            Expr selectItemExpr = queryStmt.getResultExprs().get(i);
            // String colAlias = queryStmt.getColLabels().get(i);
            String colAlias = getColLabels().get(i);

            // inline view col cannot have duplicate name
            if (columnSet.contains(colAlias)) {
                throw new AnalysisException(
                        "Duplicated inline view column alias: '" + colAlias + "'" + " in inline view "
                                + "'" + getAlias() + "'");
            }

            columnSet.add(colAlias);
            columnList.add(new Column(colAlias, selectItemExpr.getType(),
                    false, null, selectItemExpr.isNullable(),
                    null, ""));
        }
        InlineView inlineView = (view != null) ? new InlineView(view, columnList)
                : new InlineView(getExplicitAlias(), columnList);

        // Create the non-materialized tuple and set the fake table in it.
        TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor();
        result.setIsMaterialized(false);
        result.setTable(inlineView);
        analyzer.registerInlineViewTupleId(result.getId());
        return result;
    }

    /**
     * Makes each rhs expr in sMap nullable if necessary by wrapping as follows:
     * IF(TupleIsNull(), NULL, rhs expr)
     * Should be called only if this inline view is a nullable side of an outer join.
     * <p/>
     * We need to make an rhs exprs nullable if it evaluates to a non-NULL value
     * when all of its contained SlotRefs evaluate to NULL.
     * For example, constant exprs need to be wrapped or an expr such as
     * 'case slotref is null then 1 else 2 end'
     */
    //
    // protected void makeOutputNullable(Analyzer analyzer) throws AnalysisException, InternalException {
    //     // Gather all unique rhs SlotRefs into rhsSlotRefs
    //     List<SlotRef> rhsSlotRefs = Lists.newArrayList();
    //     Expr.collectList(sMap.rhs, SlotRef.class, rhsSlotRefs);
    //     // Map for substituting SlotRefs with NullLiterals.
    //     Expr.SubstitutionMap nullSMap = new Expr.SubstitutionMap();
    //     for (SlotRef rhsSlotRef : rhsSlotRefs) {
    //         nullSMap.lhs.add(rhsSlotRef.clone());
    //         nullSMap.rhs.add(NullLiteral.create(rhsSlotRef.getType()));
    //     }
    //
    //     // Make rhs exprs nullable if necessary.
    //     for (int i = 0; i < sMap.rhs.size(); ++i) {
    //         List<Expr> params = Lists.newArrayList();
    //         if (!requiresNullWrapping(analyzer, sMap.rhs.get(i), nullSMap)) {
    //             continue;
    //         }
    //         params.add(new TupleIsNullPredicate(materializedTupleIds));
    //         params.add(NullLiteral.create(sMap.rhs.get(i).getType()));
    //         params.add(sMap.rhs.get(i));
    //         Expr ifExpr = new FunctionCallExpr("if", params);
    //         ifExpr.analyze(analyzer);
    //         sMap.rhs.set(i, ifExpr);
    //     }
    // }

    protected void makeOutputNullable(Analyzer analyzer) throws AnalysisException, UserException {
        try {
            makeOutputNullableHelper(analyzer, sMap);
            makeOutputNullableHelper(analyzer, baseTblSmap);
        } catch (Exception e) {
            // should never happen
            throw new IllegalStateException(e);
        }
    }

    protected void makeOutputNullableHelper(Analyzer analyzer, ExprSubstitutionMap smap)
            throws Exception {
        // Gather all unique rhs SlotRefs into rhsSlotRefs
        List<SlotRef> rhsSlotRefs = Lists.newArrayList();
        Expr.collectList(smap.getRhs(), SlotRef.class, rhsSlotRefs);
        // Map for substituting SlotRefs with NullLiterals.
        ExprSubstitutionMap nullSMap = new ExprSubstitutionMap();
        for (SlotRef rhsSlotRef : rhsSlotRefs) {
            nullSMap.put(rhsSlotRef.clone(), NullLiteral.create(rhsSlotRef.getType()));
        }


        // Make rhs exprs nullable if necessary.
        for (int i = 0; i < smap.getRhs().size(); ++i) {
            List<Expr> params = Lists.newArrayList();
            if (!requiresNullWrapping(analyzer, smap.getRhs().get(i), nullSMap)) {
                continue;
            }
            if (analyzer.isOuterJoinedLeftSide(materializedTupleIds.get(0))) {
                params.add(new TupleIsNullPredicate(materializedTupleIds, TNullSide.LEFT));
            } else {
                params.add(new TupleIsNullPredicate(materializedTupleIds, TNullSide.RIGHT));
            }
            params.add(NullLiteral.create(smap.getRhs().get(i).getType()));
            params.add(smap.getRhs().get(i));
            Expr ifExpr = new FunctionCallExpr("if", params);
            ifExpr.analyze(analyzer);
            smap.getRhs().set(i, ifExpr);
        }
    }

    /**
     * Replaces all SloRefs in expr with a NullLiteral using nullSMap, and evaluates the
     * resulting constant expr. Returns true if the constant expr yields a non-NULL value,
     * false otherwise.
     */
    private boolean requiresNullWrapping(Analyzer analyzer, Expr expr, ExprSubstitutionMap nullSMap)
            throws UserException {
        // If the expr is already wrapped in an IF(TupleIsNull(), NULL, expr)
        // then do not try to execute it.
        if (expr.contains(TupleIsNullPredicate.class)) {
            return true;
        }
        return true;
    }

    @Override
    public void rewriteExprs(ExprRewriter rewriter, Analyzer analyzer)
            throws AnalysisException {
        super.rewriteExprs(rewriter, analyzer);
        queryStmt.rewriteExprs(rewriter);
    }

    @Override
    public List<TupleId> getMaterializedTupleIds() {
        Preconditions.checkState(isAnalyzed);
        Preconditions.checkState(materializedTupleIds.size() > 0);
        return materializedTupleIds;
    }

    public QueryStmt getViewStmt() {
        return queryStmt;
    }

    public void setViewStmt(QueryStmt queryStmt) {
        this.queryStmt = queryStmt;
    }

    public Analyzer getAnalyzer() {
        Preconditions.checkState(isAnalyzed);
        return inlineViewAnalyzer;
    }

    public ExprSubstitutionMap getSmap() {
        Preconditions.checkState(isAnalyzed);
        return sMap;
    }

    public ExprSubstitutionMap getBaseTblSmap() {
        Preconditions.checkState(isAnalyzed);
        return baseTblSmap;
    }

    public boolean isLocalView() {
        return view == null || view.isLocalView();
    }

    public View getView() {
        return view;
    }

    public QueryStmt getQueryStmt() {
        return queryStmt;
    }

    public void setExternalCtl(String externalCtl) {
        this.externalCtl = externalCtl;
    }

    public String getExternalCtl() {
        return this.externalCtl;
    }

    @Override
    public String tableNameToSql() {
        // Enclose the alias in quotes if Hive cannot parse it without quotes.
        // This is needed for view compatibility between Impala and Hive.
        if (view != null) {
            // FIXME: this may result in a sql cache problem
            // See pr #6736 and issue #6735
            return super.tableNameToSql();
        }

        String aliasSql = null;
        String alias = getExplicitAlias();
        if (alias != null) {
            aliasSql = ToSqlUtils.getIdentSql(alias);
        }
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(queryStmt.toSql()).append(") ")
            .append(aliasSql);

        return sb.toString();
    }

    @Override
    public String tableRefToDigest() {
        String aliasSql = null;
        String alias = getExplicitAlias();
        if (alias != null) {
            aliasSql = ToSqlUtils.getIdentSql(alias);
        }
        if (view != null) {
            return name.toSql() + (aliasSql == null ? "" : " " + aliasSql);
        }

        StringBuilder sb = new StringBuilder()
                .append("(")
                .append(queryStmt.toDigest())
                .append(") ")
                .append(aliasSql);

        return sb.toString();
    }
}
