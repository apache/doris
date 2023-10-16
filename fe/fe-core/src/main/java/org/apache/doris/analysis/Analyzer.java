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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/Analyzer.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.View;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.BetweenToCompoundRule;
import org.apache.doris.rewrite.CompoundPredicateWriteRule;
import org.apache.doris.rewrite.EliminateUnnecessaryFunctions;
import org.apache.doris.rewrite.EraseRedundantCastExpr;
import org.apache.doris.rewrite.ExprRewriteRule;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.rewrite.ExtractCommonFactorsRule;
import org.apache.doris.rewrite.FoldConstantsRule;
import org.apache.doris.rewrite.InferFiltersRule;
import org.apache.doris.rewrite.MatchPredicateRule;
import org.apache.doris.rewrite.NormalizeBinaryPredicatesRule;
import org.apache.doris.rewrite.RewriteAliasFunctionRule;
import org.apache.doris.rewrite.RewriteBinaryPredicatesRule;
import org.apache.doris.rewrite.RewriteDateLiteralRule;
import org.apache.doris.rewrite.RewriteEncryptKeyRule;
import org.apache.doris.rewrite.RewriteFromUnixTimeRule;
import org.apache.doris.rewrite.RewriteImplicitCastRule;
import org.apache.doris.rewrite.RewriteInPredicateRule;
import org.apache.doris.rewrite.RewriteIsNullIsNotNullRule;
import org.apache.doris.rewrite.RoundLiteralInBinaryPredicatesRule;
import org.apache.doris.rewrite.mvrewrite.CountDistinctToBitmap;
import org.apache.doris.rewrite.mvrewrite.CountDistinctToBitmapOrHLLRule;
import org.apache.doris.rewrite.mvrewrite.ExprToSlotRefRule;
import org.apache.doris.rewrite.mvrewrite.HLLHashToSlotRefRule;
import org.apache.doris.rewrite.mvrewrite.NDVToHll;
import org.apache.doris.rewrite.mvrewrite.ToBitmapToSlotRefRule;
import org.apache.doris.thrift.TQueryGlobals;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Repository of analysis state for single select block.
 * <p/>
 * All conjuncts are assigned a unique id when initially registered, and all
 * registered conjuncts are referenced by their id (ie, there are no containers
 * other than the one holding the referenced conjuncts), to make substitute()
 * simple.
 */
public class Analyzer {
    private static final Logger LOG = LogManager.getLogger(Analyzer.class);
    // used for contains inlineview analytic function's tuple changed
    private ExprSubstitutionMap changeResSmap = new ExprSubstitutionMap();

    // NOTE: Alias of table is case sensitive
    // UniqueAlias used to check whether the table ref or the alias is unique
    // table/view used db.table, inline use alias
    private final Set<String> uniqueTableAliasSet = Sets.newHashSet();
    private final Multimap<String, TupleDescriptor> tupleByAlias = ArrayListMultimap.create();

    // NOTE: Alias of column is case ignorance
    // map from lowercase table alias to descriptor.
    // protected final Map<String, TupleDescriptor> aliasMap             = Maps.newHashMap();
    // map from lowercase qualified column name ("alias.col") to descriptor
    private final Map<String, SlotDescriptor>  slotRefMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    // Notice: it's case sensitive
    // Variant column name -> Paths of sub columns
    private final Map<String, Map<List<String>, SlotDescriptor>> subColumnSlotRefMap
                                           = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    // map from tuple id to list of conjuncts referencing tuple
    private final Map<TupleId, List<ExprId>> tuplePredicates = Maps.newHashMap();
    // map from slot id to list of conjuncts referencing slot
    private final Map<SlotId, List<ExprId>> slotPredicates = Maps.newHashMap();
    // eqJoinPredicates[tid] contains all conjuncts of the form
    // "<lhs> = <rhs>" in which either lhs or rhs is fully bound by tid
    // and the other side is not bound by tid (ie, predicates that express equi-join
    // conditions between two tablerefs).
    // A predicate such as "t1.a = t2.b" has two entries, one for 't1' and
    // another one for 't2'.

    // all conjuncts of the Where clause
    private final Set<ExprId> whereClauseConjuncts = Sets.newHashSet();
    // map from tuple id to list of Exprs referencing tuple
    // which buffer can reuse in vectorized process
    private final Map<TupleId, List<Expr>> bufferReuseExprs = Maps.newHashMap();
    // map from tuple id to the current output column index
    private final Map<TupleId, Integer> currentOutputColumn = Maps.newHashMap();
    // used for Information Schema Table Scan
    private String schemaDb;
    private String schemaCatalog;
    private String schemaWild;
    private String schemaTable; // table used in DESCRIBE Table

    // Current depth of nested analyze() calls. Used for enforcing a
    // maximum expr-tree depth. Needs to be manually maintained by the user
    // of this Analyzer with incrementCallDepth() and decrementCallDepth().
    private int callDepth = 0;

    // Flag indicating if this analyzer instance belongs to a subquery.
    private boolean isSubquery = false;
    private boolean isFirstScopeInSubquery = false;
    // Flag indicating if this analyzer instance belongs to an inlineview.
    private boolean isInlineView = false;

    private String explicitViewAlias;
    // Flag indicating whether this analyzer belongs to a WITH clause view.
    private boolean isWithClause = false;

    // By default, all registered semi-joined tuples are invisible, i.e., their slots
    // cannot be referenced. If set, this semi-joined tuple is made visible. Such a tuple
    // should only be made visible for analyzing the On-clause of its semi-join.
    // In particular, if there are multiple semi-joins in the same query block, then the
    // On-clause of any such semi-join is not allowed to reference other semi-joined tuples
    // except its own. Therefore, only a single semi-joined tuple can be visible at a time.
    private TupleId visibleSemiJoinedTupleId = null;
    // for some situation that udf is not allowed.
    private boolean isUDFAllowed = true;
    // timezone specified for some operation, such as broker load
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;

    // The runtime filter that is expected to be used
    private final List<RuntimeFilter> assignedRuntimeFilters = new ArrayList<>();

    private boolean isReAnalyze = false;

    public void setIsSubquery() {
        isSubquery = true;
        isFirstScopeInSubquery = true;
        globalState.containsSubquery = true;
    }

    public boolean setHasPlanHints() {
        return globalState.hasPlanHints = true;
    }

    public boolean hasPlanHints() {
        return globalState.hasPlanHints;
    }

    public void setIsWithClause() {
        isWithClause = true;
    }

    public boolean isWithClause() {
        return isWithClause;
    }

    public void setReAnalyze(boolean reAnalyze) {
        isReAnalyze = reAnalyze;
    }

    public boolean isReAnalyze() {
        return isReAnalyze;
    }

    public void setUDFAllowed(boolean val) {
        this.isUDFAllowed = val;
    }

    public boolean isUDFAllowed() {
        return this.isUDFAllowed;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getTimezone() {
        return timezone;
    }

    public void putEquivalentSlot(SlotId srcSid, SlotId targetSid) {
        globalState.equivalentSlots.put(srcSid, targetSid);
    }

    public SlotId getEquivalentSlot(SlotId srcSid) {
        return globalState.equivalentSlots.get(srcSid);
    }

    public boolean containEquivalentSlot(SlotId srcSid) {
        return globalState.equivalentSlots.containsKey(srcSid);
    }

    public void putAssignedRuntimeFilter(RuntimeFilter rf) {
        assignedRuntimeFilters.add(rf);
    }

    public List<RuntimeFilter> getAssignedRuntimeFilter() {
        return assignedRuntimeFilters;
    }

    public void clearAssignedRuntimeFilters() {
        assignedRuntimeFilters.clear();
    }

    public long getAutoBroadcastJoinThreshold() {
        return globalState.autoBroadcastJoinThreshold;
    }

    private static class InferPredicateState {
        // map from two table tuple ids to JoinOperator between two tables.
        // NOTE: first tupleId's position in front of the second tupleId.
        public final Map<Pair<TupleId, TupleId>, JoinOperator> anyTwoTalesJoinOperator = Maps.newHashMap();

        // slotEqSlotExpr: Record existing and infer equivalent connections
        private final List<Expr> onSlotEqSlotExpr = new ArrayList<>();

        // slotEqSlotDeDuplication: De-Duplication for slotEqSlotExpr
        private final Set<Pair<Expr, Expr>> onSlotEqSlotDeDuplication = Sets.newHashSet();

        // slotToLiteralExpr: Record existing and infer expr which slot and literal are equal
        private final List<Expr> onSlotToLiteralExpr = new ArrayList<>();

        // slotToLiteralDeDuplication: De-Duplication for slotToLiteralExpr
        private final Set<Pair<Expr, Expr>> onSlotToLiteralDeDuplication = Sets.newHashSet();

        // inExpr: Recoud existing and infer expr which in predicate
        private final List<Expr> onInExpr = new ArrayList<>();

        // inExprDeDuplication: De-Duplication for inExpr
        private final Set<Expr> onInDeDuplication = Sets.newHashSet();

        // isNullExpr: Record existing and infer not null predicate
        private final List<Expr> onIsNullExpr = new ArrayList<>();

        //isNullDeDuplication: De-Duplication for isNullExpr
        private final Set<Expr> onIsNullDeDuplication = Sets.newHashSet();

        // slotToLiteralDeDuplication: De-Duplication for slotToLiteralExpr. Contain on and where.
        private final Set<Pair<Expr, Expr>> globalSlotToLiteralDeDuplication = Sets.newHashSet();

        // inExprDeDuplication: De-Duplication for inExpr. Contain on and where
        private final Set<Expr> globalInDeDuplication = Sets.newHashSet();

        public InferPredicateState() {
        }

        public InferPredicateState(InferPredicateState that) {
            anyTwoTalesJoinOperator.putAll(that.anyTwoTalesJoinOperator);
            onSlotEqSlotExpr.addAll(that.onSlotEqSlotExpr);
            onSlotEqSlotDeDuplication.addAll(that.onSlotEqSlotDeDuplication);
            onSlotToLiteralExpr.addAll(that.onSlotToLiteralExpr);
            onSlotToLiteralDeDuplication.addAll(that.onSlotToLiteralDeDuplication);
            onInExpr.addAll(that.onInExpr);
            onInDeDuplication.addAll(that.onInDeDuplication);
            onIsNullExpr.addAll(that.onIsNullExpr);
            onIsNullDeDuplication.addAll(that.onIsNullDeDuplication);
            globalSlotToLiteralDeDuplication.addAll(that.globalSlotToLiteralDeDuplication);
            globalInDeDuplication.addAll(that.globalInDeDuplication);
        }
    }

    // state shared between all objects of an Analyzer tree
    // TODO: Many maps here contain properties about tuples, e.g., whether
    //  a tuple is outer/semi joined, etc. Remove the maps in favor of making
    //  them properties of the tuple descriptor itself.
    private static class GlobalState {
        private final DescriptorTable descTbl = new DescriptorTable();
        private final Env env;
        private final IdGenerator<ExprId> conjunctIdGenerator = ExprId.createGenerator();
        private final ConnectContext context;

        // True if we are analyzing an explain request. Should be set before starting
        // analysis.
        public boolean isExplain;

        // Indicates whether the query has plan hints.
        public boolean hasPlanHints = false;

        // True if at least one of the analyzers belongs to a subquery.
        public boolean containsSubquery = false;

        // When parsing a ddl of hive view, it does not contains any catalog info,
        // so we need to record it in Analyzer
        // otherwise some error will occurs when resolving TableRef later.
        public String externalCtl;

        // all registered conjuncts (map from id to Predicate)
        private final Map<ExprId, Expr> conjuncts = Maps.newHashMap();

        // all registered conjuncts bound by a single tuple id; used in getBoundPredicates()
        public final ArrayList<ExprId> singleTidConjuncts = Lists.newArrayList();

        // eqJoinConjuncts[tid] contains all conjuncts of the form
        // "<lhs> = <rhs>" in which either lhs or rhs is fully bound by tid
        // and the other side is not bound by tid (ie, predicates that express equi-join
        // conditions between two tablerefs).
        // A predicate such as "t1.a = t2.b" has two entries, one for 't1' and
        // another one for 't2'.
        private final Map<TupleId, List<ExprId>> eqJoinConjuncts = Maps.newHashMap();

        // set of conjuncts that have been assigned to some PlanNode
        private Set<ExprId> assignedConjuncts = Collections.newSetFromMap(new IdentityHashMap<ExprId, Boolean>());

        private Set<TupleId> inlineViewTupleIds = Sets.newHashSet();

        // map from outer-joined tuple id, ie, one that is nullable in this select block,
        // to the last Join clause (represented by its rhs table ref) that outer-joined it
        private final Map<TupleId, TableRef> outerJoinedTupleIds = Maps.newHashMap();

        // set of left side and right side of tuple id to mark null side in vec
        // exec engine
        private final Set<TupleId> outerLeftSideJoinTupleIds = Sets.newHashSet();
        private final Set<TupleId> outerRightSideJoinTupleIds = Sets.newHashSet();

        // Map of registered conjunct to the last full outer join (represented by its
        // rhs table ref) that outer joined it.
        public final Map<ExprId, TableRef> fullOuterJoinedConjuncts = Maps.newHashMap();

        // Map of full-outer-joined tuple id to the last full outer join that outer-joined it
        public final Map<TupleId, TableRef> fullOuterJoinedTupleIds = Maps.newHashMap();

        // Map from semi-joined tuple id, i.e., one that is invisible outside the join's
        // On-clause, to its Join clause (represented by its rhs table ref). An anti-join is
        // a kind of semi-join, so anti-joined tuples are also registered here.
        public final Map<TupleId, TableRef> semiJoinedTupleIds = Maps.newHashMap();

        // Map from right-hand side table-ref id of an outer join to the list of
        // conjuncts in its On clause. There is always an entry for an outer join, but the
        // corresponding value could be an empty list. There is no entry for non-outer joins.
        public final Map<TupleId, List<ExprId>> conjunctsByOjClause = Maps.newHashMap();

        public final Map<TupleId, List<ExprId>> conjunctsByAntiJoinNullAwareClause = Maps.newHashMap();

        public final Map<TupleId, List<ExprId>> conjunctsBySemiAntiJoinNoNullAwareClause = Maps.newHashMap();

        // map from registered conjunct to its containing outer join On clause (represented
        // by its right-hand side table ref); only conjuncts that can only be correctly
        // evaluated by the originating outer join are registered here
        private final Map<ExprId, TableRef> ojClauseByConjunct = Maps.newHashMap();

        // map from registered conjunct to its containing semi join On clause (represented
        // by its right-hand side table ref)
        public final Map<ExprId, TableRef> sjClauseByConjunct = Maps.newHashMap();

        // map from registered conjunct to its containing inner join On clause (represented
        // by its right-hand side table ref)
        public final Map<ExprId, TableRef> ijClauseByConjunct = Maps.newHashMap();

        // TODO chenhao16, to save conjuncts, which children are constant
        public final Map<TupleId, Set<Expr>> constantConjunct = Maps.newHashMap();

        // map from slot id to the analyzer/block in which it was registered
        private final Map<SlotId, Analyzer> blockBySlot = Maps.newHashMap();

        // Expr rewriter for normalizing and rewriting expressions.
        private final ExprRewriter exprRewriter;

        private final ExprRewriter mvExprRewriter;

        private final long autoBroadcastJoinThreshold;

        private final Map<SlotId, SlotId> equivalentSlots = Maps.newHashMap();

        private final Map<String, TupleDescriptor> markTuples = Maps.newHashMap();

        private final Map<TableRef, TupleId> markTupleIdByInnerRef = Maps.newHashMap();

        private final Set<TupleId> markTupleIdsNotProcessed = Sets.newHashSet();

        private final Map<InlineViewRef, Set<Expr>> migrateFailedConjuncts = Maps.newHashMap();

        public GlobalState(Env env, ConnectContext context) {
            this.env = env;
            this.context = context;
            List<ExprRewriteRule> rules = Lists.newArrayList();
            // BetweenPredicates must be rewritten to be executable. Other non-essential
            // expr rewrites can be disabled via a query option. When rewrites are enabled
            // BetweenPredicates should be rewritten first to help trigger other rules.
            rules.add(BetweenToCompoundRule.INSTANCE);
            // Binary predicates must be rewritten to a canonical form for both predicate
            // pushdown and Parquet row group pruning based on min/max statistics.
            rules.add(NormalizeBinaryPredicatesRule.INSTANCE);
            // Put it after NormalizeBinaryPredicatesRule, make sure slotRef is on the left and Literal is on the right.
            rules.add(RewriteBinaryPredicatesRule.INSTANCE);
            rules.add(RewriteImplicitCastRule.INSTANCE);
            rules.add(RoundLiteralInBinaryPredicatesRule.INSTANCE);
            rules.add(FoldConstantsRule.INSTANCE);
            rules.add(EraseRedundantCastExpr.INSTANCE);
            rules.add(RewriteFromUnixTimeRule.INSTANCE);
            rules.add(CompoundPredicateWriteRule.INSTANCE);
            rules.add(RewriteDateLiteralRule.INSTANCE);
            rules.add(RewriteEncryptKeyRule.INSTANCE);
            rules.add(RewriteInPredicateRule.INSTANCE);
            rules.add(RewriteAliasFunctionRule.INSTANCE);
            rules.add(RewriteIsNullIsNotNullRule.INSTANCE);
            rules.add(MatchPredicateRule.INSTANCE);
            rules.add(EliminateUnnecessaryFunctions.INSTANCE);
            List<ExprRewriteRule> onceRules = Lists.newArrayList();
            onceRules.add(ExtractCommonFactorsRule.INSTANCE);
            onceRules.add(InferFiltersRule.INSTANCE);
            exprRewriter = new ExprRewriter(rules, onceRules);
            // init mv rewriter
            List<ExprRewriteRule> mvRewriteRules = Lists.newArrayList();
            mvRewriteRules.add(new ExprToSlotRefRule());
            mvRewriteRules.add(ToBitmapToSlotRefRule.INSTANCE);
            mvRewriteRules.add(CountDistinctToBitmapOrHLLRule.INSTANCE);
            mvRewriteRules.add(CountDistinctToBitmap.INSTANCE);
            mvRewriteRules.add(NDVToHll.INSTANCE);
            mvRewriteRules.add(HLLHashToSlotRefRule.INSTANCE);
            mvExprRewriter = new ExprRewriter(mvRewriteRules);

            // context maybe null. eg, for StreamLoadPlanner.
            // and autoBroadcastJoinThreshold is only used for Query's DistributedPlanner.
            // so it is ok to not set autoBroadcastJoinThreshold if context is null
            if (context != null) {
                // compute max exec mem could be used for broadcast join
                long perNodeMemLimit = context.getSessionVariable().getMaxExecMemByte();
                double autoBroadcastJoinThresholdPercentage = context.getSessionVariable().autoBroadcastJoinThreshold;
                if (autoBroadcastJoinThresholdPercentage > 1) {
                    autoBroadcastJoinThresholdPercentage = 1.0;
                } else if (autoBroadcastJoinThresholdPercentage <= 0) {
                    autoBroadcastJoinThresholdPercentage = -1.0;
                }
                autoBroadcastJoinThreshold = (long) (perNodeMemLimit * autoBroadcastJoinThresholdPercentage);
            } else {
                // autoBroadcastJoinThreshold is a "final" field, must set an initial value for it
                autoBroadcastJoinThreshold = 0;
            }
        }
    }

    private final GlobalState globalState;

    // Attached PrepareStmt
    public PrepareStmt prepareStmt;

    private final InferPredicateState inferPredicateState;

    // An analyzer stores analysis state for a single select block. A select block can be
    // a top level select statement, or an inline view select block.
    // ancestors contains the Analyzers of the enclosing select blocks of 'this'
    // (ancestors[0] contains the immediate parent, etc.).
    private final ArrayList<Analyzer> ancestors;

    // map from lowercase table alias to a view definition in this analyzer's scope
    private final Map<String, View> localViews = Maps.newHashMap();

    // Map from lowercase table alias to descriptor. Tables without an explicit alias
    // are assigned two implicit aliases: the unqualified and fully-qualified table name.
    // Such tables have two entries pointing to the same descriptor. If an alias is
    // ambiguous, then this map retains the first entry with that alias to simplify error
    // checking (duplicate vs. ambiguous alias).
    private final Map<String, TupleDescriptor> aliasMap = Maps.newHashMap();

    // Map from tuple id to its corresponding table ref.
    private final Map<TupleId, TableRef> tableRefMap = Maps.newHashMap();

    // Set of lowercase ambiguous implicit table aliases.
    private final Set<String> ambiguousAliases = Sets.newHashSet();

    // Indicates whether this analyzer/block is guaranteed to have an empty result set
    // due to a limit 0 or constant conjunct evaluating to false.
    private boolean hasEmptyResultSet = false;

    // Indicates whether the select-project-join (spj) portion of this query block
    // is guaranteed to return an empty result set. Set due to a constant non-Having
    // conjunct evaluating to false.
    private boolean hasEmptySpjResultSet = false;

    public Analyzer(Env env, ConnectContext context) {
        ancestors = Lists.newArrayList();
        globalState = new GlobalState(env, context);
        inferPredicateState = new InferPredicateState();
    }

    /**
     * Analyzer constructor for nested select block. Catalog and DescriptorTable
     * is inherited from the parentAnalyzer.
     *
     * @param parentAnalyzer the analyzer of the enclosing select block
     */
    public Analyzer(Analyzer parentAnalyzer) {
        this(parentAnalyzer, parentAnalyzer.globalState, parentAnalyzer.inferPredicateState);
        if (parentAnalyzer.isSubquery) {
            this.isSubquery = true;
        }
    }

    /**
     * Analyzer constructor for nested select block with the specified global state.
     */
    private Analyzer(Analyzer parentAnalyzer, GlobalState globalState, InferPredicateState inferPredicateState) {
        ancestors =  Lists.newArrayList(parentAnalyzer);
        ancestors.addAll(parentAnalyzer.ancestors);
        this.globalState = globalState;
        this.inferPredicateState = new InferPredicateState(inferPredicateState);
    }

    /**
     * Returns a new analyzer with the specified parent analyzer but with a new
     * global state.
     */
    public static Analyzer createWithNewGlobalState(Analyzer parentAnalyzer) {
        GlobalState globalState = new GlobalState(parentAnalyzer.globalState.env, parentAnalyzer.getContext());
        return new Analyzer(parentAnalyzer, globalState, new InferPredicateState());
    }

    public void setExternalCtl(String externalCtl) {
        globalState.externalCtl = externalCtl;
    }

    public String getExternalCtl() {
        return globalState.externalCtl;
    }

    public void setIsExplain() {
        globalState.isExplain = true;
    }

    public boolean isExplain() {
        return globalState.isExplain;
    }

    public int incrementCallDepth() {
        return ++callDepth;
    }

    public int decrementCallDepth() {
        return --callDepth;
    }

    public int getCallDepth() {
        return callDepth;
    }

    public void setInlineView(boolean inlineView) {
        isInlineView = inlineView;
    }

    public boolean isInlineViewAnalyzer() {
        return isInlineView;
    }

    public void setExplicitViewAlias(String alias) {
        explicitViewAlias = alias;
    }

    public void setPrepareStmt(PrepareStmt stmt) {
        prepareStmt = stmt;
    }

    public PrepareStmt getPrepareStmt() {
        return prepareStmt;
    }

    public String getExplicitViewAlias() {
        return explicitViewAlias;
    }

    /**
     * Registers a local view definition with this analyzer. Throws an exception if a view
     * definition with the same alias has already been registered or if the number of
     * explicit column labels is greater than the number of columns in the view statement.
     */
    public void registerLocalView(View view) throws AnalysisException {
        Preconditions.checkState(view.isLocalView());
        if (view.hasColLabels()) {
            List<String> viewLabels = view.getColLabels();
            List<String> queryStmtLabels = view.getQueryStmt().getColLabels();
            if (viewLabels.size() > queryStmtLabels.size()) {
                throw new AnalysisException("WITH-clause view '" + view.getName()
                        + "' returns " + queryStmtLabels.size() + " columns, but "
                        + viewLabels.size() + " labels were specified. The number of column "
                        + "labels must be smaller or equal to the number of returned columns.");
            }
        }
        if (localViews.put(view.getName(), view) != null) {
            throw new AnalysisException(
                    String.format("Duplicate table alias: '%s'", view.getName()));
        }
    }

    /**
     * Create query global parameters to be set in each TPlanExecRequest.
     */
    public static TQueryGlobals createQueryGlobals() {
        TQueryGlobals queryGlobals = new TQueryGlobals();
        Calendar currentDate = Calendar.getInstance();
        LocalDateTime localDateTime = LocalDateTime.ofInstant(currentDate.toInstant(),
                currentDate.getTimeZone().toZoneId());
        String nowStr = localDateTime.format(TimeUtils.DATETIME_NS_FORMAT);
        queryGlobals.setNowString(nowStr);
        queryGlobals.setNanoSeconds(LocalDateTime.now().getNano());
        return queryGlobals;
    }

    /**
     * Substitute analyzer's internal expressions (conjuncts) with the given
     * substitution map
     */
    public void substitute(ExprSubstitutionMap sMap) {
        for (ExprId id : globalState.conjuncts.keySet()) {
            // TODO(dhc): next three lines for subquery
            if (globalState.conjuncts.get(id).substitute(sMap) instanceof BoolLiteral) {
                continue;
            }
            globalState.conjuncts.put(id, (Predicate) globalState.conjuncts.get(id).substitute(sMap));
        }
    }

    public void registerTupleDescriptor(TupleDescriptor desc) {
        tupleByAlias.put(desc.getAlias(), desc);
        for (SlotDescriptor slot : desc.getSlots()) {
            String key = desc.getAlias() + "." + slot.getColumn().getName();
            slotRefMap.put(key, slot);
        }
    }

    /**
     * Creates an returns an empty TupleDescriptor for the given table ref and registers
     * it against all its legal aliases. For tables refs with an explicit alias, only the
     * explicit alias is legal. For tables refs with no explicit alias, the fully-qualified
     * and unqualified table names are legal aliases. Column references against unqualified
     * implicit aliases can be ambiguous, therefore, we register such ambiguous aliases
     * here. Requires that all views have been substituted.
     * Throws if an existing explicit alias or implicit fully-qualified alias
     * has already been registered for another table ref.
     */
    public TupleDescriptor registerTableRef(TableRef ref) throws AnalysisException {
        String uniqueAlias = ref.getUniqueAlias();
        if (uniqueTableAliasSet.contains(uniqueAlias)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NONUNIQ_TABLE, uniqueAlias);
        }
        uniqueTableAliasSet.add(uniqueAlias);

        // If ref has no explicit alias, then the unqualified and the fully-qualified table
        // names are legal implicit aliases. Column references against unqualified implicit
        // aliases can be ambiguous, therefore, we register such ambiguous aliases here.
        String unqualifiedAlias = null;
        String[] aliases = ref.getAliases();
        if (aliases.length > 1) {
            unqualifiedAlias = aliases[1];
            TupleDescriptor tupleDesc = aliasMap.get(unqualifiedAlias);
            if (tupleDesc != null) {
                if (tupleDesc.hasExplicitAlias()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NONUNIQ_TABLE, uniqueAlias);
                } else {
                    ambiguousAliases.add(unqualifiedAlias);
                }
            }
        }

        // Delegate creation of the tuple descriptor to the concrete table ref.
        TupleDescriptor result = ref.createTupleDescriptor(this);
        result.setRef(ref);
        result.setAliases(aliases, ref.hasExplicitAlias());

        // Register all legal aliases.
        for (String alias : aliases) {
            // TODO(zc)
            // aliasMap_.put(alias, result);
            tupleByAlias.put(alias, result);
        }

        tableRefMap.put(result.getId(), ref);

        // for mark join, init three context
        //   1. markTuples to records all tuples belong to mark slot
        //   2. markTupleIdByInnerRef to records relationship between inner table of mark join and the mark tuple
        //   3. markTupleIdsNotProcessed to records un-process mark tuple id. if an expr contains slot belong to
        //        the un-process mark tuple, it should not assign to current join node and should pop up its
        //        mark slot until all mark tuples in this expr has been processed.
        if (ref.getJoinOp() != null && ref.isMark()) {
            TupleDescriptor markTuple = getDescTbl().createTupleDescriptor();
            markTuple.setAliases(new String[]{ref.getMarkTupleName()}, true);
            globalState.markTuples.put(ref.getMarkTupleName(), markTuple);
            globalState.markTupleIdByInnerRef.put(ref, markTuple.getId());
            globalState.markTupleIdsNotProcessed.add(markTuple.getId());
        }

        return result;
    }

    /**
     * Create an new tuple descriptor for the given table, register all table columns.
     * Using this method requires external table read locks in advance.
     */
    public TupleDescriptor registerOlapTable(Table table, TableName tableName, List<String> partitions) {
        TableRef ref = new TableRef(tableName, null, partitions == null ? null : new PartitionNames(false, partitions));
        BaseTableRef tableRef = new BaseTableRef(ref, table, tableName);
        TupleDescriptor result = globalState.descTbl.createTupleDescriptor();
        result.setTable(table);
        result.setRef(tableRef);
        result.setAliases(tableRef.getAliases(), ref.hasExplicitAlias());
        for (Column col : table.getBaseSchema(true)) {
            SlotDescriptor slot = globalState.descTbl.addSlotDescriptor(result);
            slot.setIsMaterialized(true);
            slot.setColumn(col);
            slot.setIsNullable(col.isAllowNull());
            String key = tableRef.aliases[0] + "." + col.getName();
            slotRefMap.put(key, slot);

            if (col.getType().isVariantType()) {
                LOG.debug("add subColumnSlotRefMap, key {}, column {}", key, col.toThrift());
                subColumnSlotRefMap.put(key, Maps.newTreeMap(
                    new Comparator<List<String>>() {
                        public int compare(List<String> lst1, List<String> lst2) {
                            String str1 = String.join(".", lst1);
                            String str2 = String.join(".", lst2);
                            return str1.compareTo(str2);
                        }
                    }));
            }
        }
        globalState.descTbl.computeStatAndMemLayout();
        tableRefMap.put(result.getId(), ref);
        for (String alias : tableRef.getAliases()) {
            tupleByAlias.put(alias, result);
        }
        return result;
    }

    public List<TupleId> getAllTupleIds() {
        return new ArrayList<>(tableRefMap.keySet());
    }

    /**
     * Resolves the given TableRef into a concrete BaseTableRef, ViewRef or
     * CollectionTableRef. Returns the new resolved table ref or the given table
     * ref if it is already resolved.
     * Registers privilege requests and throws an AnalysisException if the tableRef's
     * path could not be resolved. The privilege requests are added to ensure that
     * an AuthorizationException is preferred over an AnalysisException so as not to
     * accidentally reveal the non-existence of tables/databases.
     *
     * TODO(zc): support collection table ref
     */
    public TableRef resolveTableRef(TableRef tableRef) throws AnalysisException {
        // Return the table if it is already resolved.
        if (tableRef.isResolved()) {
            return tableRef;
        }
        // Try to find a matching local view.
        TableName tableName = tableRef.getName();
        if (StringUtils.isNotEmpty(this.globalState.externalCtl)
                && StringUtils.isEmpty(tableName.getCtl())) {
            tableName.setCtl(this.globalState.externalCtl);
        }
        if (!tableName.isFullyQualified()) {
            // Searches the hierarchy of analyzers bottom-up for a registered local view with
            // a matching alias.
            String viewAlias = tableName.getTbl();
            Analyzer analyzer = this;
            do {
                View localView = analyzer.localViews.get(viewAlias);
                if (localView != null) {
                    return new InlineViewRef(localView, tableRef);
                }
                analyzer = (analyzer.ancestors.isEmpty() ? null : analyzer.ancestors.get(0));
            } while (analyzer != null);
        }

        // Resolve the table ref's path and determine what resolved table ref
        // to replace it with.
        tableName.analyze(this);

        DatabaseIf database = globalState.env.getCatalogMgr().getCatalogOrAnalysisException(tableName.getCtl())
                .getDbOrAnalysisException(tableName.getDb());
        TableIf table = database.getTableOrAnalysisException(tableName.getTbl());

        if (table.getType() == TableType.OLAP && (((OlapTable) table).getState() == OlapTableState.RESTORE
                || ((OlapTable) table).getState() == OlapTableState.RESTORE_WITH_LOAD)) {
            Boolean isNotRestoring = ((OlapTable) table).getPartitions().stream()
                    .filter(partition -> partition.getState() == PartitionState.RESTORE).collect(Collectors.toList())
                    .isEmpty();

            if (!isNotRestoring) {
                // if doing restore with partitions, the status check push down to OlapScanNode::computePartitionInfo to
                // support query that partitions is not restoring.
            } else {
                // if doing restore with table, throw exception here
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_STATE, "RESTORING");
            }
        }

        // Now hms table only support a bit of table kinds in the whole hive system.
        // So Add this strong checker here to avoid some undefine behaviour in doris.
        if (table.getType() == TableType.HMS_EXTERNAL_TABLE) {
            if (!((HMSExternalTable) table).isSupportedHmsTable()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NONSUPPORT_HMS_TABLE,
                        table.getName(),
                        ((HMSExternalTable) table).getDbName(),
                        tableName.getCtl());
            }
            if (Config.enable_query_hive_views) {
                if (((HMSExternalTable) table).isView()
                        && StringUtils.isNotEmpty(((HMSExternalTable) table).getViewText())) {
                    View hmsView = new View(table.getId(), table.getName(), table.getFullSchema());
                    hmsView.setInlineViewDefWithSqlMode(((HMSExternalTable) table).getViewText(),
                            ConnectContext.get().getSessionVariable().getSqlMode());
                    InlineViewRef inlineViewRef = new InlineViewRef(hmsView, tableRef);
                    if (StringUtils.isNotEmpty(tableName.getCtl())) {
                        inlineViewRef.setExternalCtl(tableName.getCtl());
                    }
                    return inlineViewRef;
                }
            }
        }

        // tableName.getTbl() stores the table name specified by the user in the from statement.
        // In the case of case-sensitive table names, the value of tableName.getTbl() is the same as table.getName().
        // However, since the system view is not case-sensitive, table.getName() gets the lowercase view name,
        // which may not be the same as the user's reference to the table name, causing the table name not to be found
        // in registerColumnRef(). So here the tblName is constructed using tableName.getTbl()
        // instead of table.getName().
        TableName tblName = new TableName(tableName.getCtl(), tableName.getDb(), tableName.getTbl());
        if (table instanceof View) {
            return new InlineViewRef((View) table, tableRef);
        } else {
            // The table must be a base table.
            return new BaseTableRef(tableRef, table, tblName);
        }
    }

    public TableIf getTableOrAnalysisException(TableName tblName) throws AnalysisException {
        DatabaseIf db = globalState.env.getCatalogMgr().getCatalogOrAnalysisException(tblName.getCtl())
                .getDbOrAnalysisException(tblName.getDb());
        return db.getTableOrAnalysisException(tblName.getTbl());
    }

    public ExprRewriter getExprRewriter() {
        return globalState.exprRewriter;
    }

    public ExprRewriter getMVExprRewriter() {
        return globalState.mvExprRewriter;
    }

    /**
     * Return descriptor of registered table/alias.
     *
     * @param name
     * @return null if not registered.
     */
    public Collection<TupleDescriptor> getDescriptor(TableName name) {
        return tupleByAlias.get(name.toString());
    }

    public TupleDescriptor getTupleDesc(TupleId id) {
        return globalState.descTbl.getTupleDesc(id);
    }

    public SlotDescriptor getSlotDesc(SlotId id) {
        return globalState.descTbl.getSlotDesc(id);
    }

    /**
     * Given a "table alias"."column alias", return the SlotDescriptor
     *
     * @param qualifiedColumnName table qualified column name
     */
    public SlotDescriptor getSlotDescriptor(String qualifiedColumnName) {
        return slotRefMap.get(qualifiedColumnName);
    }

    /**
     * Checks that 'col' references an existing column for a registered table
     * alias; if alias is empty, tries to resolve the column name in the context
     * of any of the registered tables. Creates and returns an empty
     * SlotDescriptor if the column hasn't previously been registered, otherwise
     * returns the existing descriptor.
     *
     * @param tblName
     * @param colName
     * @throws AnalysisException
     */
    public SlotDescriptor registerColumnRef(TableName tblName, String colName, List<String> subColNames)
                throws AnalysisException {
        TupleDescriptor d;
        TableName newTblName = tblName;
        if (newTblName == null) {
            d = resolveColumnRef(colName);
        } else {
            d = resolveColumnRef(newTblName, colName);
            //in reanalyze, the inferred expr may contain upper level table alias, and the upper level alias has not
            // been PROCESSED. So we resolve this column without tbl name.
            // For example: a view V "select * from t where t.a>1"
            // sql: select * from V as t1 join V as t2 on t1.a=t2.a and t1.a in (1,2)
            // after first analyze, sql becomes:
            //  select * from V as t1 join V as t2 on t1.a=t2.a and t1.a in (1,2) and t2.a in (1, 2)
            // in reanalyze, when we process V as t2, we indeed process sql like this:
            //    select * from t where t.a>1 and t2.a in (1, 2)
            //  in order to resolve t2.a, we have to ignore "t2"
            // ===================================================
            // Someone may concern that if t2 is not alias of t, this fix will cause incorrect resolve. In fact,
            // this does not happen, since we push t2.a in (1.2) down to this inline view, t2 must be alias of t.
            // create table tmp_can_drop_t1 (
            //     cust_id varchar(96),
            //     user_id varchar(96)
            // )
            // create table tmp_can_drop_t2 (
            //     cust_id varchar(96),
            //     usr_id varchar(96)
            // )
            // select
            // a.cust_id,
            // a.usr_id
            // from (
            // select
            //     a.cust_id,
            //     a.usr_id, --------->(report error, because there is no user_id column in tmp_can_drop_t1)
            //     a.user_id
            // from tmp_can_drop_t1 a
            // full join (
            //     select
            //     cust_id,
            //     usr_id
            //     from
            //     tmp_can_drop_t2
            // ) b
            // on b.cust_id = a.cust_id
            // ) a;
            if (d == null && isInlineView && newTblName.getTbl().equals(explicitViewAlias)
                    && !tupleByAlias.containsKey(newTblName.getTbl())) {
                d = resolveColumnRef(colName);
            }
        }
        /*
         * Now, we only support the columns in the subquery to associate the outer query columns in parent level.
         * If the level of the association exceeds one level, the associated columns in subquery could not be resolved.
         * For example:
         * Select k1 from table a where k1=(select k1 from table b where k1=(select k1 from table c where a.k1=k1));
         * The inner subquery: select k1 from table c where a.k1=k1;
         * There is a associated column (a.k1) which belongs to the outer query appears in the inner subquery.
         * This column could not be resolved because doris can only resolved the parent column instead of grandpa.
         * The exception to this query like that: Unknown column 'k1' in 'a'
         */
        if (d == null && hasAncestors() && isSubquery && isFirstScopeInSubquery) {
            // analyzer father for subquery
            if (newTblName == null) {
                d = getParentAnalyzer().resolveColumnRef(colName);
            } else {
                d = getParentAnalyzer().resolveColumnRef(newTblName, colName);
            }
        }
        if (d == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, colName,
                                                newTblName == null ? "table list" : newTblName.toString());
        }

        Column col = d.getTable() == null ? new Column(colName, ScalarType.BOOLEAN) : d.getTable().getColumn(colName);
        if (col == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, colName,
                                                newTblName == null ? d.getTable().getName() : newTblName.toString());
        }

        LOG.debug("register column ref table {}, colName {}, col {}", tblName, colName, col.toSql());
        if (col.getType().isVariantType() || (subColNames != null && !subColNames.isEmpty())) {
            if (!col.getType().isVariantType()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_ILLEGAL_COLUMN_REFERENCE_ERROR,
                        Joiner.on(".").join(tblName.getTbl(), colName));
            }
            if (subColNames == null) {
                // Root
                subColNames = new ArrayList<String>();
            }
            String key = d.getAlias() + "." + col.getName();
            if (subColumnSlotRefMap.get(key) == null) {
                subColumnSlotRefMap.put(key, Maps.newTreeMap(
                    new Comparator<List<String>>() {
                        public int compare(List<String> lst1, List<String> lst2) {
                            String str1 = String.join(".", lst1);
                            String str2 = String.join(".", lst2);
                            return str1.compareTo(str2);
                        }
                    }));
            }
            SlotDescriptor result = subColumnSlotRefMap.get(key).get(subColNames);
            if (result != null) {
                // avoid duplicate slots
                return result;
            }
            result = globalState.descTbl.addSlotDescriptor(d);
            LOG.debug("register slot descriptor {}", result);
            result.setSubColLables(subColNames);
            result.setColumn(col);
            if (!subColNames.isEmpty()) {
                result.setMaterializedColumnName(col.getName() + "." + String.join(".", subColNames));
            }
            result.setIsMaterialized(true);
            result.setIsNullable(col.isAllowNull());
            subColumnSlotRefMap.get(key).put(subColNames, result);
            return result;
        }

        // Make column name case insensitive
        String key = d.getAlias() + "." + col.getName();
        SlotDescriptor result = slotRefMap.get(key);
        if (result != null) {
            return result;
        }
        result = globalState.descTbl.addSlotDescriptor(d);
        result.setColumn(col);
        boolean isNullable;
        isNullable = col.isAllowNull();
        result.setIsNullable(isNullable);

        slotRefMap.put(key, result);
        return result;
    }

    /**
     * Register a virtual column, and it is not a real column exist in table,
     * so it does not need to resolve. now virtual slot: only use in grouping set to generate grouping id,
     * so it should always is not nullable
     */
    public SlotDescriptor registerVirtualColumnRef(String colName, Type type, TupleDescriptor tupleDescriptor)
            throws AnalysisException {
        // Make column name case insensitive
        String key = colName;
        SlotDescriptor result = slotRefMap.get(key);
        if (result != null) {
            return result;
        }
        result = addSlotDescriptor(tupleDescriptor);
        Column col = new Column(colName, type);
        result.setColumn(col);
        result.setIsNullable(col.isAllowNull());
        slotRefMap.put(key, result);
        return result;
    }

    /**
     * Resolves column name in context of any of the registered table aliases.
     * Returns null if not found or multiple bindings to different tables exist,
     * otherwise returns the table alias.
     */
    private TupleDescriptor resolveColumnRef(TableName tblName, String colName) throws AnalysisException {
        TupleDescriptor result = null;
        // find table all name
        for (TupleDescriptor desc : tupleByAlias.get(tblName.toString())) {
            //result = desc;
            if (!colName.equalsIgnoreCase(Column.DELETE_SIGN) && !isVisible(desc.getId())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_ILLEGAL_COLUMN_REFERENCE_ERROR,
                        Joiner.on(".").join(tblName.getTbl(), colName));
            }
            Column col = desc.getTable().getColumn(colName);
            if (col != null) {
                if (result != null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NON_UNIQ_ERROR, colName);
                }
                result = desc;
            }
        }

        return result != null ? result : globalState.markTuples.get(tblName.toString());
    }

    private TupleDescriptor resolveColumnRef(String colName) throws AnalysisException {
        TupleDescriptor result = null;
        for (TupleDescriptor desc : tupleByAlias.values()) {
            if (!isVisible(desc.getId())) {
                continue;
            }
            Column col = desc.getTable().getColumn(colName);
            if (col != null) {
                if (result != null) {
                    if (result != desc) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_NON_UNIQ_ERROR, colName);
                    }
                } else {
                    result = desc;
                }
            }
        }
        return result;
    }

    /**
     * Creates a new slot descriptor and related state in globalState.
     */
    public SlotDescriptor addSlotDescriptor(TupleDescriptor tupleDesc) {
        SlotDescriptor result = globalState.descTbl.addSlotDescriptor(tupleDesc);
        globalState.blockBySlot.put(result.getId(), this);
        return result;
    }

    /**
     * Adds a new slot descriptor in tupleDesc that is identical to srcSlotDesc
     * except for the path and slot id.
     */
    public SlotDescriptor copySlotDescriptor(SlotDescriptor srcSlotDesc, TupleDescriptor tupleDesc) {
        SlotDescriptor result = globalState.descTbl.addSlotDescriptor(tupleDesc);
        globalState.blockBySlot.put(result.getId(), this);
        // result.setSourceExprs(srcSlotDesc.getSourceExprs());
        // result.setLabel(srcSlotDesc.getLabel());
        result.setStats(srcSlotDesc.getStats());
        result.setType(srcSlotDesc.getType());
        result.setIsNullable(srcSlotDesc.getIsNullable());
        if (srcSlotDesc.getColumn() != null) {
            result.setColumn(srcSlotDesc.getColumn());
        }
        // result.setItemTupleDesc(srcSlotDesc.getItemTupleDesc());
        return result;
    }

    public void registerInlineViewTupleId(TupleId tupleId) {
        globalState.inlineViewTupleIds.add(tupleId);
    }

    /**
     * Register conjuncts that are outer joined by a full outer join. For a given
     * predicate, we record the last full outer join that outer-joined any of its
     * tuple ids. We need this additional information because full-outer joins obey
     * different rules with respect to predicate pushdown compared to left and right
     * outer joins.
     */
    public void registerFullOuterJoinedConjunct(Expr e) {
        Preconditions.checkState(
                !globalState.fullOuterJoinedConjuncts.containsKey(e.getId()));
        List<TupleId> tids = Lists.newArrayList();
        e.getIds(tids, null);
        for (TupleId tid : tids) {
            if (!globalState.fullOuterJoinedTupleIds.containsKey(tid)) {
                continue;
            }
            TableRef currentOuterJoin = globalState.fullOuterJoinedTupleIds.get(tid);
            globalState.fullOuterJoinedConjuncts.put(e.getId(), currentOuterJoin);
            break;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("registerFullOuterJoinedConjunct: " + globalState.fullOuterJoinedConjuncts);
        }
    }

    /**
     * Register tids as being outer-joined by a full outer join clause represented by
     * rhsRef.
     */
    public void registerFullOuterJoinedTids(List<TupleId> tids, TableRef rhsRef) {
        for (TupleId tid : tids) {
            globalState.fullOuterJoinedTupleIds.put(tid, rhsRef);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("registerFullOuterJoinedTids: " + globalState.fullOuterJoinedTupleIds);
        }
    }

    /**
     * Register tids as being outer-joined by Join clause represented by rhsRef.
     * All tuple of outer join should be null in slot desc
     */
    public void registerOuterJoinedTids(List<TupleId> tids, TableRef rhsRef) {
        for (TupleId tid : tids) {
            globalState.outerJoinedTupleIds.put(tid, rhsRef);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("registerOuterJoinedTids: " + globalState.outerJoinedTupleIds);
        }
    }

    public void registerOuterJoinedRightSideTids(List<TupleId> tids) {
        globalState.outerRightSideJoinTupleIds.addAll(tids);
    }

    public void registerOuterJoinedLeftSideTids(List<TupleId> tids) {
        globalState.outerLeftSideJoinTupleIds.addAll(tids);
    }



    /**
     * Register the given tuple id as being the invisible side of a semi-join.
     */
    public void registerSemiJoinedTid(TupleId tid, TableRef rhsRef) {
        globalState.semiJoinedTupleIds.put(tid, rhsRef);
    }

    /**
     * Register the relationship between any two tables
     */
    public void registerAnyTwoTalesJoinOperator(Pair<TupleId, TupleId> tids, JoinOperator joinOperator) {
        if (joinOperator == null) {
            joinOperator = JoinOperator.INNER_JOIN;
        }
        inferPredicateState.anyTwoTalesJoinOperator.put(tids, joinOperator);
    }

    public void registerOnSlotEqSlotExpr(Expr expr) {
        inferPredicateState.onSlotEqSlotExpr.add(expr);
    }

    public void registerOnSlotEqSlotDeDuplication(Pair<Expr, Expr> pair) {
        inferPredicateState.onSlotEqSlotDeDuplication.add(pair);
    }

    public void registerOnSlotToLiteralExpr(Expr expr) {
        inferPredicateState.onSlotToLiteralExpr.add(expr);
    }

    public void registerOnSlotToLiteralDeDuplication(Pair<Expr, Expr> pair) {
        inferPredicateState.onSlotToLiteralDeDuplication.add(pair);
    }

    public void registerInExpr(Expr expr) {
        inferPredicateState.onInExpr.add(expr);
    }

    public void registerInDeDuplication(Expr expr) {
        inferPredicateState.onInDeDuplication.add(expr);
    }

    public void registerOnIsNullExpr(Expr expr) {
        inferPredicateState.onIsNullExpr.add(expr);
    }

    public void registerOnIsNullDeDuplication(Expr expr) {
        inferPredicateState.onIsNullDeDuplication.add(expr);
    }

    public void registerGlobalSlotToLiteralDeDuplication(Pair<Expr, Expr> pair) {
        inferPredicateState.globalSlotToLiteralDeDuplication.add(pair);
    }

    public void registerGlobalInDeDuplication(Expr expr) {
        inferPredicateState.globalInDeDuplication.add(expr);
    }

    public void registerConjunct(Expr e, TupleId tupleId) throws AnalysisException {
        final List<Expr> exprs = Lists.newArrayList();
        exprs.add(e);
        registerConjuncts(exprs, tupleId);
    }

    public void registerConjuncts(List<Expr> l, TupleId tupleId) throws AnalysisException {
        final List<TupleId> tupleIds = Lists.newArrayList();
        tupleIds.add(tupleId);
        registerConjuncts(l, tupleIds);
    }

    public void registerConjunct(Expr e, List<TupleId> tupleIds) throws AnalysisException {
        final List<Expr> exprs = Lists.newArrayList();
        exprs.add(e);
        registerConjuncts(exprs, tupleIds);
    }

    // register all conjuncts and handle constant conjuncts with ids
    public void registerConjuncts(List<Expr> l, List<TupleId> ids) throws AnalysisException {
        for (Expr e : l) {
            registerConjuncts(e, true, ids);
        }
    }

    /**
     * Register all conjuncts that make up 'e'. If fromHavingClause is false, this conjunct
     * is assumed to originate from a WHERE or ON clause.
     */
    public void registerConjuncts(Expr e, boolean fromHavingClause) throws AnalysisException {
        registerConjuncts(e, fromHavingClause, null);
    }

    // Register all conjuncts and handle constant conjuncts with ids
    public void registerConjuncts(Expr e, boolean fromHavingClause, List<TupleId> ids) throws AnalysisException {
        for (Expr conjunct : e.getConjuncts()) {
            registerConjunct(conjunct);
            if (ids != null) {
                for (TupleId id : ids) {
                    registerConstantConjunct(id, conjunct);
                }
            }
            markConstantConjunct(conjunct, fromHavingClause, false);
        }
    }

    private void registerConstantConjunct(TupleId id, Expr e) {
        if (id != null && e.isConstant()) {
            Set<Expr> set = globalState.constantConjunct.get(id);
            if (set == null) {
                set = Sets.newHashSet();
                globalState.constantConjunct.put(id, set);
            }
            set.add(e);
        }
    }

    public void registerMigrateFailedConjuncts(InlineViewRef ref, Expr e) throws AnalysisException {
        markConstantConjunct(e, false, false);
        Set<Expr> exprSet = globalState.migrateFailedConjuncts.computeIfAbsent(ref, (k) -> new HashSet<>());
        exprSet.add(e);
    }

    public Set<Expr> findMigrateFailedConjuncts(InlineViewRef inlineViewRef) {
        return globalState.migrateFailedConjuncts.get(inlineViewRef);
    }

    /**
     * register expr id
     * @param expr
     */
    void registerExprId(Expr expr) {
        expr.setId(globalState.conjunctIdGenerator.getNextId());
    }

    /**
     * Register individual conjunct with all tuple and slot ids it references
     * and with the global conjunct list.
     */
    private void registerConjunct(Expr e) {
        // this conjunct would already have an id assigned if it is being re-registered
        // in a subquery analyzer

        e.setId(globalState.conjunctIdGenerator.getNextId());
        globalState.conjuncts.put(e.getId(), e);

        // LOG.info("registered conjunct " + p.getId().toString() + ": " + p.toSql());
        ArrayList<TupleId> tupleIds = Lists.newArrayList();
        ArrayList<SlotId> slotIds = Lists.newArrayList();
        e.getIds(tupleIds, slotIds);
        // register full join conjuncts
        registerFullOuterJoinedConjunct(e);

        // update tuplePredicates
        for (TupleId id : tupleIds) {
            if (!tuplePredicates.containsKey(id)) {
                List<ExprId> conjunctIds = Lists.newArrayList();
                conjunctIds.add(e.getId());
                tuplePredicates.put(id, conjunctIds);
            } else {
                tuplePredicates.get(id).add(e.getId());
            }
        }

        // update slotPredicates
        for (SlotId id : slotIds) {
            if (!slotPredicates.containsKey(id)) {
                List<ExprId> conjunctIds = Lists.newArrayList();
                conjunctIds.add(e.getId());
                slotPredicates.put(id, conjunctIds);
            } else {
                slotPredicates.get(id).add(e.getId());
            }
        }

        // check whether this is an equi-join predicate, ie, something of the
        // form <expr1> = <expr2> where at least one of the exprs is bound by
        // exactly one tuple id
        if (!(e instanceof BinaryPredicate)) {
            return;
        }
        BinaryPredicate binaryPred = (BinaryPredicate) e;
        if (!binaryPred.getOp().isEquivalence()) {
            return;
        }
        if (tupleIds.size() < 2) {
            return;
        }

        // examine children and update eqJoinConjuncts
        for (int i = 0; i < 2; ++i) {
            List<TupleId> lhsTupleIds = Lists.newArrayList();
            binaryPred.getChild(i).getIds(lhsTupleIds, null);
            if (lhsTupleIds.size() == 1) {
                if (!globalState.eqJoinConjuncts.containsKey(lhsTupleIds.get(0))) {
                    List<ExprId> conjunctIds = Lists.newArrayList();
                    conjunctIds.add(e.getId());
                    globalState.eqJoinConjuncts.put(lhsTupleIds.get(0), conjunctIds);
                } else {
                    globalState.eqJoinConjuncts.get(lhsTupleIds.get(0)).add(e.getId());
                }
                binaryPred.setIsEqJoinConjunct(true);
            }
        }
    }

    /**
     * Create and register an auxiliary predicate to express an equivalence
     * between two exprs (BinaryPredicate with EQ); this predicate does not need
     * to be assigned, but it's used for equivalence class computation. Does
     * nothing if the lhs or rhs expr are NULL. Registering an equivalence with
     * NULL would be incorrect, because <expr> = NULL is false (even NULL =
     * NULL).
     */
    public void createAuxEquivPredicate(Expr lhs, Expr rhs) {
        // Check the expr type as well as the class because NullLiteral could
        // have been
        // implicitly cast to a type different than NULL.
        if (lhs instanceof NullLiteral || rhs instanceof NullLiteral
                || lhs.getType().isNull() || rhs.getType().isNull()) {
            return;
        }
        // create an eq predicate between lhs and rhs
        BinaryPredicate p = new BinaryPredicate(BinaryPredicate.Operator.EQ, lhs, rhs);
        p.setIsAuxExpr();
        if (LOG.isDebugEnabled()) {
            LOG.debug("register equiv predicate: " + p.toSql() + " " + p.debugString());
        }
        registerConjunct(p);
    }

    public Set<ExprId> getAssignedConjuncts() {
        return Sets.newHashSet(globalState.assignedConjuncts);
    }

    public void setAssignedConjuncts(Set<ExprId> assigned) {
        if (assigned != null) {
            globalState.assignedConjuncts = Sets.newHashSet(assigned);
        }
    }

    /**
     * Return all unassigned registered conjuncts that are fully bound by the given
     * (logical) tuple ids, can be evaluated by 'tupleIds' and are not tied to an
     * Outer Join clause.
     */
    public List<Expr> getUnassignedConjuncts(List<TupleId> tupleIds) {
        List<Expr> result = Lists.newArrayList();
        for (Expr e : getUnassignedConjuncts(tupleIds, true)) {
            if (canEvalPredicate(tupleIds, e)) {
                result.add(e);
            }
        }
        return result;
    }

    /**
     * Return all unassigned non-constant registered conjuncts that are fully bound by
     * given list of tuple ids. If 'inclOjConjuncts' is false, conjuncts tied to an
     * Outer Join clause are excluded.
     */
    public List<Expr> getUnassignedConjuncts(
            List<TupleId> tupleIds, boolean inclOjConjuncts) {
        List<Expr> result = Lists.newArrayList();
        for (Expr e : globalState.conjuncts.values()) {
            // handle constant conjuncts
            if (e.isConstant()) {
                boolean isBoundByTuple = false;
                for (TupleId id : tupleIds) {
                    final Set<Expr> exprSet = globalState.constantConjunct.get(id);
                    if (exprSet != null && exprSet.contains(e)) {
                        isBoundByTuple = true;
                        break;
                    }
                }
                if (!isBoundByTuple) {
                    continue;
                }
            }
            if (e.isBoundByTupleIds(tupleIds)
                    && !e.isAuxExpr()
                    && (!globalState.assignedConjuncts.contains(e.getId()) || e.isConstant())
                    && ((inclOjConjuncts && !e.isConstant())
                            || (!globalState.ojClauseByConjunct.containsKey(e.getId())
                                    && !globalState.sjClauseByConjunct.containsKey(e.getId())))) {
                result.add(e);
            }
        }
        return result;
    }


    /**
     * Return all registered conjuncts that are fully bound by given list of tuple ids.
     * the eqJoinConjuncts and sjClauseByConjunct is excluded.
     * This method is used get conjuncts which may be able to pushed down to scan node.
     */
    public List<Expr> getConjuncts(List<TupleId> tupleIds) {
        List<Expr> result = Lists.newArrayList();
        List<ExprId> eqJoinConjunctIds = Lists.newArrayList();
        for (List<ExprId> conjuncts : globalState.eqJoinConjuncts.values()) {
            eqJoinConjunctIds.addAll(conjuncts);
        }
        for (Expr e : globalState.conjuncts.values()) {
            if (e.isBoundByTupleIds(tupleIds)
                    && !e.isAuxExpr()
                    && !eqJoinConjunctIds.contains(e.getId())  // to exclude to conjuncts like (A.id = B.id)
                    && !globalState.ojClauseByConjunct.containsKey(e.getId())
                    && !globalState.sjClauseByConjunct.containsKey(e.getId())
                    && canEvalPredicate(tupleIds, e)) {
                result.add(e);
            }
        }
        return result;
    }

    /**
     * Return all unassigned conjuncts of the outer join referenced by
     * right-hand side table ref.
     */
    public List<Expr> getUnassignedOjConjuncts(TableRef ref) {
        Preconditions.checkState(ref.getJoinOp().isOuterJoin());
        List<Expr> result = Lists.newArrayList();
        List<ExprId> candidates = globalState.conjunctsByOjClause.get(ref.getId());
        if (candidates == null) {
            return result;
        }
        for (ExprId conjunctId : candidates) {
            if (!globalState.assignedConjuncts.contains(conjunctId)) {
                Expr e = globalState.conjuncts.get(conjunctId);
                Preconditions.checkState(e != null);
                result.add(e);
            }
        }
        return result;
    }

    /**
     * Return all unassigned conjuncts of the anti join referenced by
     * right-hand side table ref.
     */
    public List<Expr> getUnassignedAntiJoinNullAwareConjuncts(TableRef ref) {
        Preconditions.checkState(ref.getJoinOp().isAntiJoinNullAware());
        List<Expr> result = Lists.newArrayList();
        List<ExprId> candidates = globalState.conjunctsByAntiJoinNullAwareClause.get(ref.getId());
        if (candidates == null) {
            return result;
        }
        for (ExprId conjunctId : candidates) {
            if (!globalState.assignedConjuncts.contains(conjunctId)) {
                Expr e = globalState.conjuncts.get(conjunctId);
                Preconditions.checkState(e != null);
                result.add(e);
            }
        }
        return result;
    }

    public List<Expr> getUnassignedSemiAntiJoinNoNullAwareConjuncts(TableRef ref) {
        Preconditions.checkState(ref.getJoinOp().isSemiOrAntiJoinNoNullAware());
        List<Expr> result = Lists.newArrayList();
        List<ExprId> candidates = globalState.conjunctsBySemiAntiJoinNoNullAwareClause.get(ref.getId());
        if (candidates == null) {
            return result;
        }
        for (ExprId conjunctId : candidates) {
            if (!globalState.assignedConjuncts.contains(conjunctId)) {
                Expr e = globalState.conjuncts.get(conjunctId);
                Preconditions.checkState(e != null);
                result.add(e);
            }
        }
        return result;
    }

    /**
     * Returns true if 'e' must be evaluated after or by a join node. Note that it may
     * still be safe to evaluate 'e' elsewhere as well, but in any case 'e' must be
     * evaluated again by or after a join.
     */
    public boolean evalAfterJoin(Expr e) {
        List<TupleId> tids = Lists.newArrayList();
        e.getIds(tids, null);
        if (tids.isEmpty()) {
            return false;
        }
        if (tids.size() > 1 || isOjConjunct(e) || isFullOuterJoined(e)
                || (isOuterJoined(tids.get(0))
                && (!e.isOnClauseConjunct() || isIjConjunct(e)))
                || (isAntiJoinedConjunct(e) && !isSemiJoined(tids.get(0)))) {
            return true;
        }
        return false;
    }

    /**
     * Returns the fully-qualified table name of tableName. If tableName
     * is already fully qualified, returns tableName.
     */
    public TableName getFqTableName(TableName tableName) {
        if (Strings.isNullOrEmpty(tableName.getCtl())) {
            tableName.setCtl(getDefaultCatalog());
        }
        if (Strings.isNullOrEmpty(tableName.getDb())) {
            tableName.setDb(getDefaultDb());
        }
        return tableName;
    }

    public TupleId getTupleId(SlotId slotId) {
        return globalState.descTbl.getSlotDesc(slotId).getParent().getId();
    }

    /**
     * Return rhs ref of last Join clause that outer-joined id.
     */
    public TableRef getLastOjClause(TupleId id) {
        return globalState.outerJoinedTupleIds.get(id);
    }

    /**
     * Return JoinOperator between two tables
     */
    public JoinOperator getAnyTwoTablesJoinOp(Pair<TupleId, TupleId> tids) {
        return inferPredicateState.anyTwoTalesJoinOperator.get(tids);
    }

    public boolean isContainTupleIds(Pair<TupleId, TupleId> tids) {
        return inferPredicateState.anyTwoTalesJoinOperator.containsKey(tids);
    }

    public boolean isWhereClauseConjunct(Expr e) {
        return whereClauseConjuncts.contains(e.getId());
    }

    public boolean isSemiJoined(TupleId tid) {
        return globalState.semiJoinedTupleIds.containsKey(tid);
    }

    public boolean isAntiJoinedConjunct(Expr e) {
        return getAntiJoinRef(e) != null;
    }

    public TableRef getAntiJoinRef(Expr e) {
        TableRef tblRef = globalState.sjClauseByConjunct.get(e.getId());
        if (tblRef == null) {
            return null;
        }
        return (tblRef.getJoinOp().isAntiJoin()) ? tblRef : null;
    }

    public boolean isAntiJoinedNullAwareConjunct(Expr e) {
        return getAntiJoinNullAwareRef(e) != null;
    }

    private TableRef getAntiJoinNullAwareRef(Expr e) {
        TableRef tblRef = globalState.sjClauseByConjunct.get(e.getId());
        if (tblRef == null) {
            return null;
        }
        return (tblRef.getJoinOp().isAntiJoinNullAware()) ? tblRef : null;
    }

    public boolean isAntiJoinedNoNullAwareConjunct(Expr e) {
        return getAntiJoinNoNullAwareRef(e) != null;
    }

    public TableRef getAntiJoinNoNullAwareRef(Expr e) {
        TableRef tblRef = globalState.sjClauseByConjunct.get(e.getId());
        if (tblRef == null) {
            return null;
        }
        return (tblRef.getJoinOp().isAntiJoinNoNullAware()) ? tblRef : null;
    }

    public boolean isFullOuterJoined(TupleId tid) {
        return globalState.fullOuterJoinedTupleIds.containsKey(tid);
    }

    public boolean isFullOuterJoined(SlotId sid) {
        return isFullOuterJoined(getTupleId(sid));
    }

    public boolean isVisible(TupleId tid) {
        return tid == visibleSemiJoinedTupleId || !isSemiJoined(tid);
    }

    public boolean containsOuterJoinedTid(List<TupleId> tids) {
        for (TupleId tid : tids) {
            if (isOuterJoined(tid)) {
                return true;
            }
        }
        return false;
    }

    public DescriptorTable getDescTbl() {
        return globalState.descTbl;
    }

    public Env getEnv() {
        return globalState.env;
    }

    public Set<String> getAliases() {
        return uniqueTableAliasSet;
    }

    public List<Expr> getAllConjuncts(TupleId id) {
        List<ExprId> conjunctIds = tuplePredicates.get(id);
        if (conjunctIds == null) {
            return null;
        }
        List<Expr> result = Lists.newArrayList();
        for (ExprId conjunctId : conjunctIds) {
            Expr e = globalState.conjuncts.get(conjunctId);
            Preconditions.checkState(e != null);
            result.add(e);
        }
        return result;
    }

    public boolean needPopUpMarkTuple(TableRef ref) {
        TupleId id = globalState.markTupleIdByInnerRef.get(ref);
        if (id == null) {
            return false;
        }
        List<Expr> exprs = getAllConjuncts(id);
        for (Expr expr : exprs) {
            List<TupleId> tupleIds = Lists.newArrayList();
            expr.getIds(tupleIds, null);
            if (tupleIds.stream().anyMatch(globalState.markTupleIdsNotProcessed::contains)) {
                return true;
            }
        }
        return false;
    }

    public List<Expr> getMarkConjuncts(TableRef ref) {
        TupleId id = globalState.markTupleIdByInnerRef.get(ref);
        if (id == null) {
            return Collections.emptyList();
        }
        globalState.markTupleIdsNotProcessed.remove(id);
        List<Expr> retExprs = Lists.newArrayList();
        List<Expr> exprs = getAllConjuncts(id);
        for (Expr expr : exprs) {
            List<TupleId> tupleIds = Lists.newArrayList();
            expr.getIds(tupleIds, null);
            if (tupleIds.stream().noneMatch(globalState.markTupleIdsNotProcessed::contains)) {
                retExprs.add(expr);
            }
        }
        return retExprs;
    }

    public TupleDescriptor getMarkTuple(TableRef ref) {
        TupleDescriptor markTuple = globalState.descTbl.getTupleDesc(globalState.markTupleIdByInnerRef.get(ref));
        if (markTuple != null) {
            markTuple.setIsMaterialized(true);
            markTuple.getSlots().forEach(s -> s.setIsMaterialized(true));
        }
        return markTuple;
    }

    public List<Expr> getMarkConjuncts() {
        List<Expr> exprs = Lists.newArrayList();
        List<TupleId> markIds = Lists.newArrayList(globalState.markTupleIdByInnerRef.values());
        for (Expr e : globalState.conjuncts.values()) {
            List<TupleId> tupleIds = Lists.newArrayList();
            e.getIds(tupleIds, null);
            if (!Collections.disjoint(markIds, tupleIds)) {
                exprs.add(e);
            }
        }
        return exprs;
    }

    /**
     * Get all predicates belonging to one or more tuples that have not yet been assigned
     * Since these predicates will be assigned by upper-level plan nodes in the future,
     * the columns associated with these predicates will also be required by upper-level nodes.
     * So these columns should be projected in the table function node.
     */
    public List<Expr> getRemainConjuncts(List<TupleId> tupleIds) {
        Set<ExprId> remainConjunctIds = Sets.newHashSet();
        for (TupleId tupleId : tupleIds) {
            if (tuplePredicates.get(tupleId) != null) {
                remainConjunctIds.addAll(tuplePredicates.get(tupleId));
            }
        }
        remainConjunctIds.removeAll(globalState.assignedConjuncts);
        List<Expr> result = Lists.newArrayList();
        for (ExprId conjunctId : remainConjunctIds) {
            Expr e = globalState.conjuncts.get(conjunctId);
            Preconditions.checkState(e != null);
            if (e.isAuxExpr()) {
                continue;
            }
            result.add(e);
        }
        return result;
    }

    public List<Expr> getOnSlotEqSlotExpr() {
        return new ArrayList<>(inferPredicateState.onSlotEqSlotExpr);
    }

    public Set<Pair<Expr, Expr>> getOnSlotEqSlotDeDuplication() {
        return Sets.newHashSet(inferPredicateState.onSlotEqSlotDeDuplication);
    }

    public List<Expr> getOnSlotToLiteralExpr() {
        return new ArrayList<>(inferPredicateState.onSlotToLiteralExpr);
    }

    public Set<Pair<Expr, Expr>> getOnSlotToLiteralDeDuplication() {
        return Sets.newHashSet(inferPredicateState.onSlotToLiteralDeDuplication);
    }

    public List<Expr> getInExpr() {
        return new ArrayList<>(inferPredicateState.onInExpr);
    }

    public Set<Expr> getInDeDuplication() {
        return Sets.newHashSet(inferPredicateState.onInDeDuplication);
    }

    public List<Expr> getOnIsNullExpr() {
        return new ArrayList<>(inferPredicateState.onIsNullExpr);
    }

    public Set<Expr> getOnIsNullDeDuplication() {
        return Sets.newHashSet(inferPredicateState.onIsNullDeDuplication);
    }

    public Set<Pair<Expr, Expr>> getGlobalSlotToLiteralDeDuplication() {
        return Sets.newHashSet(inferPredicateState.globalSlotToLiteralDeDuplication);
    }

    public Set<Expr> getGlobalInDeDuplication() {
        return Sets.newHashSet(inferPredicateState.globalInDeDuplication);
    }

    /**
     * Makes the given semi-joined tuple visible such that its slots can be referenced.
     * If tid is null, makes the currently visible semi-joined tuple invisible again.
     */
    public void setVisibleSemiJoinedTuple(TupleId tid) {
        Preconditions.checkState(tid == null
                || globalState.semiJoinedTupleIds.containsKey(tid));
        Preconditions.checkState(tid == null || visibleSemiJoinedTupleId == null);
        visibleSemiJoinedTupleId = tid;
    }

    /**
     * Return true if this analyzer has no ancestors. (i.e. false for the analyzer created
     * for inline views/ union operands, etc.)
     */
    public boolean isRootAnalyzer() {
        return ancestors.isEmpty();
    }

    public boolean hasAncestors() {
        return !ancestors.isEmpty();
    }

    public Analyzer getParentAnalyzer() {
        return hasAncestors() ? ancestors.get(0) : null;
    }

    /**
     * Returns true if the query block corresponding to this analyzer is guaranteed
     * to return an empty result set, e.g., due to a limit 0 or a constant predicate
     * that evaluates to false.
     */
    public boolean hasEmptyResultSet() {
        return hasEmptyResultSet;
    }

    public void setHasEmptyResultSet() {
        hasEmptyResultSet = true;
    }

    public boolean hasEmptySpjResultSet() {
        return hasEmptySpjResultSet;
    }

    /**
     * Register all conjuncts in 'conjuncts' that make up the On-clause of the given
     * right-hand side of a join. Assigns each conjunct a unique id. If rhsRef is
     * the right-hand side of an outer join, then the conjuncts conjuncts are
     * registered such that they can only be evaluated by the node implementing that
     * join.
     */
    public void registerOnClauseConjuncts(List<Expr> conjuncts, TableRef rhsRef)
            throws AnalysisException {
        Preconditions.checkNotNull(rhsRef);
        Preconditions.checkNotNull(conjuncts);
        List<ExprId> ojConjuncts = null;
        if (rhsRef.getJoinOp().isOuterJoin()) {
            ojConjuncts = globalState.conjunctsByOjClause.get(rhsRef.getId());
            if (ojConjuncts == null) {
                ojConjuncts = Lists.newArrayList();
                globalState.conjunctsByOjClause.put(rhsRef.getId(), ojConjuncts);
            }
        }
        for (Expr conjunct : conjuncts) {
            conjunct.setIsOnClauseConjunct(true);
            registerConjunct(conjunct);
            if (rhsRef.getJoinOp().isOuterJoin()) {
                globalState.ojClauseByConjunct.put(conjunct.getId(), rhsRef);
                ojConjuncts.add(conjunct.getId());
            }
            if (rhsRef.getJoinOp().isSemiAntiJoin()) {
                globalState.sjClauseByConjunct.put(conjunct.getId(), rhsRef);
                if (rhsRef.getJoinOp().isAntiJoinNullAware()) {
                    globalState.conjunctsByAntiJoinNullAwareClause.computeIfAbsent(rhsRef.getId(),
                            k -> Lists.newArrayList()).add(conjunct.getId());
                } else {
                    globalState.conjunctsBySemiAntiJoinNoNullAwareClause.computeIfAbsent(rhsRef.getId(),
                            k -> Lists.newArrayList()).add(conjunct.getId());
                }
            }
            if (rhsRef.getJoinOp().isInnerJoin()) {
                globalState.ijClauseByConjunct.put(conjunct.getId(), rhsRef);
            }
            markConstantConjunct(conjunct, false, true);
        }
    }

    /**
     * If the given conjunct is a constant non-oj conjunct, marks it as assigned, and
     * evaluates the conjunct. If the conjunct evaluates to false, marks this query
     * block as having an empty result set or as having an empty select-project-join
     * portion, if fromHavingClause is true or false, respectively.
     * No-op if the conjunct is not constant or is outer joined.
     * Throws an AnalysisException if there is an error evaluating `conjunct`
     */
    private void markConstantConjunct(Expr conjunct, boolean fromHavingClause, boolean join)
            throws AnalysisException {
        if (!conjunct.isConstant() || isOjConjunct(conjunct) || join) {
            return;
        }
        if ((!fromHavingClause && !hasEmptySpjResultSet)
                || (fromHavingClause && !hasEmptyResultSet)) {
            try {
                if (conjunct instanceof BetweenPredicate) {
                    // Rewrite the BetweenPredicate into a CompoundPredicate so we can evaluate it
                    // below (BetweenPredicates are not executable). We might be in the first
                    // analysis pass, so the conjunct may not have been rewritten yet.
                    ExprRewriter rewriter = new ExprRewriter(BetweenToCompoundRule.INSTANCE);
                    conjunct = rewriter.rewrite(conjunct, this);
                    // analyze this conjunct here: we know it can't contain references to select list
                    // aliases and having it analyzed is needed for the following EvalPredicate() call
                    conjunct.analyze(this);
                }
                // getResultValue will modify the conjunct internally
                // we have to use a clone to keep conjunct unchanged
                Expr newConjunct = conjunct.clone().getResultValue(true);
                newConjunct = FoldConstantsRule.INSTANCE.apply(newConjunct, this, null);
                if (newConjunct instanceof BoolLiteral || newConjunct instanceof NullLiteral) {
                    boolean evalResult = true;
                    if (newConjunct instanceof BoolLiteral) {
                        evalResult = ((BoolLiteral) newConjunct).getValue();
                    } else {
                        evalResult = false;
                    }
                    if (fromHavingClause) {
                        hasEmptyResultSet = !evalResult;
                    } else {
                        if (isAntiJoinedNoNullAwareConjunct(conjunct)) {
                            hasEmptySpjResultSet = evalResult;
                        } else {
                            hasEmptySpjResultSet = !evalResult;
                        }
                    }
                    if (hasEmptyResultSet || hasEmptySpjResultSet) {
                        markConjunctAssigned(conjunct);
                    }
                }
            } catch (AnalysisException ex) {
                throw new AnalysisException("Error evaluating \"" + conjunct.toSql() + "\"", ex);
            }
        }
    }

    public boolean isOjConjunct(Expr e) {
        return globalState.ojClauseByConjunct.containsKey(e.getId());
    }

    public boolean isIjConjunct(Expr e) {
        return globalState.ijClauseByConjunct.containsKey(e.getId());
    }

    public boolean isSjConjunct(Expr e) {
        return globalState.sjClauseByConjunct.containsKey(e.getId());
    }

    public TableRef getFullOuterJoinRef(Expr e) {
        return globalState.fullOuterJoinedConjuncts.get(e.getId());
    }

    public boolean isFullOuterJoined(Expr e) {
        return globalState.fullOuterJoinedConjuncts.containsKey(e.getId());
    }

    public TableRef getOjRef(Expr e) {
        return globalState.ojClauseByConjunct.get(e.getId());
    }

    /**
     * Returns false if 'e' originates from an outer-join On-clause and it is incorrect to
     * evaluate 'e' at a node materializing 'tids'. Returns true otherwise.
     */
    public boolean canEvalOuterJoinedConjunct(Expr e, List<TupleId> tids) {
        TableRef outerJoin = getOjRef(e);
        if (outerJoin == null) {
            return true;
        }
        return tids.containsAll(outerJoin.getAllTableRefIds());
    }

    /**
     * Returns list of candidate equi-join conjuncts to be evaluated by the join node
     * that is specified by the table ref ids of its left and right children.
     * If the join to be performed is an outer join, then only equi-join conjuncts
     * from its On-clause are returned. If an equi-join conjunct is full outer joined,
     * then it is only added to the result if this join is the one to full-outer join it.
     */
    public List<Expr> getEqJoinConjuncts(List<TupleId> lhsTblRefIds,
                                         List<TupleId> rhsTblRefIds) {
        // Contains all equi-join conjuncts that have one child fully bound by one of the
        // rhs table ref ids (the other child is not bound by that rhs table ref id).
        List<ExprId> conjunctIds = Lists.newArrayList();
        for (TupleId rhsId : rhsTblRefIds) {
            List<ExprId> cids = globalState.eqJoinConjuncts.get(rhsId);
            if (cids == null) {
                continue;
            }
            for (ExprId eid : cids) {
                if (!conjunctIds.contains(eid)) {
                    conjunctIds.add(eid);
                }
            }
        }

        // Since we currently prevent join re-reordering across outer joins, we can never
        // have a bushy outer join with multiple rhs table ref ids. A busy outer join can
        // only be constructed with an inline view (which has a single table ref id).
        List<ExprId> ojClauseConjuncts = null;
        if (rhsTblRefIds.size() == 1) {
            ojClauseConjuncts = globalState.conjunctsByOjClause.get(rhsTblRefIds.get(0));
        }

        // List of table ref ids that the join node will 'materialize'.
        List<TupleId> nodeTblRefIds = Lists.newArrayList(lhsTblRefIds);
        nodeTblRefIds.addAll(rhsTblRefIds);
        List<Expr> result = Lists.newArrayList();
        for (ExprId conjunctId : conjunctIds) {
            Expr e = globalState.conjuncts.get(conjunctId);
            Preconditions.checkState(e != null);
            if (!canEvalFullOuterJoinedConjunct(e, nodeTblRefIds)
                    || !canEvalAntiJoinedConjunct(e, nodeTblRefIds)
                    || !canEvalOuterJoinedConjunct(e, nodeTblRefIds)) {
                continue;
            }

            if (ojClauseConjuncts != null && !ojClauseConjuncts.contains(conjunctId)) {
                continue;
            }
            result.add(e);
        }
        return result;
    }

    /**
     * return equal conjuncts, used by OlapScanNode.normalizePredicate and SelectStmt.reorderTable
     */
    public List<Expr> getEqJoinConjuncts(TupleId id) {
        final List<ExprId> conjunctIds = globalState.eqJoinConjuncts.get(id);
        if (conjunctIds == null) {
            return Lists.newArrayList();
        }
        final List<Expr> result = Lists.newArrayList();
        for (ExprId conjunctId : conjunctIds) {
            final Expr e = globalState.conjuncts.get(conjunctId);
            Preconditions.checkState(e != null);
            result.add(e);
        }
        return result;
    }

    /**
     * Returns list of candidate equi-join conjuncts excluding auxiliary predicates
     */
    public List<Expr> getEqJoinConjunctsExcludeAuxPredicates(TupleId id) {
        final List<Expr> candidateEqJoinPredicates = getEqJoinConjuncts(id);
        final Iterator<Expr> iterator = candidateEqJoinPredicates.iterator();
        while (iterator.hasNext()) {
            final Expr expr = iterator.next();
            if (expr.isAuxExpr()) {
                iterator.remove();
            }
        }
        return candidateEqJoinPredicates;
    }

    public List<Expr> getBufferReuseConjuncts(TupleId id) {
        List<Expr> result = bufferReuseExprs.get(id);
        if (null == result) {
            result = Lists.newArrayList();
            bufferReuseExprs.put(id, result);
        }
        return result;
    }

    public int getCurrentOutputColumn(TupleId id) {
        Integer result = currentOutputColumn.get(id);
        if (null == result) {
            return this.getTupleDesc(id).getSlots().size();
        }
        return result;
    }

    public void setCurrentOutputColumn(TupleId id, int v) {
        currentOutputColumn.put(id, v);
    }

    /**
     * Mark predicates as assigned.
     */
    public void markConjunctsAssigned(List<Expr> conjuncts) {
        if (conjuncts == null) {
            return;
        }
        for (Expr p : conjuncts) {
            globalState.assignedConjuncts.add(p.getId());
        }
    }

    /**
     * Mark predicate as assigned.
     */
    public void markConjunctAssigned(Expr conjunct) {
        globalState.assignedConjuncts.add(conjunct.getId());
    }

    /**
     * Return true if there's at least one unassigned conjunct.
     */
    public boolean hasUnassignedConjuncts() {
        // for (Map.Entry<ExprId, Expr> entry : globalState.conjuncts.entrySet()) {
        //     if (!globalState.assignedConjuncts.contains(entry.getKey())) {
        //         LOG.warn("hasUnassignedConjuncts exps");
        //         ((Expr)(entry.getValue())).printChild();
        //     }
        // }
        return !globalState.assignedConjuncts.containsAll(globalState.conjuncts.keySet());
    }

    /**
     * Returns assignment-compatible type of expr.getType() and lastCompatibleType.
     * If lastCompatibleType is null, returns expr.getType() (if valid).
     * If types are not compatible throws an exception reporting
     * the incompatible types and their expr.toSql().
     *
     * lastCompatibleExpr is passed for error reporting purposes,
     * but note that lastCompatibleExpr may not yet have lastCompatibleType,
     * because it was not cast yet.
     */
    public Type getCompatibleType(Type lastCompatibleType, Expr lastCompatibleExpr, Expr expr)
            throws AnalysisException {
        Type newCompatibleType;
        if (lastCompatibleType == null) {
            newCompatibleType = expr.getType();
        } else {
            newCompatibleType = Type.getAssignmentCompatibleType(lastCompatibleType, expr.getType(), false);
        }
        if (!newCompatibleType.isValid()) {
            throw new AnalysisException(String.format(
                    "Incompatible return types '%s' and '%s' of exprs '%s' and '%s'.",
                    lastCompatibleType.toSql(), expr.getType().toSql(),
                    lastCompatibleExpr.toSql(), expr.toSql()));
        }
        return newCompatibleType;
    }

    /**
     * Determines compatible type for given exprs, and casts them to compatible
     * type. Calls analyze() on each of the exprs. Throw an AnalysisException if
     * the types are incompatible, returns compatible type otherwise.
     */
    public Type castAllToCompatibleType(List<Expr> exprs) throws AnalysisException {
        // Determine compatible type of exprs.
        exprs.get(0).analyze(this);
        Type compatibleType = exprs.get(0).getType();
        for (int i = 1; i < exprs.size(); ++i) {
            exprs.get(i).analyze(this);
            if (compatibleType.isDateV2() && exprs.get(i) instanceof StringLiteral
                    && ((StringLiteral) exprs.get(i)).canConvertToDateType(compatibleType)) {
                // If string literal can be converted to dateV2, we use datev2 as the compatible type
                // instead of datetimev2.
            } else if (exprs.get(i).isConstantImpl()) {
                exprs.get(i).compactForLiteral(compatibleType);
                compatibleType = Type.getCmpType(compatibleType, exprs.get(i).getType());
            } else {
                compatibleType = Type.getCmpType(compatibleType, exprs.get(i).getType());
            }
        }
        if (compatibleType.equals(Type.VARCHAR)) {
            if (exprs.get(0).getType().isDateType()) {
                compatibleType = exprs.get(0).getType().isDateOrDateTime()
                        ? Type.DATETIME : exprs.get(0).getType().isDatetimeV2()
                        ? exprs.get(0).getType() : Type.DATETIMEV2;
            }
        }
        // Add implicit casts if necessary.
        for (int i = 0; i < exprs.size(); ++i) {
            if (!exprs.get(i).getType().equals(compatibleType)) {
                Expr castExpr = exprs.get(i).castTo(compatibleType);
                exprs.set(i, castExpr);
            }
        }
        return compatibleType;
    }

    /**
     * Casts the exprs in the given lists position-by-position such that for every i,
     * the i-th expr among all expr lists is compatible.
     * Throw an AnalysisException if the types are incompatible.
     */
    public void castToSetOpsCompatibleTypes(List<List<Expr>> exprLists)
            throws AnalysisException {
        if (exprLists == null || exprLists.size() < 2) {
            return;
        }

        // Determine compatible types for exprs, position by position.
        List<Expr> firstList = exprLists.get(0);
        for (int i = 0; i < firstList.size(); ++i) {
            // Type compatible with the i-th exprs of all expr lists.
            // Initialize with type of i-th expr in first list.
            Type compatibleType = firstList.get(i).getType();
            // Remember last compatible expr for error reporting.
            Expr lastCompatibleExpr = firstList.get(i);
            for (int j = 1; j < exprLists.size(); ++j) {
                Preconditions.checkState(exprLists.get(j).size() == firstList.size());
                compatibleType = getCompatibleType(compatibleType,
                        lastCompatibleExpr, exprLists.get(j).get(i));
                lastCompatibleExpr = exprLists.get(j).get(i);
            }
            // Now that we've found a compatible type, add implicit casts if necessary.
            for (int j = 0; j < exprLists.size(); ++j) {
                if (!exprLists.get(j).get(i).getType().equals(compatibleType)) {
                    Expr castExpr = exprLists.get(j).get(i).castTo(compatibleType);
                    exprLists.get(j).set(i, castExpr);
                }
            }
        }
    }

    public long getConnectId() {
        return globalState.context.getConnectionId();
    }

    public String getDefaultCatalog() {
        return globalState.context.getDefaultCatalog();
    }

    public String getDefaultDb() {
        return globalState.context.getDatabase();
    }

    public String getClusterName() {
        return globalState.context.getClusterName();
    }

    public String getQualifiedUser() {
        return globalState.context.getQualifiedUser();
    }

    public String getUserIdentity(boolean currentUser) {
        if (currentUser) {
            return "";
        } else {
            return getQualifiedUser() + "@" + ConnectContext.get().getRemoteIP();
        }
    }

    public String getSchemaDb() {
        return schemaDb;
    }

    public String getSchemaCatalog() {
        return schemaCatalog;
    }

    public String getSchemaTable() {
        return schemaTable;
    }

    // TODO: `globalState.context` could be null, refactor return value type to
    // `Optional<ConnectContext>`.
    public ConnectContext getContext() {
        return globalState.context;
    }

    public String getSchemaWild() {
        return schemaWild;
    }

    public TQueryGlobals getQueryGlobals() {
        return new TQueryGlobals();
    }

    // for Schema Table Schema like SHOW TABLES LIKE "abc%"
    public void setSchemaInfo(String db, String table, String wild, String catalog) {
        schemaDb = db;
        schemaTable = table;
        schemaWild = wild;
        schemaCatalog = catalog;
    }

    public String getTargetDbName(FunctionName fnName) {
        return fnName.isFullyQualified() ? fnName.getDb() : getDefaultDb();
    }

    public ExprSubstitutionMap getChangeResSmap() {
        return changeResSmap;
    }

    public void setChangeResSmap(ExprSubstitutionMap changeResSmap) {
        this.changeResSmap = changeResSmap;
    }

    // The star join reorder is turned on
    // when 'enable_join_reorder_based_cost = false' and 'disable_join_reorder = false'
    public boolean enableStarJoinReorder() {
        if (globalState.context == null) {
            return false;
        }
        return !globalState.context.getSessionVariable().isEnableJoinReorderBasedCost()
                && !globalState.context.getSessionVariable().isDisableJoinReorder();
    }

    public boolean enableInferPredicate() {
        if (globalState.context == null) {
            return false;
        }
        return globalState.context.getSessionVariable().isEnableInferPredicate();
    }

    // The cost based join reorder is turned on
    // when 'enable_join_reorder_based_cost = true' and 'disable_join_reorder = false'
    // Load plan and query plan are the same framework
    // Some Load method in doris access through http protocol, which will cause the session may be empty.
    // In order to avoid the occurrence of null pointer exceptions, a check will be added here
    public boolean safeIsEnableJoinReorderBasedCost() {
        if (globalState.context == null) {
            return false;
        }
        return globalState.context.getSessionVariable().isEnableJoinReorderBasedCost()
                && !globalState.context.getSessionVariable().isDisableJoinReorder();
    }

    public boolean safeIsEnableFoldConstantByBe() {
        if (globalState.context == null) {
            return false;
        }
        return globalState.context.getSessionVariable().isEnableFoldConstantByBe();
    }

    /**
     * Returns true if predicate 'e' can be correctly evaluated by a tree materializing
     * 'tupleIds', otherwise false:
     * - The predicate needs to be bound by tupleIds.
     * - For On-clause predicates:
     *   - If the predicate is from an anti-join On-clause it must be evaluated by the
     *     corresponding anti-join node.
     *   - Predicates from the On-clause of an inner or semi join are evaluated at the
     *     node that materializes the required tuple ids, unless they reference outer
     *     joined tuple ids. In that case, the predicates are evaluated at the join node
     *     of the corresponding On-clause.
     *   - Predicates referencing full-outer joined tuples are assigned at the originating
     *     join if it is a full-outer join, otherwise at the last full-outer join that does
     *     not materialize the table ref ids of the originating join.
     *   - Predicates from the On-clause of a left/right outer join are assigned at
     *     the corresponding outer join node with the exception of simple predicates
     *     that only reference a single tuple id. Those may be assigned below the
     *     outer join node if they are from the same On-clause that makes the tuple id
     *     nullable.
     * - Otherwise, a predicate can only be correctly evaluated if for all outer-joined
     *   referenced tids the last join to outer-join this tid has been materialized.
     */
    public boolean canEvalPredicate(List<TupleId> tupleIds, Expr e) {
        if (!e.isBoundByTupleIds(tupleIds)) {
            return false;
        }
        ArrayList<TupleId> tids = Lists.newArrayList();
        e.getIds(tids, null);
        if (tids.isEmpty()) {
            return true;
        }

        if (e.isOnClauseConjunct()) {

            if (isAntiJoinedConjunct(e)) {
                return canEvalAntiJoinedConjunct(e, tupleIds);
            }
            if (isIjConjunct(e) || isSjConjunct(e)) {
                if (!containsOuterJoinedTid(tids)) {
                    return true;
                }
                // If the predicate references an outer-joined tuple, then evaluate it at
                // the join that the On-clause belongs to.
                TableRef onClauseTableRef = null;
                if (isIjConjunct(e)) {
                    onClauseTableRef = globalState.ijClauseByConjunct.get(e.getId());
                } else {
                    onClauseTableRef = globalState.sjClauseByConjunct.get(e.getId());
                }
                Preconditions.checkNotNull(onClauseTableRef);
                return tupleIds.containsAll(onClauseTableRef.getAllTableRefIds());
            }

            if (isFullOuterJoined(e)) {
                return canEvalFullOuterJoinedConjunct(e, tupleIds);
            }
            if (isOjConjunct(e)) {
                // Force this predicate to be evaluated by the corresponding outer join node.
                // The join node will pick up the predicate later via getUnassignedOjConjuncts().
                if (tids.size() > 1) {
                    return false;
                }
                // Optimization for single-tid predicates: Legal to assign below the outer join
                // if the predicate is from the same On-clause that makes tid nullable
                // (otherwise e needn't be true when that tuple is set).
                TupleId tid = tids.get(0);
                return globalState.ojClauseByConjunct.get(e.getId()) == getLastOjClause(tid);
            }
        }

        for (TupleId tid : tids) {
            TableRef rhsRef = getLastOjClause(tid);
            // this is not outer-joined; ignore
            if (rhsRef == null) {
                continue;
            }
            // check whether the last join to outer-join 'tid' is materialized by tupleIds
            if (!tupleIds.containsAll(rhsRef.getAllTupleIds())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if a conjunct from the On-clause of an anti join can be evaluated in a node
     * that materializes a given list of tuple ids.
     */
    public boolean canEvalAntiJoinedConjunct(Expr e, List<TupleId> nodeTupleIds) {
        TableRef antiJoinRef = getAntiJoinRef(e);
        if (antiJoinRef == null) {
            return true;
        }
        List<TupleId> tids = Lists.newArrayList();
        e.getIds(tids, null);
        if (tids.size() > 1) {
            return nodeTupleIds.containsAll(antiJoinRef.getAllTableRefIds())
                    && antiJoinRef.getAllTableRefIds().containsAll(nodeTupleIds);
        }
        // A single tid conjunct that is anti-joined can be safely assigned to a
        // node below the anti join that specified it.
        return globalState.semiJoinedTupleIds.containsKey(tids.get(0));
    }

    /**
     * Returns false if 'e' references a full outer joined tuple and it is incorrect to
     * evaluate 'e' at a node materializing 'tids'. Returns true otherwise.
     */
    public boolean canEvalFullOuterJoinedConjunct(Expr e, List<TupleId> tids) {
        TableRef fullOuterJoin = getFullOuterJoinRef(e);
        if (fullOuterJoin == null) {
            return true;
        }
        return tids.containsAll(fullOuterJoin.getAllTableRefIds());
    }

    /**
     * Return all unassigned registered conjuncts for node's table ref ids.
     * Wrapper around getUnassignedConjuncts(List<TupleId> tupleIds).
     */
    public List<Expr> getUnassignedConjuncts(PlanNode node) {
        // constant conjuncts should be push down to all leaf node except agg node.
        // (see getPredicatesBoundedByGroupbysSourceExpr method)
        // so we need remove constant conjuncts when expr is not a leaf node.
        List<Expr> unassigned = getUnassignedConjuncts(
                node instanceof AggregationNode ? node.getTupleIds() : node.getTblRefIds());
        if (!node.getChildren().isEmpty() && !(node instanceof AggregationNode)) {
            unassigned = unassigned.stream()
                    .filter(e -> !e.isConstant()).collect(Collectors.toList());
        }
        return unassigned;
    }

    /**
     * Returns true if e must be evaluated by a join node. Note that it may still be
     * safe to evaluate e elsewhere as well, but in any case the join must evaluate e.
     */
    public boolean evalByJoin(Expr e) {
        List<TupleId> tids = Lists.newArrayList();
        e.getIds(tids, null);

        if (tids.isEmpty()) {
            return false;
        }

        if (tids.size() > 1 || globalState.ojClauseByConjunct.containsKey(e.getId())
                || globalState.outerJoinedTupleIds.containsKey(e.getId()) && whereClauseConjuncts.contains(e.getId())
                || globalState.conjunctsByOjClause.containsKey(e.getId())) {
            return true;
        }

        return false;
    }

    /**
     * Mark all slots that are referenced in exprs as materialized.
     */
    public void materializeSlots(List<Expr> exprs) {
        List<SlotId> slotIds = Lists.newArrayList();

        for (Expr e : exprs) {
            Preconditions.checkState(e.isAnalyzed);
            e.getIds(null, slotIds);
        }

        for (TupleDescriptor tupleDesc : this.getDescTbl().getTupleDescs()) {
            for (SlotDescriptor slotDesc : tupleDesc.getSlots()) {
                if (slotIds.contains(slotDesc.getId())) {
                    slotDesc.setIsMaterialized(true);
                }
            }
        }
    }

    public Map<String, View> getLocalViews() {
        return localViews;
    }

    public boolean isOuterJoined(TupleId tid) {
        return globalState.outerJoinedTupleIds.containsKey(tid);
    }

    public boolean isOuterJoinedLeftSide(TupleId tid) {
        return globalState.outerLeftSideJoinTupleIds.contains(tid);
    }

    public boolean isOuterJoinedRightSide(TupleId tid) {
        return globalState.outerRightSideJoinTupleIds.contains(tid);
    }

    public boolean isInlineView(TupleId tid) {
        return globalState.inlineViewTupleIds.contains(tid);
    }

    public boolean containSubquery() {
        return globalState.containsSubquery;
    }

    /**
     * Mark slots that are being referenced by the plan tree itself or by the outputExprs exprs as materialized. If the
     * latter is null, mark all slots in planRoot's tupleIds() as being referenced. All aggregate slots are
     * materialized.
     * <p/>
     * TODO: instead of materializing everything produced by the plan root, derived referenced slots from destination
     * fragment and add a materialization node if not all output is needed by destination fragment TODO 2: should the
     * materialization decision be cost-based?
     */
    public void markRefdSlots(Analyzer analyzer, PlanNode planRoot,
                              List<Expr> outputExprs, AnalyticInfo analyticInfo) {
        if (planRoot == null) {
            return;
        }
        List<SlotId> refdIdList = Lists.newArrayList();
        planRoot.getMaterializedIds(analyzer, refdIdList);
        if (outputExprs != null) {
            Expr.getIds(outputExprs, null, refdIdList);
        }

        HashSet<SlotId> refdIds = Sets.newHashSet(refdIdList);
        for (TupleDescriptor tupleDesc : analyzer.getDescTbl().getTupleDescs()) {
            for (SlotDescriptor slotDesc : tupleDesc.getSlots()) {
                if (refdIds.contains(slotDesc.getId())) {
                    slotDesc.setIsMaterialized(true);
                }
            }
        }
        if (analyticInfo != null) {
            ArrayList<SlotDescriptor> list = analyticInfo.getOutputTupleDesc().getSlots();

            for (SlotDescriptor slotDesc : list) {
                if (refdIds.contains(slotDesc.getId())) {
                    slotDesc.setIsMaterialized(true);
                }
            }
        }
        if (outputExprs == null) {
            // mark all slots in planRoot.getTupleIds() as materialized
            ArrayList<TupleId> tids = planRoot.getTupleIds();
            for (TupleId tid : tids) {
                TupleDescriptor tupleDesc = analyzer.getDescTbl().getTupleDesc(tid);
                for (SlotDescriptor slotDesc : tupleDesc.getSlots()) {
                    slotDesc.setIsMaterialized(true);
                }
            }
        }
    }

    /**
     * Column conduction, can slot a value-transfer to slot b
     * <p>
     * TODO(zxy) Use value-transfer graph to check
     */
    public boolean hasValueTransfer(SlotId a, SlotId b) {
        return getValueTransferTargets(a).contains(b);
    }

    /**
     * Returns sorted slot IDs with value transfers from 'srcSid'.
     * Time complexity: O(V) where V = number of slots
     * <p>
     * TODO(zxy) Use value-transfer graph to check
     */
    public List<SlotId> getValueTransferTargets(SlotId srcSid) {
        List<SlotId> result = new ArrayList<>();
        result.add(srcSid);
        SlotId equalSlot = srcSid;
        while (containEquivalentSlot(equalSlot)) {
            result.add(getEquivalentSlot(equalSlot));
            equalSlot = getEquivalentSlot(equalSlot);
        }
        return result;
    }

    /**
     * Returns true if any of the given slot ids or their value-transfer targets belong
     * to an outer-joined tuple.
     */
    public boolean hasOuterJoinedValueTransferTarget(List<SlotId> sids) {
        for (SlotId srcSid : sids) {
            for (SlotId dstSid : getValueTransferTargets(srcSid)) {
                if (isOuterJoined(getTupleId(dstSid))) {
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * Change all outer joined slots to nullable
     * Returns the slots actually be changed from not nullable to nullable
     */
    public List<SlotDescriptor> changeSlotToNullableOfOuterJoinedTuples() {
        List<SlotDescriptor> result = new ArrayList<>();
        for (TupleId id : globalState.outerJoinedTupleIds.keySet()) {
            TupleDescriptor tupleDescriptor = globalState.descTbl.getTupleDesc(id);
            if (tupleDescriptor != null) {
                for (SlotDescriptor desc : tupleDescriptor.getSlots()) {
                    if (!desc.getIsNullable()) {
                        desc.setIsNullable(true);
                        result.add(desc);
                    }
                }
            }
        }
        return result;
    }

    public void changeSlotsToNotNullable(List<SlotDescriptor> slots) {
        for (SlotDescriptor slot : slots) {
            slot.setIsNullable(false);
        }
    }
}
