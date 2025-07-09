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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.AnalyticEvalNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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

    private boolean isReplay = false;

    public void setIsSubquery() {
        isSubquery = true;
        isFirstScopeInSubquery = true;
        globalState.containsSubquery = true;
    }

    public void setReAnalyze(boolean reAnalyze) {
        isReAnalyze = reAnalyze;
    }

    public boolean isReAnalyze() {
        return isReAnalyze;
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

    public boolean isReplay() {
        return isReplay;
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

        // Record the statement clazz that the analyzer would to analyze,
        // this give an opportunity to control analyzing behavior according to the statement type.
        public Class<? extends StatementBase> rootStatementClazz;

        // True if at least one of the analyzers belongs to a subquery.
        public boolean containsSubquery = false;

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



        private final Map<SlotId, SlotId> equivalentSlots = Maps.newHashMap();

        private final Map<String, TupleDescriptor> markTuples = Maps.newHashMap();

        private final Map<TableRef, TupleId> markTupleIdByInnerRef = Maps.newHashMap();

        private final Set<TupleId> markTupleIdsNotProcessed = Sets.newHashSet();


        public GlobalState(Env env, ConnectContext context) {
            this.env = env;
            this.context = context;
        }
    }

    private final GlobalState globalState;

    private final InferPredicateState inferPredicateState;

    // An analyzer stores analysis state for a single select block. A select block can be
    // a top level select statement, or an inline view select block.
    // ancestors contains the Analyzers of the enclosing select blocks of 'this'
    // (ancestors[0] contains the immediate parent, etc.).
    private final ArrayList<Analyzer> ancestors;

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

    public boolean isExplain() {
        return globalState.isExplain;
    }

    public void setRootStatementClazz(Class<? extends StatementBase> statementClazz) {
        globalState.rootStatementClazz = statementClazz;
    }

    public Class<? extends StatementBase> getRootStatementClazz() {
        return globalState.rootStatementClazz;
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

    public List<TupleId> getAllTupleIds() {
        return new ArrayList<>(tableRefMap.keySet());
    }

    public TableIf getTableOrAnalysisException(TableName tblName) throws AnalysisException {
        DatabaseIf db = globalState.env.getCatalogMgr().getCatalogOrAnalysisException(tblName.getCtl())
                .getDbOrAnalysisException(tblName.getDb());
        return db.getTableOrAnalysisException(tblName.getTbl());
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

        Column col = (d.getTable() == null)
                ? new Column(colName, ScalarType.BOOLEAN,
                        globalState.markTuples.get(d.getAlias()) != null)
                : d.getTable().getColumn(colName);
        if (col == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, colName,
                                                newTblName == null ? d.getTable().getName() : newTblName.toString());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("register column ref table {}, colName {}, col {}", tblName, colName, col.toSql());
        }
        if (col.getType().isVariantType() || (subColNames != null && !subColNames.isEmpty())) {
            if (getContext() != null && !getContext().getSessionVariable().enableVariantAccessInOriginalPlanner
                    && (subColNames != null && !subColNames.isEmpty())) {
                ErrorReport.reportAnalysisException("Variant sub-column access is disabled in original planner,"
                        + "set enable_variant_access_in_original_planner = true in session variable");
            }
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
                            Iterator<String> it1 = lst1.iterator();
                            Iterator<String> it2 = lst2.iterator();
                            while (it1.hasNext() && it2.hasNext()) {
                                int result = it1.next().compareTo(it2.next());
                                if (result != 0) {
                                    return result;
                                }
                            }
                            return Integer.compare(lst1.size(), lst2.size());
                        }
                    }));
            }
            SlotDescriptor result = subColumnSlotRefMap.get(key).get(subColNames);
            if (result != null) {
                // avoid duplicate slots
                return result;
            }
            result = globalState.descTbl.addSlotDescriptor(d);
            if (LOG.isDebugEnabled()) {
                LOG.debug("register slot descriptor {}", result);
            }
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
        result.setSubColLables(srcSlotDesc.getSubColLables());
        if (srcSlotDesc.getColumn() != null) {
            result.setColumn(srcSlotDesc.getColumn());
        }
        // result.setItemTupleDesc(srcSlotDesc.getItemTupleDesc());
        return result;
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
     * Register all conjuncts in 'conjuncts' that make up the On-clause of the given
     * right-hand side of a join. Assigns each conjunct a unique id. If rhsRef is
     * the right-hand side of an outer join, then the conjuncts conjuncts are
     * registered such that they can only be evaluated by the node implementing that
     * join.
     */
    public void registerOnClauseConjuncts(List<Expr> conjuncts, TableRef rhsRef)
            throws AnalysisException {

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
            newCompatibleType = Type.getAssignmentCompatibleType(
                lastCompatibleType, expr.getType(), false, SessionVariable.getEnableDecimal256());
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
            boolean enableDecimal256 = SessionVariable.getEnableDecimal256();
            if (compatibleType.isDateV2() && exprs.get(i) instanceof StringLiteral
                    && ((StringLiteral) exprs.get(i)).canConvertToDateType(compatibleType)) {
                // If string literal can be converted to dateV2, we use datev2 as the compatible type
                // instead of datetimev2.
            } else if (exprs.get(i).isConstantImpl()) {
                exprs.get(i).compactForLiteral(compatibleType);
                compatibleType = Type.getCmpType(compatibleType, exprs.get(i).getType(), enableDecimal256);
            } else {
                compatibleType = Type.getCmpType(compatibleType, exprs.get(i).getType(), enableDecimal256);
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

    public long getConnectId() {
        return globalState.context.getConnectionId();
    }

    public String getDefaultCatalog() {
        return globalState.context.getDefaultCatalog();
    }

    public String getDefaultDb() {
        return globalState.context.getDatabase();
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

    // TODO: `globalState.context` could be null, refactor return value type to
    // `Optional<ConnectContext>`.
    public ConnectContext getContext() {
        return globalState.context;
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
        // constant conjuncts should be push down to all leaf node except agg and analytic node.
        // (see getPredicatesBoundedByGroupbysSourceExpr method)
        // so we need remove constant conjuncts when expr is not a leaf node.
        List<Expr> unassigned = getUnassignedConjuncts(node.getTblRefIds());
        if (!node.getChildren().isEmpty()
                && !(node instanceof AggregationNode || node instanceof AnalyticEvalNode)) {
            unassigned = unassigned.stream()
                    .filter(e -> !e.isConstant()).collect(Collectors.toList());
        }
        return unassigned;
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

    public boolean isOuterJoined(TupleId tid) {
        return globalState.outerJoinedTupleIds.containsKey(tid);
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
