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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.ConnectContext;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        // Record the statement clazz that the analyzer would to analyze,
        // this give an opportunity to control analyzing behavior according to the statement type.
        public Class<? extends StatementBase> rootStatementClazz;

        // all registered conjuncts (map from id to Predicate)
        private final Map<ExprId, Expr> conjuncts = Maps.newHashMap();

        // eqJoinConjuncts[tid] contains all conjuncts of the form
        // "<lhs> = <rhs>" in which either lhs or rhs is fully bound by tid
        // and the other side is not bound by tid (ie, predicates that express equi-join
        // conditions between two tablerefs).
        // A predicate such as "t1.a = t2.b" has two entries, one for 't1' and
        // another one for 't2'.
        private final Map<TupleId, List<ExprId>> eqJoinConjuncts = Maps.newHashMap();

        // map from outer-joined tuple id, ie, one that is nullable in this select block,
        // to the last Join clause (represented by its rhs table ref) that outer-joined it
        private final Map<TupleId, TableRef> outerJoinedTupleIds = Maps.newHashMap();

        // Map of registered conjunct to the last full outer join (represented by its
        // rhs table ref) that outer joined it.
        public final Map<ExprId, TableRef> fullOuterJoinedConjuncts = Maps.newHashMap();

        // Map of full-outer-joined tuple id to the last full outer join that outer-joined it
        public final Map<TupleId, TableRef> fullOuterJoinedTupleIds = Maps.newHashMap();

        // map from slot id to the analyzer/block in which it was registered
        private final Map<SlotId, Analyzer> blockBySlot = Maps.newHashMap();


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
     * Creates a new slot descriptor and related state in globalState.
     */
    public SlotDescriptor addSlotDescriptor(TupleDescriptor tupleDesc) {
        SlotDescriptor result = globalState.descTbl.addSlotDescriptor(tupleDesc);
        globalState.blockBySlot.put(result.getId(), this);
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

    public DescriptorTable getDescTbl() {
        return globalState.descTbl;
    }

    public Env getEnv() {
        return globalState.env;
    }

    public Set<String> getAliases() {
        return uniqueTableAliasSet;
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

    public String getDefaultCatalog() {
        return globalState.context.getDefaultCatalog();
    }

    public String getDefaultDb() {
        return globalState.context.getDatabase();
    }

    public String getQualifiedUser() {
        return globalState.context.getQualifiedUser();
    }

    // TODO: `globalState.context` could be null, refactor return value type to
    // `Optional<ConnectContext>`.
    public ConnectContext getContext() {
        return globalState.context;
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
