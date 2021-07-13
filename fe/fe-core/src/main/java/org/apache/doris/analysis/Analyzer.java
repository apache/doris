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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.View;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.BetweenToCompoundRule;
import org.apache.doris.rewrite.ExprRewriteRule;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.rewrite.ExtractCommonFactorsRule;
import org.apache.doris.rewrite.FoldConstantsRule;
import org.apache.doris.rewrite.RewriteFromUnixTimeRule;
import org.apache.doris.rewrite.NormalizeBinaryPredicatesRule;
import org.apache.doris.rewrite.SimplifyInvalidDateBinaryPredicatesDateRule;
import org.apache.doris.rewrite.mvrewrite.CountDistinctToBitmap;
import org.apache.doris.rewrite.mvrewrite.CountDistinctToBitmapOrHLLRule;
import org.apache.doris.rewrite.mvrewrite.CountFieldToSum;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
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
    private final static Logger LOG = LogManager.getLogger(Analyzer.class);
    // used for contains inlineview analytic function's tuple changed
    private ExprSubstitutionMap changeResSmap = new ExprSubstitutionMap();

    // NOTE: Alias of table is case sensitive
    // UniqueAlias used to check whether the table ref or the alias is unique
    // table/view used db.table, inline use alias
    private final Set<String> uniqueTableAliasSet_ = Sets.newHashSet();
    private final Multimap<String, TupleDescriptor> tupleByAlias = ArrayListMultimap.create();

    // NOTE: Alias of column is case ignorance
    // map from lowercase table alias to descriptor.
    // protected final Map<String, TupleDescriptor> aliasMap             = Maps.newHashMap();
    // map from lowercase qualified column name ("alias.col") to descriptor
    private final Map<String, SlotDescriptor>  slotRefMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

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
    private String schemaWild;
    private String schemaTable; // table used in DESCRIBE Table

    // True if the corresponding select block has a limit and/or offset clause.
    private boolean hasLimitOffsetClause_ = false;

    // Current depth of nested analyze() calls. Used for enforcing a
    // maximum expr-tree depth. Needs to be manually maintained by the user
    // of this Analyzer with incrementCallDepth() and decrementCallDepth().
    private int callDepth = 0;

    // Flag indicating if this analyzer instance belongs to a subquery.
    private boolean isSubquery = false;

    // Flag indicating whether this analyzer belongs to a WITH clause view.
    private boolean isWithClause_ = false;

    // By default, all registered semi-joined tuples are invisible, i.e., their slots
    // cannot be referenced. If set, this semi-joined tuple is made visible. Such a tuple
    // should only be made visible for analyzing the On-clause of its semi-join.
    // In particular, if there are multiple semi-joins in the same query block, then the
    // On-clause of any such semi-join is not allowed to reference other semi-joined tuples
    // except its own. Therefore, only a single semi-joined tuple can be visible at a time.
    private TupleId visibleSemiJoinedTupleId_ = null;
    // for some situation that udf is not allowed.
    private boolean isUDFAllowed = true;
    // timezone specified for some operation, such as broker load
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;

    // The runtime filter that is expected to be used
    private final List<RuntimeFilter> assignedRuntimeFilters = new ArrayList<>();
    
    public void setIsSubquery() {
        isSubquery = true;
        globalState.containsSubquery = true;
    }
    public boolean setHasPlanHints() { return globalState.hasPlanHints = true; }
    public boolean hasPlanHints() { return globalState.hasPlanHints; }
    public void setIsWithClause() { isWithClause_ = true; }
    public boolean isWithClause() { return isWithClause_; }
    
    public void setUDFAllowed(boolean val) { this.isUDFAllowed = val; }
    public boolean isUDFAllowed() { return this.isUDFAllowed; }
    public void setTimezone(String timezone) { this.timezone = timezone; }
    public String getTimezone() { return timezone; }

    public void putAssignedRuntimeFilter(RuntimeFilter rf) { assignedRuntimeFilters.add(rf); }
    public List<RuntimeFilter> getAssignedRuntimeFilter() { return assignedRuntimeFilters; }
    public void clearAssignedRuntimeFilters() { assignedRuntimeFilters.clear(); }

    // state shared between all objects of an Analyzer tree
    // TODO: Many maps here contain properties about tuples, e.g., whether
    // a tuple is outer/semi joined, etc. Remove the maps in favor of making
    // them properties of the tuple descriptor itself.
    private static class GlobalState {
        private final DescriptorTable descTbl = new DescriptorTable();
        private final Catalog catalog;
        private final IdGenerator<ExprId> conjunctIdGenerator = ExprId.createGenerator();
        private final ConnectContext context;

        // True if we are analyzing an explain request. Should be set before starting
        // analysis.
        public boolean isExplain;

        // Indicates whether the query has plan hints.
        public boolean hasPlanHints = false;

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
        private final Set<ExprId> assignedConjuncts =
            Collections.newSetFromMap(new IdentityHashMap<ExprId, Boolean>());

        // map from outer-joined tuple id, ie, one that is nullable in this select block,
        // to the last Join clause (represented by its rhs table ref) that outer-joined it
        private final Map<TupleId, TableRef> outerJoinedTupleIds = Maps.newHashMap();

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
        public final Map<SlotId, Analyzer> blockBySlot = Maps.newHashMap();

        // Expr rewriter for normalizing and rewriting expressions.
        private final ExprRewriter exprRewriter_;

        private final ExprRewriter mvExprRewriter;

        public GlobalState(Catalog catalog, ConnectContext context) {
            this.catalog = catalog;
            this.context = context;
            List<ExprRewriteRule> rules = Lists.newArrayList();
            // BetweenPredicates must be rewritten to be executable. Other non-essential
            // expr rewrites can be disabled via a query option. When rewrites are enabled
            // BetweenPredicates should be rewritten first to help trigger other rules.
            rules.add(BetweenToCompoundRule.INSTANCE);
            // Binary predicates must be rewritten to a canonical form for both predicate
            // pushdown and Parquet row group pruning based on min/max statistics.
            rules.add(NormalizeBinaryPredicatesRule.INSTANCE);
            rules.add(FoldConstantsRule.INSTANCE);
            rules.add(RewriteFromUnixTimeRule.INSTANCE);
            rules.add(SimplifyInvalidDateBinaryPredicatesDateRule.INSTANCE);
            List<ExprRewriteRule> onceRules = Lists.newArrayList();
            onceRules.add(ExtractCommonFactorsRule.INSTANCE);
            exprRewriter_ = new ExprRewriter(rules, onceRules);
            // init mv rewriter
            List<ExprRewriteRule> mvRewriteRules = Lists.newArrayList();
            mvRewriteRules.add(ToBitmapToSlotRefRule.INSTANCE);
            mvRewriteRules.add(CountDistinctToBitmapOrHLLRule.INSTANCE);
            mvRewriteRules.add(CountDistinctToBitmap.INSTANCE);
            mvRewriteRules.add(NDVToHll.INSTANCE);
            mvRewriteRules.add(HLLHashToSlotRefRule.INSTANCE);
            mvRewriteRules.add(CountFieldToSum.INSTANCE);
            mvExprRewriter = new ExprRewriter(mvRewriteRules);
        }
    };
    private final GlobalState globalState;

    // An analyzer stores analysis state for a single select block. A select block can be
    // a top level select statement, or an inline view select block.
    // ancestors contains the Analyzers of the enclosing select blocks of 'this'
    // (ancestors[0] contains the immediate parent, etc.).
    private final ArrayList<Analyzer> ancestors;

    // map from lowercase table alias to a view definition in this analyzer's scope
    private final Map<String, View> localViews_ = Maps.newHashMap();

    // Map from lowercase table alias to descriptor. Tables without an explicit alias
    // are assigned two implicit aliases: the unqualified and fully-qualified table name.
    // Such tables have two entries pointing to the same descriptor. If an alias is
    // ambiguous, then this map retains the first entry with that alias to simplify error
    // checking (duplicate vs. ambiguous alias).
    private final Map<String, TupleDescriptor> aliasMap_ = Maps.newHashMap();

    // Map from tuple id to its corresponding table ref.
    private final Map<TupleId, TableRef> tableRefMap_ = Maps.newHashMap();

    // Set of lowercase ambiguous implicit table aliases.
    private final Set<String> ambiguousAliases_ = Sets.newHashSet();

    // Indicates whether this analyzer/block is guaranteed to have an empty result set
    // due to a limit 0 or constant conjunct evaluating to false.
    private boolean hasEmptyResultSet_ = false;

    // Indicates whether the select-project-join (spj) portion of this query block
    // is guaranteed to return an empty result set. Set due to a constant non-Having
    // conjunct evaluating to false.
    private boolean hasEmptySpjResultSet_ = false;

    public Analyzer(Catalog catalog, ConnectContext context) {
        ancestors = Lists.newArrayList();
        globalState = new GlobalState(catalog, context);
    }

    /**
     * Analyzer constructor for nested select block. Catalog and DescriptorTable
     * is inherited from the parentAnalyzer.
     *
     * @param parentAnalyzer the analyzer of the enclosing select block
     */
    public Analyzer(Analyzer parentAnalyzer) {
        this(parentAnalyzer, parentAnalyzer.globalState);
        if (parentAnalyzer.isSubquery) {
            this.isSubquery = true;
        }
    }

    /**
     * Analyzer constructor for nested select block with the specified global state.
     */
    private Analyzer(Analyzer parentAnalyzer, GlobalState globalState) {
        ancestors =  Lists.newArrayList(parentAnalyzer);
        ancestors.addAll(parentAnalyzer.ancestors);
        this.globalState = globalState;
    }

    /**
     * Returns a new analyzer with the specified parent analyzer but with a new
     * global state.
     */
    public static Analyzer createWithNewGlobalState(Analyzer parentAnalyzer) {
        GlobalState globalState = new GlobalState(parentAnalyzer.globalState.catalog, parentAnalyzer.getContext());
        return new Analyzer(parentAnalyzer, globalState);
    }

    public void setIsExplain() { globalState.isExplain = true; }
    public boolean isExplain() { return globalState.isExplain; }

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
                throw new AnalysisException("WITH-clause view '" + view.getName() +
                        "' returns " + queryStmtLabels.size() + " columns, but " +
                        viewLabels.size() + " labels were specified. The number of column " +
                        "labels must be smaller or equal to the number of returned columns.");
            }
        }
        if (localViews_.put(view.getName(), view) != null) {
            throw new AnalysisException(
                    String.format("Duplicate table alias: '%s'", view.getName()));
        }
    }

    /**
     * Create query global parameters to be set in each TPlanExecRequest.
     */
    public static TQueryGlobals createQueryGlobals() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
        TQueryGlobals queryGlobals = new TQueryGlobals();
        Calendar currentDate = Calendar.getInstance();
        String nowStr = formatter.format(currentDate.getTime());
        queryGlobals.setNowString(nowStr);
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
        if (uniqueTableAliasSet_.contains(uniqueAlias)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NONUNIQ_TABLE, uniqueAlias);
        }
        uniqueTableAliasSet_.add(uniqueAlias);

        // If ref has no explicit alias, then the unqualified and the fully-qualified table
        // names are legal implicit aliases. Column references against unqualified implicit
        // aliases can be ambiguous, therefore, we register such ambiguous aliases here.
        String unqualifiedAlias = null;
        String[] aliases = ref.getAliases();
        if (aliases.length > 1) {
            unqualifiedAlias = aliases[1];
            TupleDescriptor tupleDesc = aliasMap_.get(unqualifiedAlias);
            if (tupleDesc != null) {
                if (tupleDesc.hasExplicitAlias()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NONUNIQ_TABLE, uniqueAlias);
                } else {
                    ambiguousAliases_.add(unqualifiedAlias);
                }
            }
        }

        // Delegate creation of the tuple descriptor to the concrete table ref.
        TupleDescriptor result = ref.createTupleDescriptor(this);
        result.setRef(ref);
        result.setAliases(aliases, ref.hasExplicitAlias());

        // Register all legal aliases.
        for (String alias: aliases) {
            // TODO(zc)
            // aliasMap_.put(alias, result);
            tupleByAlias.put(alias, result);
        }
        tableRefMap_.put(result.getId(), ref);

        return result;
    }

    public List<TupleId> getAllTupleIds() {
        return new ArrayList<>(tableRefMap_.keySet());
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
        if (tableRef.isResolved()) return tableRef;
        // Try to find a matching local view.
        TableName tableName = tableRef.getName();
        if (!tableName.isFullyQualified()) {
            // Searches the hierarchy of analyzers bottom-up for a registered local view with
            // a matching alias.
            String viewAlias = tableName.getTbl();
            Analyzer analyzer = this;
            do {
                View localView = analyzer.localViews_.get(viewAlias);
                if (localView != null) return new InlineViewRef(localView, tableRef);
                analyzer = (analyzer.ancestors.isEmpty() ? null : analyzer.ancestors.get(0));
            } while (analyzer != null);
        }

        // Resolve the table ref's path and determine what resolved table ref
        // to replace it with.
        String dbName = tableName.getDb();
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = getDefaultDb();
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), tableName.getDb());
        }
        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }

        Database database = globalState.catalog.getDb(dbName);
        if (database == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        Table table = database.getTable(tableName.getTbl());
        if (table == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName.getTbl());
        }

        if (table.getType() == TableType.OLAP && (((OlapTable) table).getState() == OlapTableState.RESTORE
                || ((OlapTable) table).getState() == OlapTableState.RESTORE_WITH_LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_STATE, "RESTORING");
        }

        TableName tblName = new TableName(database.getFullName(), table.getName());
        if (table instanceof View) {
            return new InlineViewRef((View) table, tableRef);
        } else {
            // The table must be a base table.
            return new BaseTableRef(tableRef, table, tblName);
        }
    }

    public Table getTable(TableName tblName) {
        Database db = globalState.catalog.getDb(tblName.getDb());
        if (db == null) {
            return null;
        }
        return db.getTable(tblName.getTbl());
    }

    public ExprRewriter getExprRewriter() { return globalState.exprRewriter_; }

    public ExprRewriter getMVExprRewriter() { return globalState.mvExprRewriter; }

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
    public SlotDescriptor registerColumnRef(TableName tblName, String colName) throws AnalysisException {
        TupleDescriptor d;
        TableName newTblName = tblName;
        if (newTblName == null) {
            d = resolveColumnRef(colName);
        } else {
            if (InfoSchemaDb.isInfoSchemaDb(newTblName.getDb())
                    || (newTblName.getDb() == null && InfoSchemaDb.isInfoSchemaDb(getDefaultDb()))) {
                newTblName = new TableName(newTblName.getDb(), newTblName.getTbl().toLowerCase());
            }
            d = resolveColumnRef(newTblName, colName);
        }
        /*
         * Now, we only support the columns in the subquery to associate the outer query columns in parent level.
         * If the level of the association exceeds one level, the associated columns in subquery could not be resolved.
         * For example:
         * Select k1 from table a where k1=(select k1 from table b where k1=(select k1 from table c where a.k1=k1));
         * The inner subquery: select k1 from table c where a.k1=k1;
         * There is a associated column (a.k1) which belongs to the outer query appears in the inner subquery.
         * This column could not be resolved because doris can only resolved the parent column instead of grandpa.
         * The exception of this query like that: Unknown column 'k1' in 'a'
         */
        if (d == null && hasAncestors() && isSubquery) {
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

        Column col = d.getTable().getColumn(colName);
        if (col == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, colName,
                                                newTblName == null ? d.getTable().getName() : newTblName.toString());
        }

        // Make column name case insensitive
        String key = d.getAlias() + "." + col.getName();
        SlotDescriptor result = slotRefMap.get(key);
        if (result != null) {
            result.setMultiRef(true);
            return result;
        }
        result = globalState.descTbl.addSlotDescriptor(d);
        result.setColumn(col);
        if (true == col.isAllowNull()) {
            result.setIsNullable(true);
        } else {
            result.setIsNullable(false);
        }
        slotRefMap.put(key, result);
        return result;
    }

    /**
     * Register a virtual column, and it is not a real column exist in table,
     * so it does not need to resolve.
     */
    public SlotDescriptor registerVirtualColumnRef(String colName, Type type, TupleDescriptor tupleDescriptor)
            throws AnalysisException {
        // Make column name case insensitive
        String key = colName;
        SlotDescriptor result = slotRefMap.get(key);
        if (result != null) {
            result.setMultiRef(true);
            return result;
        }

        result = addSlotDescriptor(tupleDescriptor);
        Column col = new Column(colName, type);
        result.setColumn(col);
        result.setIsNullable(true);
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
                        Joiner.on(".").join(tblName.getTbl(),colName));
            }
            Column col = desc.getTable().getColumn(colName);
            if (col != null) {
                if (result != null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NON_UNIQ_ERROR, colName);
                }
                result = desc;
            }
        }

        return result;
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
        for (TupleId tid: tids) {
            if (!globalState.fullOuterJoinedTupleIds.containsKey(tid)) continue;
            TableRef currentOuterJoin = globalState.fullOuterJoinedTupleIds.get(tid);
            globalState.fullOuterJoinedConjuncts.put(e.getId(), currentOuterJoin);
            break;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("registerFullOuterJoinedConjunct: " +
                    globalState.fullOuterJoinedConjuncts.toString());
        }
    }

    /**
     * Register tids as being outer-joined by a full outer join clause represented by
     * rhsRef.
     */
    public void registerFullOuterJoinedTids(List<TupleId> tids, TableRef rhsRef) {
        for (TupleId tid: tids) {
            globalState.fullOuterJoinedTupleIds.put(tid, rhsRef);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("registerFullOuterJoinedTids: " +
                    globalState.fullOuterJoinedTupleIds.toString());
        }
    }

    /**
     * Register tids as being outer-joined by Join clause represented by rhsRef.
     */
    public void registerOuterJoinedTids(List<TupleId> tids, TableRef rhsRef) {
        for (TupleId tid: tids) {
            globalState.outerJoinedTupleIds.put(tid, rhsRef);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("registerOuterJoinedTids: " +
                    globalState.outerJoinedTupleIds.toString());
        }
    }

    /**
     * Register the given tuple id as being the invisible side of a semi-join.
     */
    public void registerSemiJoinedTid(TupleId tid, TableRef rhsRef) {
        globalState.semiJoinedTupleIds.put(tid, rhsRef);
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
        for (Expr conjunct: e.getConjuncts()) {
            registerConjunct(conjunct);
            if (ids != null) {
                for (TupleId id : ids) {
                    registerConstantConjunct(id, conjunct);
                }
            }
            markConstantConjunct(conjunct, fromHavingClause);
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
        for (Expr e: getUnassignedConjuncts(tupleIds, true)) {
            if (canEvalPredicate(tupleIds, e)) result.add(e);
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
                    && !globalState.assignedConjuncts.contains(e.getId())
                    && ((inclOjConjuncts && !e.isConstant())
                    || !globalState.ojClauseByConjunct.containsKey(e.getId()))) {
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
     * Return all unassigned registered conjuncts that are fully bound by given
     * list of tuple ids
     */
    public List<Expr> getAllUnassignedConjuncts(List<TupleId> tupleIds) {
        List<Expr> result = Lists.newArrayList();
        for (Expr e : globalState.conjuncts.values()) {
            if (!e.isAuxExpr() 
                && e.isBoundByTupleIds(tupleIds) 
                && !globalState.assignedConjuncts.contains(e.getId()) 
                && !globalState.ojClauseByConjunct.containsKey(e.getId())) {
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
     * Returns true if 'e' must be evaluated after or by a join node. Note that it may
     * still be safe to evaluate 'e' elsewhere as well, but in any case 'e' must be
     * evaluated again by or after a join.
     */
    public boolean evalAfterJoin(Expr e) {
        List<TupleId> tids = Lists.newArrayList();
        e.getIds(tids, null);
        if (tids.isEmpty()) return false;
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
        if (tableName.isFullyQualified()) {
            return tableName;
        }
        return new TableName(getDefaultDb(), tableName.getTbl());
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
        if (tblRef == null) return null;
        return (tblRef.getJoinOp().isAntiJoin()) ? tblRef : null;
    }

    public boolean isFullOuterJoined(TupleId tid) {
        return globalState.fullOuterJoinedTupleIds.containsKey(tid);
    }

    public boolean isFullOuterJoined(SlotId sid) {
        return isFullOuterJoined(getTupleId(sid));
    }

    public boolean isVisible(TupleId tid) {
        return tid == visibleSemiJoinedTupleId_ || !isSemiJoined(tid);
    }

    public boolean containsOuterJoinedTid(List<TupleId> tids) {
        for (TupleId tid: tids) {
            if (isOuterJoined(tid)) return true;
        }
        return false;
    }

    public DescriptorTable getDescTbl() {
        return globalState.descTbl;
    }

    public Catalog getCatalog() {
        return globalState.catalog;
    }

    public Set<String> getAliases() {
        return uniqueTableAliasSet_;
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

    /**
     * Makes the given semi-joined tuple visible such that its slots can be referenced.
     * If tid is null, makes the currently visible semi-joined tuple invisible again.
     */
    public void setVisibleSemiJoinedTuple(TupleId tid) {
        Preconditions.checkState(tid == null
                || globalState.semiJoinedTupleIds.containsKey(tid));
        Preconditions.checkState(tid == null || visibleSemiJoinedTupleId_ == null);
        visibleSemiJoinedTupleId_ = tid;
    }

    /**
     * Return true if this analyzer has no ancestors. (i.e. false for the analyzer created
     * for inline views/ union operands, etc.)
     */
    public boolean isRootAnalyzer() { return ancestors.isEmpty(); }

    public boolean hasAncestors() { return !ancestors.isEmpty(); }
    public Analyzer getParentAnalyzer() {
        return hasAncestors() ? ancestors.get(0) : null;
    }

    /**
     * Returns true if the query block corresponding to this analyzer is guaranteed
     * to return an empty result set, e.g., due to a limit 0 or a constant predicate
     * that evaluates to false.
     */
    public boolean hasEmptyResultSet() { return hasEmptyResultSet_; }
    public void setHasEmptyResultSet() { hasEmptyResultSet_ = true; }

    public boolean hasEmptySpjResultSet() { return hasEmptySpjResultSet_; }

    public void setHasLimitOffsetClause(boolean hasLimitOffset) {
        this.hasLimitOffsetClause_ = hasLimitOffset;
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
        for (Expr conjunct: conjuncts) {
            conjunct.setIsOnClauseConjunct(true);
            registerConjunct(conjunct);
            if (rhsRef.getJoinOp().isOuterJoin()) {
                globalState.ojClauseByConjunct.put(conjunct.getId(), rhsRef);
                ojConjuncts.add(conjunct.getId());
            }
            if (rhsRef.getJoinOp().isSemiJoin()) {
                globalState.sjClauseByConjunct.put(conjunct.getId(), rhsRef);
            }
            if (rhsRef.getJoinOp().isInnerJoin()) {
                globalState.ijClauseByConjunct.put(conjunct.getId(), rhsRef);
            }
            markConstantConjunct(conjunct, false);
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
    private void markConstantConjunct(Expr conjunct, boolean fromHavingClause)
            throws AnalysisException {
        if (!conjunct.isConstant() || isOjConjunct(conjunct)) return;
        if ((!fromHavingClause && !hasEmptySpjResultSet_)
                || (fromHavingClause && !hasEmptyResultSet_)) {
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
                final Expr newConjunct = conjunct.getResultValue();
                if (newConjunct instanceof BoolLiteral) {
                    final BoolLiteral value = (BoolLiteral)newConjunct;
                    if (!value.getValue()) {
                        if (fromHavingClause) {
                            hasEmptyResultSet_ = true;
                        } else {
                            hasEmptySpjResultSet_ = true;
                        }
                    }
                    markConjunctAssigned(conjunct);
                }
                if (newConjunct instanceof NullLiteral) {
                    if (fromHavingClause) {
                        hasEmptyResultSet_ = true;
                    } else {
                        hasEmptySpjResultSet_ = true;
                    }
                    markConjunctAssigned(conjunct);
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
        if (outerJoin == null) return true;
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
        for (TupleId rhsId: rhsTblRefIds) {
            List<ExprId> cids = globalState.eqJoinConjuncts.get(rhsId);
            if (cids == null) continue;
            for (ExprId eid: cids) {
                if (!conjunctIds.contains(eid)) conjunctIds.add(eid);
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
        for (ExprId conjunctId: conjunctIds) {
            Expr e = globalState.conjuncts.get(conjunctId);
            Preconditions.checkState(e != null);
            if (!canEvalFullOuterJoinedConjunct(e, nodeTblRefIds) ||
                    !canEvalAntiJoinedConjunct(e, nodeTblRefIds) ||
                    !canEvalOuterJoinedConjunct(e, nodeTblRefIds)) {
                continue;
            }

            if (ojClauseConjuncts != null && !ojClauseConjuncts.contains(conjunctId)) continue;
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
            // TODO(zc)
            compatibleType = Type.getCmpType(compatibleType, exprs.get(i).getType());
        }
        if (compatibleType.equals(Type.VARCHAR)) {
            if (exprs.get(0).getType().isDateType()) {
                compatibleType = Type.DATETIME;
            }
        }
        // Add implicit casts if necessary.
        for (int i = 0; i < exprs.size(); ++i) {
            if (exprs.get(i).getType() != compatibleType) {
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
        if (exprLists == null || exprLists.size() < 2) return;

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

    public String getSchemaTable() {
        return schemaTable;
    }

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
    public void setSchemaInfo(String db, String table, String wild) {
        schemaDb = db;
        schemaTable = table;
        schemaWild = wild;
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
    /**
     * Returns true if predicate 'e' can be correctly evaluated by a tree materializing
     * 'tupleIds', otherwise false:
     * - the predicate needs to be bound by tupleIds
     * - a Where clause predicate can only be correctly evaluated if for all outer-joined
     *   referenced tids the last join to outer-join this tid has been materialized
     * - an On clause predicate against the non-nullable side of an Outer Join clause
     *   can only be correctly evaluated by the join node that materializes the
     *   Outer Join clause
     */
    private boolean canEvalPredicate(PlanNode node, Expr e) {
        return canEvalPredicate(node.getTblRefIds(), e);
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
         
            if (isAntiJoinedConjunct(e)) return canEvalAntiJoinedConjunct(e, tupleIds);
            if (isIjConjunct(e) || isSjConjunct(e)) {
                if (!containsOuterJoinedTid(tids)) return true;
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

            if (isFullOuterJoined(e)) return canEvalFullOuterJoinedConjunct(e, tupleIds);
            if (isOjConjunct(e)) {
                // Force this predicate to be evaluated by the corresponding outer join node.
                // The join node will pick up the predicate later via getUnassignedOjConjuncts().
                if (tids.size() > 1) return false;
                // Optimization for single-tid predicates: Legal to assign below the outer join
                // if the predicate is from the same On-clause that makes tid nullable
                // (otherwise e needn't be true when that tuple is set).
                TupleId tid = tids.get(0);
                return globalState.ojClauseByConjunct.get(e.getId()) == getLastOjClause(tid);
            }
        }

        for (TupleId tid: tids) {
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
        if (antiJoinRef == null) return true;
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
        if (fullOuterJoin == null) return true;
        return tids.containsAll(fullOuterJoin.getAllTableRefIds());
    }

    /**
     * Return all unassigned registered conjuncts for node's table ref ids.
     * Wrapper around getUnassignedConjuncts(List<TupleId> tupleIds).
     */
    public List<Expr> getUnassignedConjuncts(PlanNode node) {
        return getUnassignedConjuncts(node.getTblRefIds());
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
                ||  globalState.conjunctsByOjClause.containsKey(e.getId())) {
            return true;
        }

        return false;
    }
    /**
     * Mark all slots that are referenced in exprs as materialized.
     */
    public void materializeSlots(List<Expr> exprs) {
        List<SlotId> slotIds = Lists.newArrayList();

        for (Expr e: exprs) {
            Preconditions.checkState(e.isAnalyzed);
            e.getIds(null, slotIds);
        }

        for (TupleDescriptor tupleDesc: this.getDescTbl().getTupleDescs()) {
            for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
                if (slotIds.contains(slotDesc.getId())) {
                    slotDesc.setIsMaterialized(true);
                }
            }
        }
    }

    public Map<String, View> getLocalViews() { return localViews_; }

    public boolean isOuterJoined(TupleId tid) {
        return globalState.outerJoinedTupleIds.containsKey(tid);
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
     *
     * TODO(zxy) Use value-transfer graph to check
     */
    public boolean hasValueTransfer(SlotId a, SlotId b) {
        return a.equals(b);
    }

    /**
     * Returns sorted slot IDs with value transfers from 'srcSid'.
     * Time complexity: O(V) where V = number of slots
     *
     * TODO(zxy) Use value-transfer graph to check
     */
    public List<SlotId> getValueTransferTargets(SlotId srcSid) {
        List<SlotId> result = new ArrayList<>();
        result.add(srcSid);
        return result;
    }

    /**
     * Returns true if any of the given slot ids or their value-transfer targets belong
     * to an outer-joined tuple.
     */
    public boolean hasOuterJoinedValueTransferTarget(List<SlotId> sids) {
        for (SlotId srcSid: sids) {
            for (SlotId dstSid: getValueTransferTargets(srcSid)) {
                if (isOuterJoined(getTupleId(dstSid))) return true;
            }
        }
        return false;
    }
}
