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

package org.apache.doris.nereids;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.Id;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.rules.analysis.ColumnAliasGenerator;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.TableId;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShortCircuitQueryContext;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.statistics.Statistics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;

/**
 * Statement context for nereids
 */
public class StatementContext implements Closeable {
    private static final Logger LOG = LogManager.getLogger(StatementContext.class);

    /**
     * indicate where the table come from.
     * QUERY: in query sql directly
     * INSERT_TARGET: the insert target table
     * MTMV: mtmv itself and its related tables witch do not belong to this sql, but maybe used in rewrite by mtmv.
     */
    public enum TableFrom {
        QUERY,
        INSERT_TARGET,
        MTMV
    }

    private ConnectContext connectContext;

    private final Stopwatch stopwatch = Stopwatch.createUnstarted();

    @GuardedBy("this")
    private final Map<String, Supplier<Object>> contextCacheMap = Maps.newLinkedHashMap();

    private OriginStatement originStatement;
    // NOTICE: we set the plan parsed by DorisParser to parsedStatement and if the plan is command, create a
    // LogicalPlanAdapter with the logical plan in the command.
    private StatementBase parsedStatement;
    private ColumnAliasGenerator columnAliasGenerator;

    private int joinCount = 0;
    private int maxNAryInnerJoin = 0;

    private boolean isDpHyp = false;

    private boolean hasNondeterministic = false;

    // hasUnknownColStats true if any column stats in the tables used by this sql is unknown
    // the algorithm to derive plan when column stats are unknown is implemented in cascading framework, not in dphyper.
    // And hence, when column stats are unknown, even if the tables used by a sql is more than
    // MAX_TABLE_COUNT_USE_CASCADES_JOIN_REORDER, join reorder should choose cascading framework.
    // Thus hasUnknownColStats has higher priority than isDpHyp
    private boolean hasUnknownColStats = false;

    private final IdGenerator<ExprId> exprIdGenerator;
    private final IdGenerator<ObjectId> objectIdGenerator = ObjectId.createGenerator();
    private final IdGenerator<RelationId> relationIdGenerator = RelationId.createGenerator();
    private final IdGenerator<CTEId> cteIdGenerator = CTEId.createGenerator();
    private final IdGenerator<TableId> talbeIdGenerator = TableId.createGenerator();

    private final Map<CTEId, Set<LogicalCTEConsumer>> cteIdToConsumers = new HashMap<>();
    private final Map<CTEId, Set<Slot>> cteIdToOutputIds = new HashMap<>();
    private final Map<RelationId, Set<Expression>> consumerIdToFilters = new HashMap<>();
    // Used to update consumer's stats
    private final Map<CTEId, List<Pair<Multimap<Slot, Slot>, Group>>> cteIdToConsumerGroup = new HashMap<>();
    private final Map<CTEId, LogicalPlan> rewrittenCteProducer = new HashMap<>();
    private final Map<CTEId, LogicalPlan> rewrittenCteConsumer = new HashMap<>();
    private final Set<String> viewDdlSqlSet = Sets.newHashSet();
    private final SqlCacheContext sqlCacheContext;

    // generate for next id for prepared statement's placeholders, which is connection level
    private final IdGenerator<PlaceholderId> placeHolderIdGenerator = PlaceholderId.createGenerator();
    // relation id to placeholders for prepared statement, ordered by placeholder id
    private final Map<PlaceholderId, Expression> idToPlaceholderRealExpr = new TreeMap<>();
    // map placeholder id to comparison slot, which will used to replace conjuncts directly
    private final Map<PlaceholderId, SlotReference> idToComparisonSlot = new TreeMap<>();

    // collect all hash join conditions to compute node connectivity in join graph
    private final List<Expression> joinFilters = new ArrayList<>();

    private final List<Hint> hints = new ArrayList<>();

    // Map slot to its relation, currently used in SlotReference to find its original
    // Relation for example LogicalOlapScan
    private final Map<Slot, Relation> slotToRelation = Maps.newHashMap();

    private BitSet disableRules;

    // table locks
    private final Stack<CloseableResource> plannerResources = new Stack<>();

    // placeholder params for prepared statement
    private List<Placeholder> placeholders;

    // all tables in query
    private boolean needLockTables = true;

    // tables in this query directly
    private final Map<List<String>, TableIf> tables = Maps.newHashMap();
    // tables maybe used by mtmv rewritten in this query
    private final Map<List<String>, TableIf> mtmvRelatedTables = Maps.newHashMap();
    // insert into target tables
    private final Map<List<String>, TableIf> insertTargetTables = Maps.newHashMap();
    // save view's def to avoid them change before lock
    private final Map<List<String>, String> viewInfos = Maps.newHashMap();

    // for create view support in nereids
    // key is the start and end position of the sql substring that needs to be replaced,
    // and value is the new string used for replacement.
    private final TreeMap<Pair<Integer, Integer>, String> indexInSqlToString
            = new TreeMap<>(new Pair.PairComparator<>());
    // Record table id mapping, the key is the hash code of union catalogId, databaseId, tableId
    // the value is the auto-increment id in the cascades context
    private final Map<TableIdentifier, TableId> tableIdMapping = new LinkedHashMap<>();
    // Record the materialization statistics by id which is used for cost estimation.
    // Maybe return null, which means the id according statistics should calc normally rather than getting
    // form this map
    private final Map<RelationId, Statistics> relationIdToStatisticsMap = new LinkedHashMap<>();

    // Indicates the query is short-circuited in both plan and execution phase, typically
    // for high speed/concurrency point queries
    private boolean isShortCircuitQuery;

    private ShortCircuitQueryContext shortCircuitQueryContext;

    private FormatOptions formatOptions = FormatOptions.getDefault();

    private final List<PlannerHook> plannerHooks = new ArrayList<>();

    private final Map<MvccTableInfo, MvccSnapshot> snapshots = Maps.newHashMap();

    private boolean privChecked;

    public StatementContext() {
        this(ConnectContext.get(), null, 0);
    }

    public StatementContext(int initialId) {
        this(ConnectContext.get(), null, initialId);
    }

    public StatementContext(ConnectContext connectContext, OriginStatement originStatement) {
        this(connectContext, originStatement, 0);
    }

    /**
     * StatementContext
     */
    public StatementContext(ConnectContext connectContext, OriginStatement originStatement, int initialId) {
        this.connectContext = connectContext;
        this.originStatement = originStatement;
        exprIdGenerator = ExprId.createGenerator(initialId);
        if (connectContext != null && connectContext.getSessionVariable() != null
                && connectContext.queryId() != null
                && CacheAnalyzer.canUseSqlCache(connectContext.getSessionVariable())) {
            this.sqlCacheContext = new SqlCacheContext(
                    connectContext.getCurrentUserIdentity(), connectContext.queryId());
            if (originStatement != null) {
                this.sqlCacheContext.setOriginSql(originStatement.originStmt.trim());
            }
        } else {
            this.sqlCacheContext = null;
        }
    }

    public void setNeedLockTables(boolean needLockTables) {
        this.needLockTables = needLockTables;
    }

    /**
     * cache view info to avoid view's def and sql mode changed before lock it.
     *
     * @param qualifiedViewName full qualified name of the view
     * @param view view need to cache info
     *
     * @return view info, first is view's def sql, second is view's sql mode
     */
    public String getAndCacheViewInfo(List<String> qualifiedViewName, View view) {
        return viewInfos.computeIfAbsent(qualifiedViewName, k -> {
            String viewDef;
            view.readLock();
            try {
                viewDef = view.getInlineViewDef();
            } finally {
                view.readUnlock();
            }
            return viewDef;
        });
    }

    public Map<List<String>, TableIf> getInsertTargetTables() {
        return insertTargetTables;
    }

    public Map<List<String>, TableIf> getMtmvRelatedTables() {
        return mtmvRelatedTables;
    }

    public Map<List<String>, TableIf> getTables() {
        return tables;
    }

    public void setTables(Map<List<String>, TableIf> tables) {
        this.tables.clear();
        this.tables.putAll(tables);
    }

    /** get table by table name, try to get from information from dumpfile first */
    public TableIf getAndCacheTable(List<String> tableQualifier, TableFrom tableFrom) {
        Map<List<String>, TableIf> tables;
        switch (tableFrom) {
            case QUERY:
                tables = this.tables;
                break;
            case INSERT_TARGET:
                tables = this.insertTargetTables;
                break;
            case MTMV:
                tables = this.mtmvRelatedTables;
                break;
            default:
                throw new AnalysisException("Unknown table from " + tableFrom);
        }
        return tables.computeIfAbsent(tableQualifier, k -> RelationUtil.getTable(k, connectContext.getEnv()));
    }

    public void setConnectContext(ConnectContext connectContext) {
        this.connectContext = connectContext;
    }

    public void setHasNondeterministic(boolean hasNondeterministic) {
        this.hasNondeterministic = hasNondeterministic;
    }

    public boolean hasNondeterministic() {
        return hasNondeterministic;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public void setOriginStatement(OriginStatement originStatement) {
        this.originStatement = originStatement;
        if (originStatement != null && sqlCacheContext != null) {
            sqlCacheContext.setOriginSql(originStatement.originStmt.trim());
        }
    }

    public OriginStatement getOriginStatement() {
        return originStatement;
    }

    public Stopwatch getStopwatch() {
        return stopwatch;
    }

    public void setMaxNAryInnerJoin(int maxNAryInnerJoin) {
        if (maxNAryInnerJoin > this.maxNAryInnerJoin) {
            this.maxNAryInnerJoin = maxNAryInnerJoin;
        }
    }

    public int getMaxNAryInnerJoin() {
        return maxNAryInnerJoin;
    }

    public void setMaxContinuousJoin(int joinCount) {
        if (joinCount > this.joinCount) {
            this.joinCount = joinCount;
        }
    }

    public boolean isShortCircuitQuery() {
        return isShortCircuitQuery;
    }

    public void setShortCircuitQuery(boolean shortCircuitQuery) {
        isShortCircuitQuery = shortCircuitQuery;
    }

    public ShortCircuitQueryContext getShortCircuitQueryContext() {
        return shortCircuitQueryContext;
    }

    public void setShortCircuitQueryContext(ShortCircuitQueryContext shortCircuitQueryContext) {
        this.shortCircuitQueryContext = shortCircuitQueryContext;
    }

    public Optional<SqlCacheContext> getSqlCacheContext() {
        return Optional.ofNullable(sqlCacheContext);
    }

    public boolean isDpHyp() {
        return isDpHyp;
    }

    public void setDpHyp(boolean dpHyp) {
        isDpHyp = dpHyp;
    }

    public ExprId getNextExprId() {
        return exprIdGenerator.getNextId();
    }

    public CTEId getNextCTEId() {
        return cteIdGenerator.getNextId();
    }

    public ObjectId getNextObjectId() {
        return objectIdGenerator.getNextId();
    }

    public RelationId getNextRelationId() {
        return relationIdGenerator.getNextId();
    }

    public TableId getNextTableId() {
        return talbeIdGenerator.getNextId();
    }

    public void setParsedStatement(StatementBase parsedStatement) {
        this.parsedStatement = parsedStatement;
    }

    /** getOrRegisterCache */
    public synchronized <T> T getOrRegisterCache(String key, Supplier<T> cacheSupplier) {
        Supplier<T> supplier = (Supplier<T>) contextCacheMap.get(key);
        if (supplier == null) {
            contextCacheMap.put(key, (Supplier<Object>) Suppliers.memoize(cacheSupplier));
            supplier = cacheSupplier;
        }
        return supplier.get();
    }

    public synchronized BitSet getOrCacheDisableRules(SessionVariable sessionVariable) {
        if (this.disableRules != null) {
            return this.disableRules;
        }
        this.disableRules = sessionVariable.getDisableNereidsRules();
        return this.disableRules;
    }

    /**
     * Some value of the cacheKey may change, invalid cache when value change
     */
    public synchronized void invalidCache(String cacheKey) {
        contextCacheMap.remove(cacheKey);
        if (cacheKey.equalsIgnoreCase(SessionVariable.DISABLE_NEREIDS_RULES)) {
            this.disableRules = null;
        }
    }

    public ColumnAliasGenerator getColumnAliasGenerator() {
        return columnAliasGenerator == null
            ? columnAliasGenerator = new ColumnAliasGenerator()
            : columnAliasGenerator;
    }

    public String generateColumnName() {
        return getColumnAliasGenerator().getNextAlias();
    }

    public StatementBase getParsedStatement() {
        return parsedStatement;
    }

    public Map<CTEId, Set<LogicalCTEConsumer>> getCteIdToConsumers() {
        return cteIdToConsumers;
    }

    public Map<CTEId, Set<Slot>> getCteIdToOutputIds() {
        return cteIdToOutputIds;
    }

    public Map<RelationId, Set<Expression>> getConsumerIdToFilters() {
        return consumerIdToFilters;
    }

    public PlaceholderId getNextPlaceholderId() {
        return placeHolderIdGenerator.getNextId();
    }

    public Map<PlaceholderId, Expression> getIdToPlaceholderRealExpr() {
        return idToPlaceholderRealExpr;
    }

    public Map<PlaceholderId, SlotReference> getIdToComparisonSlot() {
        return idToComparisonSlot;
    }

    public Map<CTEId, List<Pair<Multimap<Slot, Slot>, Group>>> getCteIdToConsumerGroup() {
        return cteIdToConsumerGroup;
    }

    public Map<CTEId, LogicalPlan> getRewrittenCteProducer() {
        return rewrittenCteProducer;
    }

    public Map<CTEId, LogicalPlan> getRewrittenCteConsumer() {
        return rewrittenCteConsumer;
    }

    public void addViewDdlSql(String ddlSql) {
        this.viewDdlSqlSet.add(ddlSql);
    }

    public List<String> getViewDdlSqls() {
        return ImmutableList.copyOf(viewDdlSqlSet);
    }

    public void addHint(Hint hint) {
        this.hints.add(hint);
    }

    public List<Hint> getHints() {
        return ImmutableList.copyOf(hints);
    }

    public List<Expression> getJoinFilters() {
        return joinFilters;
    }

    public void addJoinFilters(Collection<Expression> newJoinFilters) {
        this.joinFilters.addAll(newJoinFilters);
    }

    public boolean isHasUnknownColStats() {
        return hasUnknownColStats;
    }

    public void setHasUnknownColStats(boolean hasUnknownColStats) {
        this.hasUnknownColStats = hasUnknownColStats;
    }

    public TreeMap<Pair<Integer, Integer>, String> getIndexInSqlToString() {
        return indexInSqlToString;
    }

    public void addIndexInSqlToString(Pair<Integer, Integer> pair, String replacement) {
        indexInSqlToString.put(pair, replacement);
    }

    public void addStatistics(Id id, Statistics statistics) {
        if (id instanceof RelationId) {
            this.relationIdToStatisticsMap.put((RelationId) id, statistics);
        }
    }

    public Optional<Statistics> getStatistics(Id id) {
        if (id instanceof RelationId) {
            return Optional.ofNullable(this.relationIdToStatisticsMap.get((RelationId) id));
        }
        return Optional.empty();
    }

    @VisibleForTesting
    public Map<RelationId, Statistics> getRelationIdToStatisticsMap() {
        return relationIdToStatisticsMap;
    }

    /**
     * lock all table collect by TableCollector
     */
    public synchronized void lock() {
        if (!needLockTables
                || (tables.isEmpty() && mtmvRelatedTables.isEmpty() && insertTargetTables.isEmpty())
                || !plannerResources.isEmpty()) {
            return;
        }
        PriorityQueue<TableIf> tableIfs = new PriorityQueue<>(
                tables.size() + mtmvRelatedTables.size() + insertTargetTables.size(),
                Comparator.comparing(TableIf::getId));
        tableIfs.addAll(tables.values());
        tableIfs.addAll(mtmvRelatedTables.values());
        tableIfs.addAll(insertTargetTables.values());
        while (!tableIfs.isEmpty()) {
            TableIf tableIf = tableIfs.poll();
            if (!tableIf.needReadLockWhenPlan()) {
                continue;
            }
            if (!tableIf.tryReadLock(1, TimeUnit.MINUTES)) {
                close();
                throw new RuntimeException("Failed to get read lock on table:" + tableIf.getName());
            }
            String fullTableName = tableIf.getNameWithFullQualifiers();
            String resourceName = "tableReadLock(" + fullTableName + ")";
            plannerResources.push(new CloseableResource(
                    resourceName, Thread.currentThread().getName(),
                    originStatement == null ? null : originStatement.originStmt, tableIf::readUnlock));
        }
    }

    /** releasePlannerResources */
    public synchronized void releasePlannerResources() {
        Throwable throwable = null;
        while (!plannerResources.isEmpty()) {
            try {
                plannerResources.pop().close();
            } catch (Throwable t) {
                if (throwable == null) {
                    throwable = t;
                }
            }
        }
        if (throwable != null) {
            Throwables.throwIfInstanceOf(throwable, RuntimeException.class);
            throw new IllegalStateException("Release resource failed", throwable);
        }
    }

    // CHECKSTYLE OFF
    @Override
    protected void finalize() throws Throwable {
        if (!plannerResources.isEmpty()) {
            String msg = "Resources leak: " + plannerResources;
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }
    }
    // CHECKSTYLE ON

    @Override
    public void close() {
        releasePlannerResources();
    }

    public List<Placeholder> getPlaceholders() {
        return placeholders;
    }

    public void setPlaceholders(List<Placeholder> placeholders) {
        this.placeholders = placeholders;
    }

    public void setFormatOptions(FormatOptions options) {
        this.formatOptions = options;
    }

    public FormatOptions getFormatOptions() {
        return formatOptions;
    }

    public List<PlannerHook> getPlannerHooks() {
        return plannerHooks;
    }

    public void addPlannerHook(PlannerHook plannerHook) {
        this.plannerHooks.add(plannerHook);
    }

    /**
     * Load snapshot information of mvcc
     */

    public void loadSnapshots() {
        for (TableIf tableIf : tables.values()) {
            if (tableIf instanceof MvccTable) {
                MvccTableInfo mvccTableInfo = new MvccTableInfo(tableIf);
                // may be set by MTMV, we can not load again
                if (!snapshots.containsKey(mvccTableInfo)) {
                    snapshots.put(mvccTableInfo, ((MvccTable) tableIf).loadSnapshot());
                }
            }
        }
    }

    /**
     * Obtain snapshot information of mvcc
     *
     * @param tableIf tableIf
     * @return MvccSnapshot
     */
    public Optional<MvccSnapshot> getSnapshot(TableIf tableIf) {
        if (!(tableIf instanceof MvccTable)) {
            return Optional.empty();
        }
        MvccTableInfo mvccTableInfo = new MvccTableInfo(tableIf);
        return Optional.ofNullable(snapshots.get(mvccTableInfo));
    }

    /**
     * Obtain snapshot information of mvcc
     *
     * @param mvccTableInfo mvccTableInfo
     * @param snapshot snapshot
     */
    public void setSnapshot(MvccTableInfo mvccTableInfo, MvccSnapshot snapshot) {
        snapshots.put(mvccTableInfo, snapshot);
    }

    private static class CloseableResource implements Closeable {
        public final String resourceName;
        public final String threadName;
        public final String sql;

        private final Closeable resource;

        private boolean closed;

        public CloseableResource(String resourceName, String threadName, String sql, Closeable resource) {
            this.resourceName = resourceName;
            this.threadName = threadName;
            this.sql = sql;
            this.resource = resource;
        }

        @Override
        public void close() {
            if (!closed) {
                try {
                    resource.close();
                } catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, RuntimeException.class);
                    throw new IllegalStateException("Close resource failed: " + t.getMessage(), t);
                }
                closed = true;
            }
        }

        @Override
        public String toString() {
            return "\nResource {\n  name: " + resourceName + ",\n  thread: " + threadName
                    + ",\n  sql:\n" + sql + "\n}";
        }
    }

    /** Get table id with lazy */
    public TableId getTableId(TableIf tableIf) {
        TableIdentifier tableIdentifier = new TableIdentifier(tableIf);
        TableId tableId = this.tableIdMapping.get(tableIdentifier);
        if (tableId != null) {
            return tableId;
        }
        tableId = StatementScopeIdGenerator.newTableId();
        this.tableIdMapping.put(tableIdentifier, tableId);
        return tableId;
    }

    public boolean isPrivChecked() {
        return privChecked;
    }

    public void setPrivChecked(boolean privChecked) {
        this.privChecked = privChecked;
    }
}
