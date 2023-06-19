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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.executor.Analyzer;
import org.apache.doris.nereids.jobs.rewrite.CustomRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteBottomUpJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteTopDownJob;
import org.apache.doris.nereids.jobs.rewrite.RootPlanTreeRewriteJob.RootRewriteJobContext;
import org.apache.doris.nereids.jobs.scheduler.JobPool;
import org.apache.doris.nereids.jobs.scheduler.JobScheduler;
import org.apache.doris.nereids.jobs.scheduler.JobStack;
import org.apache.doris.nereids.jobs.scheduler.ScheduleContext;
import org.apache.doris.nereids.jobs.scheduler.SimpleJobScheduler;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Context used in memo.
 */
public class CascadesContext implements ScheduleContext {

    // in analyze/rewrite stage, the plan will storage in this field
    private Plan plan;
    private Optional<RootRewriteJobContext> currentRootRewriteJobContext;
    // in optimize stage, the plan will storage in the memo
    private Memo memo;
    private final StatementContext statementContext;

    private CTEContext cteContext;
    private final RuleSet ruleSet;
    private final JobPool jobPool;
    private final JobScheduler jobScheduler;
    private JobContext currentJobContext;
    // subqueryExprIsAnalyzed: whether the subquery has been analyzed.
    private final Map<SubqueryExpr, Boolean> subqueryExprIsAnalyzed;
    private final RuntimeFilterContext runtimeFilterContext;
    private Optional<Scope> outerScope = Optional.empty();
    private List<Table> tables = null;

    private boolean isRewriteRoot;
    private volatile boolean isTimeout = false;

    private Map<CTEId, Set<LogicalCTEConsumer>> cteIdToConsumers = new HashMap<>();
    private Map<CTEId, Callable<LogicalPlan>> cteIdToCTEClosure = new HashMap<>();
    private Map<CTEId, Set<Expression>> cteIdToProjects = new HashMap<>();
    private Map<Integer, Set<Expression>> consumerIdToFilters = new HashMap<>();
    private Map<CTEId, Set<Integer>> cteIdToConsumerUnderProjects = new HashMap<>();

    public CascadesContext(Plan plan, Memo memo, StatementContext statementContext,
            PhysicalProperties requestProperties) {
        this(plan, memo, statementContext, new CTEContext(), requestProperties);
    }

    /**
     * Constructor of OptimizerContext.
     *
     * @param memo {@link Memo} reference
     * @param statementContext {@link StatementContext} reference
     */
    public CascadesContext(Plan plan, Memo memo, StatementContext statementContext,
            CTEContext cteContext, PhysicalProperties requireProperties) {
        this.plan = plan;
        this.memo = memo;
        this.statementContext = statementContext;
        this.ruleSet = new RuleSet();
        this.jobPool = new JobStack();
        this.jobScheduler = new SimpleJobScheduler();
        this.currentJobContext = new JobContext(this, requireProperties, Double.MAX_VALUE);
        this.subqueryExprIsAnalyzed = new HashMap<>();
        this.runtimeFilterContext = new RuntimeFilterContext(getConnectContext().getSessionVariable());
        this.cteContext = cteContext;
    }

    public static CascadesContext newRewriteContext(StatementContext statementContext,
            Plan initPlan, PhysicalProperties requireProperties) {
        return new CascadesContext(initPlan, null, statementContext, requireProperties);
    }

    public static CascadesContext newRewriteContext(StatementContext statementContext,
            Plan initPlan, CTEContext cteContext) {
        return newRewriteContext(statementContext, initPlan, cteContext, PhysicalProperties.ANY);
    }

    public static CascadesContext newRewriteContext(StatementContext statementContext,
            Plan initPlan, CTEContext cteContext, PhysicalProperties requireProperties) {
        return new CascadesContext(initPlan, null, statementContext, cteContext, requireProperties);
    }

    /**
     * New rewrite context.
     */
    public static CascadesContext newRewriteContext(CascadesContext context, Plan plan) {
        return newRewriteContext(context, plan, PhysicalProperties.ANY);
    }

    /**
     * New rewrite context copy from current context, used in cbo rewriter.
     */
    public static CascadesContext newRewriteContext(CascadesContext context,
            Plan plan, PhysicalProperties requireProperties) {
        CascadesContext cascadesContext = CascadesContext.newRewriteContext(
                context.getStatementContext(), plan, context.getCteContext(), requireProperties);
        cascadesContext.cteIdToConsumers = context.cteIdToConsumers;
        cascadesContext.cteIdToProjects = context.cteIdToProjects;
        cascadesContext.cteContext = context.cteContext;
        cascadesContext.cteIdToCTEClosure = context.cteIdToCTEClosure;
        cascadesContext.consumerIdToFilters = context.consumerIdToFilters;
        return cascadesContext;
    }

    public synchronized void setIsTimeout(boolean isTimeout) {
        this.isTimeout = isTimeout;
    }

    public synchronized boolean isTimeout() {
        return isTimeout;
    }

    public void toMemo() {
        this.memo = new Memo(plan);
    }

    public Analyzer newAnalyzer() {
        return new Analyzer(this);
    }

    @Override
    public void pushJob(Job job) {
        jobPool.push(job);
    }

    public Memo getMemo() {
        return memo;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public ConnectContext getConnectContext() {
        return statementContext.getConnectContext();
    }

    public StatementContext getStatementContext() {
        return statementContext;
    }

    public RuleSet getRuleSet() {
        return ruleSet;
    }

    @Override
    public JobPool getJobPool() {
        return jobPool;
    }

    public JobScheduler getJobScheduler() {
        return jobScheduler;
    }

    public JobContext getCurrentJobContext() {
        return currentJobContext;
    }

    public RuntimeFilterContext getRuntimeFilterContext() {
        return runtimeFilterContext;
    }

    public void setCurrentJobContext(JobContext currentJobContext) {
        this.currentJobContext = currentJobContext;
    }

    public CascadesContext setJobContext(PhysicalProperties physicalProperties) {
        this.currentJobContext = new JobContext(this, physicalProperties, Double.MAX_VALUE);
        return this;
    }

    public Plan getRewritePlan() {
        return plan;
    }

    public void setRewritePlan(Plan plan) {
        this.plan = plan;
    }

    public Optional<RootRewriteJobContext> getCurrentRootRewriteJobContext() {
        return currentRootRewriteJobContext;
    }

    public void setCurrentRootRewriteJobContext(
            RootRewriteJobContext currentRootRewriteJobContext) {
        this.currentRootRewriteJobContext = Optional.ofNullable(currentRootRewriteJobContext);
    }

    public void setSubqueryExprIsAnalyzed(SubqueryExpr subqueryExpr, boolean isAnalyzed) {
        subqueryExprIsAnalyzed.put(subqueryExpr, isAnalyzed);
    }

    public boolean subqueryIsAnalyzed(SubqueryExpr subqueryExpr) {
        if (subqueryExprIsAnalyzed.get(subqueryExpr) == null) {
            setSubqueryExprIsAnalyzed(subqueryExpr, false);
            return false;
        }
        return subqueryExprIsAnalyzed.get(subqueryExpr);
    }

    public CascadesContext bottomUpRewrite(RuleFactory... rules) {
        return execute(new RewriteBottomUpJob(memo.getRoot(), currentJobContext, ImmutableList.copyOf(rules)));
    }

    public CascadesContext bottomUpRewrite(Rule... rules) {
        return bottomUpRewrite(ImmutableList.copyOf(rules));
    }

    public CascadesContext bottomUpRewrite(List<Rule> rules) {
        return execute(new RewriteBottomUpJob(memo.getRoot(), rules, currentJobContext));
    }

    public CascadesContext topDownRewrite(RuleFactory... rules) {
        return execute(new RewriteTopDownJob(memo.getRoot(), currentJobContext, ImmutableList.copyOf(rules)));
    }

    public CascadesContext topDownRewrite(Rule... rules) {
        return topDownRewrite(ImmutableList.copyOf(rules));
    }

    public CascadesContext topDownRewrite(List<Rule> rules) {
        return execute(new RewriteTopDownJob(memo.getRoot(), rules, currentJobContext));
    }

    public CascadesContext topDownRewrite(CustomRewriter customRewriter) {
        CustomRewriteJob customRewriteJob = new CustomRewriteJob(() -> customRewriter, RuleType.TEST_REWRITE);
        customRewriteJob.execute(currentJobContext);
        toMemo();
        return this;
    }

    public CTEContext getCteContext() {
        return cteContext;
    }

    public void setCteContext(CTEContext cteContext) {
        this.cteContext = cteContext;
    }

    public void setIsRewriteRoot(boolean isRewriteRoot) {
        this.isRewriteRoot = isRewriteRoot;
    }

    public boolean isRewriteRoot() {
        return isRewriteRoot;
    }

    public Optional<Scope> getOuterScope() {
        return outerScope;
    }

    public void setOuterScope(@Nullable Scope outerScope) {
        this.outerScope = Optional.ofNullable(outerScope);
    }

    /**
     * getAndCacheSessionVariable
     */
    public <T> T getAndCacheSessionVariable(String cacheName,
            T defaultValue, Function<SessionVariable, T> variableSupplier) {
        ConnectContext connectContext = getConnectContext();
        if (connectContext == null) {
            return defaultValue;
        }

        StatementContext statementContext = getStatementContext();
        if (statementContext == null) {
            return defaultValue;
        }
        T cacheResult = statementContext.getOrRegisterCache(cacheName,
                () -> variableSupplier.apply(connectContext.getSessionVariable()));
        return cacheResult;
    }

    private CascadesContext execute(Job job) {
        pushJob(job);
        jobScheduler.executeJobPool(this);
        return this;
    }

    /**
     * Extract tables.
     */
    public void extractTables(LogicalPlan logicalPlan) {
        Set<UnboundRelation> relations = getTables(logicalPlan);
        tables = new ArrayList<>();
        for (UnboundRelation r : relations) {
            try {
                tables.add(getTable(r));
            } catch (Throwable e) {
                // IGNORE
            }
        }
    }

    /** get table by table name, try to get from information from dumpfile first */
    public Table getTableByName(String tableName) {
        Preconditions.checkState(tables != null);
        for (Table table : tables) {
            if (table.getName().equals(tableName)) {
                return table;
            }
        }
        return null;
    }

    public List<Table> getTables() {
        return tables;
    }

    private Set<UnboundRelation> getTables(LogicalPlan logicalPlan) {
        Set<UnboundRelation> unboundRelations = new HashSet<>();
        logicalPlan.foreach(p -> {
            if (p instanceof LogicalFilter) {
                unboundRelations.addAll(extractUnboundRelationFromFilter((LogicalFilter) p));
            } else if (p instanceof LogicalCTE) {
                unboundRelations.addAll(extractUnboundRelationFromCTE((LogicalCTE) p));
            } else {
                unboundRelations.addAll(p.collect(UnboundRelation.class::isInstance));
            }
        });
        return unboundRelations;
    }

    private Set<UnboundRelation> extractUnboundRelationFromFilter(LogicalFilter filter) {
        Set<SubqueryExpr> subqueryExprs = filter.getPredicate()
                .collect(SubqueryExpr.class::isInstance);
        Set<UnboundRelation> relations = new HashSet<>();
        for (SubqueryExpr expr : subqueryExprs) {
            LogicalPlan plan = expr.getQueryPlan();
            relations.addAll(getTables(plan));
        }
        return relations;
    }

    private Set<UnboundRelation> extractUnboundRelationFromCTE(LogicalCTE cte) {
        List<LogicalSubQueryAlias<Plan>> subQueryAliases = cte.getAliasQueries();
        Set<UnboundRelation> relations = new HashSet<>();
        for (LogicalSubQueryAlias<Plan> subQueryAlias : subQueryAliases) {
            relations.addAll(getTables(subQueryAlias));
        }
        return relations;
    }

    private Table getTable(UnboundRelation unboundRelation) {
        List<String> nameParts = unboundRelation.getNameParts();
        switch (nameParts.size()) {
            case 1: { // table
                String dbName = getConnectContext().getDatabase();
                return getTable(dbName, nameParts.get(0), getConnectContext().getEnv());
            }
            case 2: { // db.table
                String dbName = nameParts.get(0);
                if (!dbName.equals(getConnectContext().getDatabase())) {
                    dbName = getConnectContext().getClusterName() + ":" + dbName;
                }
                return getTable(dbName, nameParts.get(1), getConnectContext().getEnv());
            }
            default:
                throw new IllegalStateException("Table name [" + unboundRelation.getTableName() + "] is invalid.");
        }
    }

    /**
     * Find table from catalog.
     */
    public Table getTable(String dbName, String tableName, Env env) {
        Database db = env.getInternalCatalog().getDb(dbName)
                .orElseThrow(() -> new RuntimeException("Database [" + dbName + "] does not exist."));
        db.readLock();
        try {
            return db.getTable(tableName).orElseThrow(() -> new RuntimeException(
                    "Table [" + tableName + "] does not exist in database [" + dbName + "]."));
        } finally {
            db.readUnlock();
        }
    }

    /**
     * Used to lock table
     */
    public static class Lock implements AutoCloseable {

        CascadesContext cascadesContext;

        private Stack<Table> locked = new Stack<>();

        /**
         * Try to acquire read locks on tables, throw runtime exception once the acquiring for read lock failed.
         */
        public Lock(LogicalPlan plan, CascadesContext cascadesContext) {
            this.cascadesContext = cascadesContext;
            // tables can also be load from dump file
            if (cascadesContext.getTables() == null) {
                cascadesContext.extractTables(plan);
            }
            for (Table table : cascadesContext.tables) {
                if (!table.tryReadLock(1, TimeUnit.MINUTES)) {
                    close();
                    throw new RuntimeException(String.format("Failed to get read lock on table: %s", table.getName()));
                }
                locked.push(table);
            }
        }

        @Override
        public void close() {
            while (!locked.empty()) {
                locked.pop().readUnlock();
            }
        }
    }

    public void putCTEIdToCTEClosure(CTEId cteId, Callable<LogicalPlan> cteClosure) {
        this.cteIdToCTEClosure.put(cteId, cteClosure);
    }

    public void putAllCTEIdToCTEClosure(Map<CTEId, Callable<LogicalPlan>> cteConsumers) {
        this.cteIdToCTEClosure.putAll(cteConsumers);
    }

    public void putCTEIdToConsumer(LogicalCTEConsumer cteConsumer) {
        Set<LogicalCTEConsumer> consumers =
                this.cteIdToConsumers.computeIfAbsent(cteConsumer.getCteId(), k -> new HashSet<>());
        consumers.add(cteConsumer);
    }

    public void putAllCTEIdToConsumer(Map<CTEId, Set<LogicalCTEConsumer>> cteConsumers) {
        this.cteIdToConsumers.putAll(cteConsumers);
    }

    public void putCTEIdToProject(CTEId cteId, Expression p) {
        Set<Expression> projects = this.cteIdToProjects.computeIfAbsent(cteId, k -> new HashSet<>());
        projects.add(p);
    }

    public Set<Expression> getProjectForProducer(CTEId cteId) {
        return this.cteIdToProjects.get(cteId);
    }

    /**
     * Fork for rewritten child tree of CTEProducer.
     */
    public CascadesContext forkForCTEProducer(Plan plan) {
        CascadesContext cascadesContext = new CascadesContext(plan, memo, statementContext, PhysicalProperties.ANY);
        cascadesContext.cteIdToConsumers = cteIdToConsumers;
        cascadesContext.cteIdToProjects = cteIdToProjects;
        cascadesContext.cteContext = cteContext;
        cascadesContext.cteIdToCTEClosure = cteIdToCTEClosure;
        cascadesContext.consumerIdToFilters = consumerIdToFilters;
        return cascadesContext;
    }

    public int cteReferencedCount(CTEId cteId) {
        Set<LogicalCTEConsumer> cteConsumer = cteIdToConsumers.get(cteId);
        if (cteConsumer == null) {
            return 0;
        }
        return cteIdToConsumers.get(cteId).size();
    }

    public Map<CTEId, Set<LogicalCTEConsumer>> getCteIdToConsumers() {
        return cteIdToConsumers;
    }

    public Map<CTEId, Callable<LogicalPlan>> getCteIdToCTEClosure() {
        return cteIdToCTEClosure;
    }

    public LogicalPlan findCTEPlanForInline(CTEId cteId) {
        try {
            return cteIdToCTEClosure.get(cteId).call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void putConsumerIdToFilter(int id, Expression filter) {
        Set<Expression> filters = this.consumerIdToFilters.computeIfAbsent(id, k -> new HashSet<>());
        filters.add(filter);
    }

    public Map<Integer, Set<Expression>> getConsumerIdToFilters() {
        return consumerIdToFilters;
    }

    public void markConsumerUnderProject(LogicalCTEConsumer cteConsumer) {
        Set<Integer> consumerIds =
                this.cteIdToConsumerUnderProjects.computeIfAbsent(cteConsumer.getCteId(), k -> new HashSet<>());
        consumerIds.add(cteConsumer.getConsumerId());
    }

    public boolean couldPruneColumnOnProducer(CTEId cteId) {
        Set<Integer> consumerIds = this.cteIdToConsumerUnderProjects.get(cteId);
        return consumerIds.size() == this.cteIdToConsumers.get(cteId).size();
    }
}
