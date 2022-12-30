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
import org.apache.doris.nereids.analyzer.NereidsAnalyzer;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.rewrite.RewriteBottomUpJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteTopDownJob;
import org.apache.doris.nereids.jobs.scheduler.JobPool;
import org.apache.doris.nereids.jobs.scheduler.JobScheduler;
import org.apache.doris.nereids.jobs.scheduler.JobStack;
import org.apache.doris.nereids.jobs.scheduler.SimpleJobScheduler;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.analysis.CTEContext;
import org.apache.doris.nereids.rules.analysis.Scope;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.TimeUnit;

/**
 * Context used in memo.
 */
public class CascadesContext {
    private final Memo memo;
    private final StatementContext statementContext;

    private CTEContext cteContext;
    private RuleSet ruleSet;
    private JobPool jobPool;
    private final JobScheduler jobScheduler;
    private JobContext currentJobContext;
    // subqueryExprIsAnalyzed: whether the subquery has been analyzed.
    private final Map<SubqueryExpr, Boolean> subqueryExprIsAnalyzed;
    private final RuntimeFilterContext runtimeFilterContext;

    private List<Table> tables = null;

    public CascadesContext(Memo memo, StatementContext statementContext, PhysicalProperties requestProperties) {
        this(memo, statementContext, new CTEContext(), requestProperties);
    }

    /**
     * Constructor of OptimizerContext.
     *
     * @param memo {@link Memo} reference
     * @param statementContext {@link StatementContext} reference
     */
    public CascadesContext(Memo memo, StatementContext statementContext,
            CTEContext cteContext, PhysicalProperties requireProperties) {
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

    public static CascadesContext newContext(StatementContext statementContext,
            Plan initPlan, PhysicalProperties requireProperties) {
        return new CascadesContext(new Memo(initPlan), statementContext, requireProperties);
    }

    public NereidsAnalyzer newAnalyzer() {
        return new NereidsAnalyzer(this);
    }

    public NereidsAnalyzer newAnalyzer(Optional<Scope> outerScope) {
        return new NereidsAnalyzer(this, outerScope);
    }

    public void pushJob(Job job) {
        jobPool.push(job);
    }

    public Memo getMemo() {
        return memo;
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

    public void setRuleSet(RuleSet ruleSet) {
        this.ruleSet = ruleSet;
    }

    public JobPool getJobPool() {
        return jobPool;
    }

    public void setJobPool(JobPool jobPool) {
        this.jobPool = jobPool;
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

    public CTEContext getCteContext() {
        return cteContext;
    }

    public void setCteContext(CTEContext cteContext) {
        this.cteContext = cteContext;
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
            cascadesContext.extractTables(plan);
            for (Table table : cascadesContext.tables) {
                if (!table.tryReadLock(1, TimeUnit.MINUTES)) {
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
}
