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

package org.apache.doris.common.cache;

import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.SqlCacheContext.FullColumnName;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * What the planner writes into a statement's {@link SqlCacheContext} about the data policies of the tables it
 * read, which is what a later cache hit compares against.
 *
 * <p>The comparison itself is pinned by {@code NereidsSqlCacheDataPolicyTest}, on a context built by hand.
 * That leaves the half this class covers: the record has to be written for every column and every table
 * scanned, including the ones nothing applies to. A negative answer that goes unrecorded is invisible to the
 * comparison, which then has nothing to notice when a mask is written afterwards - and the entry cached
 * before it keeps serving the raw column until something else evicts it. That is the one direction where a
 * stale hit hands back more than the current policy allows.
 */
public class NereidsSqlCachePolicyRecordTest extends TestWithFeService {

    private static final String DB = "sql_cache_policy_record";
    private static final String TBL = "orders";
    /**
     * Not root. {@code root@'%'} and {@code admin@'%'} are exempt from every row filter and column mask -
     * an engine-reserved exemption that predates the plugin contract - so as either of them nothing is
     * looked up and nothing is recorded, and every case here would pass on an empty answer.
     */
    private static final String USER = "sql_cache_policy_user";

    private static OlapTable table;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(DB);
        useDatabase(DB);
        createTable("create table " + TBL + " (region varchar(8), phone varchar(32))"
                + " distributed by hash(region) buckets 1 properties(\"replication_num\" = \"1\");");
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(DB);
        table = (OlapTable) db.getTableOrAnalysisException(TBL);

        addUser(USER, true);
        grantPriv("GRANT SELECT_PRIV ON internal." + DB + "." + TBL + " TO '" + USER + "'@'%'");
    }

    @Test
    public void testTheAnswerIsRecordedForEveryColumnEvenWhenNothingMasksIt() throws Exception {
        SqlCacheContext recorded = planAScanRecordingItsPolicies();

        Map<FullColumnName, Optional<DataMaskSpec>> masks = recorded.getDataMaskPolicies();
        Set<String> columns = new TreeSet<>();
        for (FullColumnName column : masks.keySet()) {
            columns.add(column.getColumn());
        }
        Assertions.assertEquals(new TreeSet<>(Arrays.asList("phone", "region")), columns,
                "the planner recorded nothing about a column no mask applies to, so a mask written after"
                        + " this entry was cached has nothing to compare against and the entry keeps"
                        + " serving the raw column");
        for (Map.Entry<FullColumnName, Optional<DataMaskSpec>> entry : masks.entrySet()) {
            Assertions.assertFalse(entry.getValue().isPresent(),
                    "a mask was recorded for " + entry.getKey() + ", which no source defines one for");
        }
    }

    /** And the same for the table's row filters: an empty answer is an answer and has to be recorded. */
    @Test
    public void testTheEmptyRowFilterAnswerIsRecorded() throws Exception {
        SqlCacheContext recorded = planAScanRecordingItsPolicies();

        Set<String> tables = new LinkedHashSet<>();
        recorded.getRowPolicies().forEach((table, policies) -> {
            tables.add(table.getTable());
            Assertions.assertTrue(policies.isEmpty(),
                    "a row filter was recorded for " + table + ", which no source defines one for");
        });
        Assertions.assertEquals(new LinkedHashSet<>(Arrays.asList(TBL)), tables,
                "the planner recorded nothing about a table no row filter applies to, so a filter written"
                        + " after this entry was cached has nothing to compare against");
    }

    /**
     * Plans a scan the way a cacheable statement is planned, and hands back what it recorded.
     *
     * <p>The record is written by {@link LogicalCheckPolicy} while the {@link CheckPolicy} rule runs, and
     * only when the statement has a {@link SqlCacheContext} at all - which is what {@code enable_sql_cache}
     * decides, on the session, when the {@link StatementContext} is built.
     */
    private SqlCacheContext planAScanRecordingItsPolicies() throws Exception {
        boolean cacheEnabled = connectContext.getSessionVariable().isEnableSqlCache();
        connectContext.getSessionVariable().setEnableSqlCache(true);
        useUser(USER);
        try {
            StatementContext statementContext = new StatementContext(connectContext,
                    new OriginStatement("select * from " + TBL, 0));
            Optional<SqlCacheContext> sqlCacheContext = statementContext.getSqlCacheContext();
            Assertions.assertTrue(sqlCacheContext.isPresent(),
                    "this statement is not cacheable, so it records nothing and this case proves nothing");

            CascadesContext cascadesContext = CascadesContext.initContext(statementContext,
                    new LogicalCheckPolicy<>(new LogicalOlapScan(
                            StatementScopeIdGenerator.newRelationId(), table, Arrays.asList(DB))),
                    PhysicalProperties.GATHER);
            Rewriter.getWholeTreeRewriterWithCustomJobs(cascadesContext,
                    ImmutableList.of(Rewriter.bottomUp(new CheckPolicy()))).execute();

            return sqlCacheContext.get();
        } finally {
            useUser("root");
            connectContext.getSessionVariable().setEnableSqlCache(cacheEnabled);
        }
    }
}
