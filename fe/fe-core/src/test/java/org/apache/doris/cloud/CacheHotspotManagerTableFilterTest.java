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

package org.apache.doris.cloud;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cloud.OnTablesFilter.TableFilterRule;
import org.apache.doris.cloud.OnTablesFilter.TableFilterRule.RuleType;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.WarmUpClusterCommand;
import org.apache.doris.persist.EditLog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Tests for CacheHotspotManager's table filter methods:
 * resolveTableIds() and refreshAllTableFilters().
 * Uses Mockito to mock Env.getCurrentInternalCatalog() with fake databases/tables.
 */
public class CacheHotspotManagerTableFilterTest {

    private Env env;
    private CatalogMgr mockCatalogMgr;
    private InternalCatalog mockCatalog;
    private EditLog mockEditLog;
    private CacheHotspotManager manager;
    private List<DatabaseIf<? extends TableIf>> databases;
    private Object originalCatalogMgr;
    private EditLog originalEditLog;
    private Object originalSystemInfo;

    private static Object getField(Object target, Class<?> clazz, String fieldName) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, Class<?> clazz, String fieldName, Object value) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @BeforeEach
    public void setUp() throws Exception {
        env = Env.getCurrentEnv();
        mockCatalogMgr = Mockito.mock(CatalogMgr.class);
        mockCatalog = Mockito.mock(InternalCatalog.class);
        mockEditLog = Mockito.mock(EditLog.class);

        originalCatalogMgr = getField(env, Env.class, "catalogMgr");
        originalEditLog = env.getEditLog();
        originalSystemInfo = getField(env, Env.class, "systemInfo");
        setField(env, Env.class, "catalogMgr", mockCatalogMgr);
        setField(env, Env.class, "systemInfo", Mockito.mock(CloudSystemInfoService.class));
        env.setEditLog(mockEditLog);
        Mockito.when(mockCatalogMgr.getInternalCatalog()).thenReturn(mockCatalog);

        databases = new ArrayList<>();
        Mockito.when(mockCatalog.getAllDbs()).thenAnswer(inv -> databases);

        manager = new CacheHotspotManager(Mockito.mock(CloudSystemInfoService.class));
    }

    @AfterEach
    public void tearDown() throws Exception {
        setField(env, Env.class, "catalogMgr", originalCatalogMgr);
        setField(env, Env.class, "systemInfo", originalSystemInfo);
        env.setEditLog(originalEditLog);
    }

    @SuppressWarnings("unchecked")
    private DatabaseIf<TableIf> mockDb(String name, TableIf... tables) {
        DatabaseIf<TableIf> db = Mockito.mock(DatabaseIf.class);
        Mockito.when(db.getFullName()).thenReturn(name);
        // For resolveTableIds: getTableNamesOrEmptyWithLock + getTableNullable
        HashSet<String> tableNames = new HashSet<>();
        for (TableIf t : tables) {
            tableNames.add(t.getName());
            Mockito.when(db.getTableNullable(t.getName())).thenReturn(t);
        }
        Mockito.when(db.getTableNamesOrEmptyWithLock()).thenReturn(tableNames);
        // Keep getTables for other test paths (refreshAllTableFilters)
        Mockito.when(db.getTables()).thenReturn(Arrays.asList(tables));
        return db;
    }

    private TableIf mockTable(long id, String name) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getId()).thenReturn(id);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getType()).thenReturn(TableIf.TableType.OLAP);
        Mockito.when(table.isManagedTable()).thenReturn(true);
        return table;
    }

    private TableIf mockMtmv(long id, String name) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getId()).thenReturn(id);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getType()).thenReturn(TableIf.TableType.MATERIALIZED_VIEW);
        Mockito.when(table.isManagedTable()).thenReturn(true);
        return table;
    }

    private TableIf mockView(long id, String name) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getId()).thenReturn(id);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getType()).thenReturn(TableIf.TableType.VIEW);
        Mockito.when(table.isManagedTable()).thenReturn(false);
        return table;
    }

    private OnTablesFilter buildFilter(TableFilterRule... rules) {
        return new OnTablesFilter(Arrays.asList(rules));
    }

    private Map<String, String> eventDrivenProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("sync_mode", "event_driven");
        properties.put("sync_event", "load");
        return properties;
    }

    private WarmUpClusterCommand buildEventDrivenStmt(String src, String dst, TableFilterRule... rules) {
        return new WarmUpClusterCommand(null, src, dst, false, false,
                eventDrivenProperties(), rules.length == 0 ? null : Arrays.asList(rules));
    }

    private CloudWarmUpJob createEventDrivenJob(String src, String dst, TableFilterRule... rules) throws Exception {
        long jobId = manager.createJob(buildEventDrivenStmt(src, dst, rules));
        CloudWarmUpJob job = manager.getCloudWarmUpJob(jobId);
        Assertions.assertNotNull(job);
        return job;
    }

    private CloudWarmUpJob replayEventDrivenJob(long jobId, String src, String dst, TableFilterRule... rules)
            throws Exception {
        CloudWarmUpJob.Builder builder = new CloudWarmUpJob.Builder()
                .setJobId(jobId)
                .setSrcClusterName(src)
                .setDstClusterName(dst)
                .setJobType(CloudWarmUpJob.JobType.CLUSTER)
                .setSyncMode(CloudWarmUpJob.SyncMode.EVENT_DRIVEN)
                .setSyncEvent(CloudWarmUpJob.SyncEvent.LOAD);
        if (rules.length > 0) {
            List<CloudWarmUpJob.PersistedTableFilterRule> persistedRules = new ArrayList<>();
            for (TableFilterRule rule : rules) {
                CloudWarmUpJob.PersistedTableFilterRule persistedRule =
                        new CloudWarmUpJob.PersistedTableFilterRule();
                persistedRule.ruleType = rule.getRuleType().name();
                persistedRule.pattern = rule.getRawPattern();
                persistedRules.add(persistedRule);
            }
            builder.setTableFilterRules(persistedRules);
        }
        CloudWarmUpJob job = builder.build();
        manager.replayCloudWarmUpJob(job);
        return job;
    }

    // ===== resolveTableIds() =====

    @Test
    public void testResolveTableIdsBasicMatching() {
        // Scenario: INCLUDE 'ods.*' matches all tables in ods database
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users"),
                mockTable(1003, "tmp_staging")));
        databases.add(mockDb("dw",
                mockTable(2001, "fact_sales")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));
        Map<Long, String> idNames = manager.resolveTableIds(filter);

        Assertions.assertEquals(3, idNames.size());
        Assertions.assertEquals("ods.orders", idNames.get(1001L));
        Assertions.assertEquals("ods.users", idNames.get(1002L));
        Assertions.assertEquals("ods.tmp_staging", idNames.get(1003L));
        Assertions.assertFalse(idNames.containsKey(2001L));
    }

    @Test
    public void testResolveTableIdsWithExclude() {
        // Scenario: INCLUDE 'ods.*' EXCLUDE 'ods.tmp_*' — exclude tmp tables
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "tmp_staging"),
                mockTable(1003, "tmp_data")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.EXCLUDE, "ods.tmp_*"));
        Map<Long, String> idNames = manager.resolveTableIds(filter);

        Assertions.assertEquals(1, idNames.size());
        Assertions.assertEquals("ods.orders", idNames.get(1001L));
    }

    @Test
    public void testResolveTableIdsMultipleDatabases() {
        // Scenario: INCLUDE 'ods.*', INCLUDE 'dw.fact_*' — match across two databases
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users")));
        databases.add(mockDb("dw",
                mockTable(2001, "fact_sales"),
                mockTable(2002, "dim_product"),
                mockTable(2003, "fact_orders")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.INCLUDE, "dw.fact_*"));
        Map<Long, String> idNames = manager.resolveTableIds(filter);

        Assertions.assertEquals(4, idNames.size());
        Assertions.assertEquals("ods.orders", idNames.get(1001L));
        Assertions.assertEquals("ods.users", idNames.get(1002L));
        Assertions.assertEquals("dw.fact_sales", idNames.get(2001L));
        Assertions.assertEquals("dw.fact_orders", idNames.get(2003L));
    }

    @Test
    public void testResolveTableIdsNoMatch() {
        // Scenario: pattern matches nothing → empty map
        databases.add(mockDb("ods", mockTable(1001, "orders")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "nonexistent.*"));
        Map<Long, String> idNames = manager.resolveTableIds(filter);

        Assertions.assertTrue(idNames.isEmpty());
    }

    @Test
    public void testResolveTableIdsNullFilter() {
        Map<Long, String> idNames = manager.resolveTableIds(null);
        Assertions.assertTrue(idNames.isEmpty());
    }

    @Test
    public void testResolveTableIdsSkipsViews() {
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockView(1002, "orders_view")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));
        Map<Long, String> idNames = manager.resolveTableIds(filter);

        Assertions.assertEquals(1, idNames.size());
        Assertions.assertEquals("ods.orders", idNames.get(1001L));
        Assertions.assertFalse(idNames.containsKey(1002L));
    }

    @Test
    public void testResolveTableIdsDbNameWithPrefix() {
        // CacheHotspotManager strips "default_cluster:" prefix from db name
        databases.add(mockDb("default_cluster:ods",
                mockTable(1001, "orders")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));
        Map<Long, String> idNames = manager.resolveTableIds(filter);

        Assertions.assertEquals(1, idNames.size());
        Assertions.assertEquals("ods.orders", idNames.get(1001L));
    }

    // ===== resolveTableIds() with dynamic table changes =====

    @Test
    public void testResolveTableIdsAfterNewTableCreated() {
        // Initial: ods has orders. After new table created, re-resolve picks it up.
        DatabaseIf<TableIf> odsDb = mockDb("ods", mockTable(1001, "orders"));
        databases.add(odsDb);

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));

        Map<Long, String> ids1 = manager.resolveTableIds(filter);
        Assertions.assertEquals(1, ids1.size());

        // Simulate new table created: replace the db mock to include new table
        databases.clear();
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1004, "payments")));

        Map<Long, String> ids2 = manager.resolveTableIds(filter);
        Assertions.assertEquals(2, ids2.size());
        Assertions.assertEquals("ods.orders", ids2.get(1001L));
        Assertions.assertEquals("ods.payments", ids2.get(1004L));
    }

    @Test
    public void testResolveTableIdsAfterTableDropped() {
        // Initial: ods has orders and users. After orders dropped, re-resolve removes it.
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));

        Map<Long, String> ids1 = manager.resolveTableIds(filter);
        Assertions.assertEquals(2, ids1.size());

        databases.clear();
        databases.add(mockDb("ods", mockTable(1002, "users")));

        Map<Long, String> ids2 = manager.resolveTableIds(filter);
        Assertions.assertEquals(1, ids2.size());
        Assertions.assertEquals("ods.users", ids2.get(1002L));
    }

    @Test
    public void testResolveTableIdsAfterTableRenamed() {
        // Scenario from user guide: INCLUDE 'db.order_*', rename order_2024→archive_2024 → stops matching
        databases.add(mockDb("db",
                mockTable(1001, "order_2024"),
                mockTable(1002, "order_2025")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "db.order_*"));

        Map<Long, String> ids1 = manager.resolveTableIds(filter);
        Assertions.assertEquals(2, ids1.size());

        // Rename order_2024 → archive_2024 (no longer matches order_*)
        databases.clear();
        databases.add(mockDb("db",
                mockTable(1001, "archive_2024"),
                mockTable(1002, "order_2025")));

        Map<Long, String> ids2 = manager.resolveTableIds(filter);
        Assertions.assertEquals(1, ids2.size());
        Assertions.assertEquals("db.order_2025", ids2.get(1002L));
    }

    @Test
    public void testResolveTableIdsAfterAllTablesDropped() {
        // User guide: all matched tables dropped → empty set, Job stays RUNNING
        databases.add(mockDb("ods", mockTable(1001, "orders")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));

        Map<Long, String> ids1 = manager.resolveTableIds(filter);
        Assertions.assertEquals(1, ids1.size());

        databases.clear();
        databases.add(mockDb("ods"));  // empty database

        Map<Long, String> ids2 = manager.resolveTableIds(filter);
        Assertions.assertTrue(ids2.isEmpty());
    }

    // ===== refreshAllTableFilters() =====

    @Test
    public void testRefreshAllTableFiltersUpdatesJobTableIds() throws Exception {
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users")));

        CloudWarmUpJob job = createEventDrivenJob("write_cg", "read_cg",
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));

        // Verify initial resolution picked up 2 tables with correct names
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(1001L, 1002L)),
                job.getCurrentTableIds());

        // Simulate new table created
        databases.clear();
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users"),
                mockTable(1003, "payments")));

        manager.refreshAllTableFilters();

        // Verify job now has 3 table IDs
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(1001L, 1002L, 1003L)),
                job.getCurrentTableIds());
    }

    @Test
    public void testRefreshAllTableFiltersSkipsClusterLevelJob() throws Exception {
        // Cluster-level job (no table filter) should not be affected by refresh
        databases.add(mockDb("ods", mockTable(1001, "orders")));

        CloudWarmUpJob clusterJob = replayEventDrivenJob(200L, "write_cg", "read_cg");

        // currentTableIds should be empty (no table filter)
        Assertions.assertTrue(clusterJob.getCurrentTableIds().isEmpty());

        manager.refreshAllTableFilters();

        // Still empty after refresh — cluster-level jobs are skipped
        Assertions.assertTrue(clusterJob.getCurrentTableIds().isEmpty());
    }

    @Test
    public void testRefreshAllTableFiltersHandlesTableDrop() throws Exception {
        // Setup: job matching ods.*, initially 2 tables
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users")));

        CloudWarmUpJob job = replayEventDrivenJob(300L, "write_cg", "read_cg",
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));
        Assertions.assertEquals(2, job.getCurrentTableIds().size());

        // Drop one table
        databases.clear();
        databases.add(mockDb("ods", mockTable(1002, "users")));

        manager.refreshAllTableFilters();

        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(1002L)),
                job.getCurrentTableIds());
    }

    @Test
    public void testRefreshAllTableFiltersUpdatesMatchedNamesAfterRenameStillMatches() throws Exception {
        databases.add(mockDb("db",
                mockTable(1001, "order_2024"),
                mockTable(1002, "order_2025")));

        CloudWarmUpJob job = createEventDrivenJob("write_cg", "read_cg",
                new TableFilterRule(RuleType.INCLUDE, "db.order_*"));
        Assertions.assertEquals("db.order_2024, db.order_2025", job.getJobInfo(null).get(14));

        databases.clear();
        databases.add(mockDb("db",
                mockTable(1001, "order_2024_v2"),
                mockTable(1002, "order_2025")));

        manager.refreshAllTableFilters();

        Assertions.assertEquals(new HashSet<>(Arrays.asList(1001L, 1002L)), job.getCurrentTableIds());
        Assertions.assertEquals("db.order_2024_v2, db.order_2025", job.getJobInfo(null).get(14));
    }

    @Test
    public void testCreateJobRejectsOnTablesWithoutInitialMatches() {
        databases.add(mockDb("ods", mockTable(1001, "orders")));

        WarmUpClusterCommand stmt = buildEventDrivenStmt("write_cg", "read_cg",
                new TableFilterRule(RuleType.INCLUDE, "dw.*"));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> manager.createJob(stmt));
        Assertions.assertTrue(exception.getMessage().contains("No tables matched the ON TABLES filter"));
    }

    @Test
    public void testCreateJobRejectsEquivalentDuplicateTableFilter() throws Exception {
        databases.add(mockDb("ods", mockTable(1001, "orders")));
        databases.add(mockDb("dw",
                mockTable(2001, "fact_sales"),
                mockTable(2002, "tmp_staging")));

        WarmUpClusterCommand first = buildEventDrivenStmt("write_cg", "read_cg",
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.INCLUDE, "dw.*"),
                new TableFilterRule(RuleType.EXCLUDE, "dw.tmp_*"));
        WarmUpClusterCommand second = buildEventDrivenStmt("write_cg", "read_cg",
                new TableFilterRule(RuleType.EXCLUDE, "dw.tmp_*"),
                new TableFilterRule(RuleType.INCLUDE, "dw.*"),
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));

        manager.createJob(first);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> manager.createJob(second));
        Assertions.assertTrue(exception.getMessage().contains("already has a runnable job"));
    }

    @Test
    public void testCreateJobAllowsClusterLevelAndTableLevelToCoexist() throws Exception {
        databases.add(mockDb("ods", mockTable(1001, "orders")));

        WarmUpClusterCommand clusterLevel = buildEventDrivenStmt("write_cg", "read_cg");
        WarmUpClusterCommand tableLevel = buildEventDrivenStmt("write_cg", "read_cg",
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));

        long clusterJobId = manager.createJob(clusterLevel);
        long tableJobId = manager.createJob(tableLevel);

        Assertions.assertNotEquals(clusterJobId, tableJobId);
        Assertions.assertEquals(2, manager.getAllJobInfos(10).size());
    }

    // ===== Async materialized view (MTMV) matching =====

    @Test
    public void testResolveTableIdsMatchesAsyncMaterializedView() {
        // Async MVs (MTMV) are separate table entries in the database catalog.
        // They should be matched by ON TABLES filter just like regular OlapTables.
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users"),
                mockMtmv(1003, "mv_order_summary")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));
        Map<Long, String> idNames = manager.resolveTableIds(filter);

        Assertions.assertEquals(3, idNames.size());
        Assertions.assertEquals("ods.orders", idNames.get(1001L));
        Assertions.assertEquals("ods.users", idNames.get(1002L));
        Assertions.assertEquals("ods.mv_order_summary", idNames.get(1003L));
    }

    @Test
    public void testResolveTableIdsMtmvMatchedByMvPattern() {
        // Verify async MVs can be matched by mv_* pattern while base tables are not
        databases.add(mockDb("analytics",
                mockTable(2001, "fact_sales"),
                mockMtmv(2002, "mv_daily_sales"),
                mockMtmv(2003, "mv_monthly_revenue"),
                mockTable(2004, "dim_product")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "analytics.mv_*"));
        Map<Long, String> idNames = manager.resolveTableIds(filter);

        Assertions.assertEquals(2, idNames.size());
        Assertions.assertEquals("analytics.mv_daily_sales", idNames.get(2002L));
        Assertions.assertEquals("analytics.mv_monthly_revenue", idNames.get(2003L));
        Assertions.assertFalse(idNames.containsKey(2001L));
        Assertions.assertFalse(idNames.containsKey(2004L));
    }

    @Test
    public void testResolveTableIdsMtmvExcludedByPattern() {
        // Verify async MVs can be excluded by EXCLUDE rule
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockMtmv(1002, "mv_order_summary"),
                mockMtmv(1003, "mv_user_stats")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.EXCLUDE, "ods.mv_*"));
        Map<Long, String> idNames = manager.resolveTableIds(filter);

        Assertions.assertEquals(1, idNames.size());
        Assertions.assertEquals("ods.orders", idNames.get(1001L));
    }

    @Test
    public void testResolveTableIdsMixedTableTypesAcrossDatabases() {
        // Multiple databases with mixed OlapTable and MTMV types
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockMtmv(1002, "mv_orders_agg")));
        databases.add(mockDb("dw",
                mockTable(2001, "fact_sales"),
                mockMtmv(2002, "mv_daily_report")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.INCLUDE, "dw.mv_*"));
        Map<Long, String> idNames = manager.resolveTableIds(filter);

        // ods.* matches orders + mv_orders_agg; dw.mv_* matches mv_daily_report
        Assertions.assertEquals(3, idNames.size());
        Assertions.assertEquals("ods.orders", idNames.get(1001L));
        Assertions.assertEquals("ods.mv_orders_agg", idNames.get(1002L));
        Assertions.assertEquals("dw.mv_daily_report", idNames.get(2002L));
        Assertions.assertFalse(idNames.containsKey(2001L));
    }

    @Test
    public void testRefreshAllTableFiltersPicksUpNewMtmv() throws Exception {
        // When a new async MV is created after job creation, refreshAllTableFilters picks it up
        databases.add(mockDb("ods",
                mockTable(1001, "orders")));

        CloudWarmUpJob job = createEventDrivenJob("write_cg", "read_cg",
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));
        Assertions.assertEquals(1, job.getCurrentTableIds().size());

        // Simulate async MV created
        databases.clear();
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockMtmv(1002, "mv_order_summary")));

        manager.refreshAllTableFilters();

        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(1001L, 1002L)),
                job.getCurrentTableIds());
    }

    // ========== Performance tests: regex matching throughput at scale ==========

    /**
     * Generate table name strings (db.table) for timing shouldWarmUp regex calls.
     * No mocks needed — we test the filter's regex matching performance directly.
     */
    private List<String[]> generateTableNames(int dbCount, int tablesPerDb) {
        List<String[]> names = new ArrayList<>(dbCount * tablesPerDb);
        for (int d = 0; d < dbCount; d++) {
            String db = "db_" + d;
            for (int t = 0; t < tablesPerDb; t++) {
                names.add(new String[]{db, "tbl_" + String.format("%05d", t)});
            }
        }
        return names;
    }

    @Test
    public void testShouldWarmUpPerformance10kTables() {
        List<String[]> names = generateTableNames(10, 1000); // 10K
        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "db_*.*"));

        long start = System.nanoTime();
        int matched = 0;
        for (String[] pair : names) {
            if (filter.shouldWarmUp(pair[0], pair[1])) {
                matched++;
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        Assertions.assertEquals(10000, matched);
        System.out.println("[Perf] 10K tables, wildcard match-all: " + elapsedMs + " ms");
        Assertions.assertTrue(elapsedMs < 500,
                "10K regex matches should complete within 500ms, took " + elapsedMs + " ms");
    }

    @Test
    public void testShouldWarmUpPerformance50kTables() {
        List<String[]> names = generateTableNames(50, 1000); // 50K
        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "db_*.*"));

        long start = System.nanoTime();
        int matched = 0;
        for (String[] pair : names) {
            if (filter.shouldWarmUp(pair[0], pair[1])) {
                matched++;
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        Assertions.assertEquals(50000, matched);
        System.out.println("[Perf] 50K tables, wildcard match-all: " + elapsedMs + " ms");
        Assertions.assertTrue(elapsedMs < 500,
                "50K regex matches should complete within 500ms, took " + elapsedMs + " ms");
    }

    @Test
    public void testShouldWarmUpPerformance200kTables() {
        List<String[]> names = generateTableNames(100, 2000); // 200K
        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "db_*.*"));

        long start = System.nanoTime();
        int matched = 0;
        for (String[] pair : names) {
            if (filter.shouldWarmUp(pair[0], pair[1])) {
                matched++;
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        Assertions.assertEquals(200000, matched);
        System.out.println("[Perf] 200K tables, wildcard match-all: " + elapsedMs + " ms");
        Assertions.assertTrue(elapsedMs < 1000,
                "200K regex matches should complete within 1s, took " + elapsedMs + " ms");
    }

    @Test
    public void testShouldWarmUpPerformance500kTables() {
        List<String[]> names = generateTableNames(100, 5000); // 500K
        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "db_*.*"));

        long start = System.nanoTime();
        int matched = 0;
        for (String[] pair : names) {
            if (filter.shouldWarmUp(pair[0], pair[1])) {
                matched++;
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        Assertions.assertEquals(500000, matched);
        System.out.println("[Perf] 500K tables, wildcard match-all: " + elapsedMs + " ms");
        Assertions.assertTrue(elapsedMs < 2000,
                "500K regex matches should complete within 2s, took " + elapsedMs + " ms");
    }

    @Test
    public void testShouldWarmUpPerformanceSelectivePattern50k() {
        List<String[]> names = generateTableNames(50, 1000); // 50K
        // Only match tables in db_0
        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "db_0.*"));

        long start = System.nanoTime();
        int matched = 0;
        for (String[] pair : names) {
            if (filter.shouldWarmUp(pair[0], pair[1])) {
                matched++;
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        Assertions.assertEquals(1000, matched);
        System.out.println("[Perf] 50K tables, selective db_0 pattern: " + elapsedMs + " ms");
        Assertions.assertTrue(elapsedMs < 500,
                "50K regex matches (selective) should complete within 500ms, took " + elapsedMs + " ms");
    }

    @Test
    public void testShouldWarmUpPerformanceMultipleRules50k() {
        List<String[]> names = generateTableNames(50, 1000); // 50K
        // Include db_1* tables, exclude tables ending with digit 9
        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "db_1*.*"),
                new TableFilterRule(RuleType.EXCLUDE, "*.*9"));

        long start = System.nanoTime();
        int matched = 0;
        for (String[] pair : names) {
            if (filter.shouldWarmUp(pair[0], pair[1])) {
                matched++;
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // db_1, db_10..db_19 = 11 dbs × 1000 tables = 11000 candidates
        // Exclude tables ending with "9": tbl_00009, tbl_00019, ..., tbl_00999 = 100 per db
        // Result = 11000 - 11*100 = 9900
        Assertions.assertEquals(9900, matched);
        System.out.println("[Perf] 50K tables, include+exclude: " + elapsedMs + " ms, matched=" + matched);
        Assertions.assertTrue(elapsedMs < 500,
                "50K regex matches (multi-rule) should complete within 500ms, took " + elapsedMs + " ms");
    }

    @Test
    public void testShouldWarmUpPerformanceManyRules200k() {
        List<String[]> names = generateTableNames(100, 2000); // 200K
        // 10 include rules + 5 exclude rules
        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "db_0.*"),
                new TableFilterRule(RuleType.INCLUDE, "db_1.*"),
                new TableFilterRule(RuleType.INCLUDE, "db_2.*"),
                new TableFilterRule(RuleType.INCLUDE, "db_3.*"),
                new TableFilterRule(RuleType.INCLUDE, "db_4.*"),
                new TableFilterRule(RuleType.INCLUDE, "db_5.*"),
                new TableFilterRule(RuleType.INCLUDE, "db_6.*"),
                new TableFilterRule(RuleType.INCLUDE, "db_7.*"),
                new TableFilterRule(RuleType.INCLUDE, "db_8.*"),
                new TableFilterRule(RuleType.INCLUDE, "db_9.*"),
                new TableFilterRule(RuleType.EXCLUDE, "*.tbl_00000"),
                new TableFilterRule(RuleType.EXCLUDE, "*.tbl_00001"),
                new TableFilterRule(RuleType.EXCLUDE, "*.tbl_00002"),
                new TableFilterRule(RuleType.EXCLUDE, "*.tbl_00003"),
                new TableFilterRule(RuleType.EXCLUDE, "*.tbl_00004"));

        long start = System.nanoTime();
        int matched = 0;
        for (String[] pair : names) {
            if (filter.shouldWarmUp(pair[0], pair[1])) {
                matched++;
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // 10 dbs × 2000 tables = 20000 included, minus 10 × 5 excluded = 19950
        Assertions.assertEquals(19950, matched);
        System.out.println("[Perf] 200K tables, 15 rules (10 incl + 5 excl): " + elapsedMs + " ms");
        Assertions.assertTrue(elapsedMs < 2000,
                "200K regex matches with 15 rules should complete within 2s, took " + elapsedMs + " ms");
    }

    @Test
    public void testShouldWarmUpPerformanceRepeatedCycles200k() {
        List<String[]> names = generateTableNames(100, 2000); // 200K
        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "db_*.*"));

        // JIT warm-up
        for (String[] pair : names) {
            filter.shouldWarmUp(pair[0], pair[1]);
        }

        long start = System.nanoTime();
        int iterations = 5;
        int totalMatched = 0;
        for (int i = 0; i < iterations; i++) {
            for (String[] pair : names) {
                if (filter.shouldWarmUp(pair[0], pair[1])) {
                    totalMatched++;
                }
            }
        }
        long totalMs = (System.nanoTime() - start) / 1_000_000;
        long avgMs = totalMs / iterations;

        Assertions.assertEquals(200000 * iterations, totalMatched);
        System.out.println("[Perf] 200K tables × 5 cycles: total=" + totalMs + " ms, avg=" + avgMs + " ms/cycle");
        Assertions.assertTrue(avgMs < 1000,
                "Avg per refresh cycle for 200K tables should be < 1s, avg=" + avgMs + " ms");
    }
}
