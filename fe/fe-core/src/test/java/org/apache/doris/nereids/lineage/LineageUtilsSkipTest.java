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

package org.apache.doris.nereids.lineage;

import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public class LineageUtilsSkipTest extends TestWithFeService {

    private String[] originalPlugins;
    private boolean originalEnableInternalSchemaDb;

    @Override
    protected void beforeCluster() {
        originalEnableInternalSchemaDb = FeConstants.enableInternalSchemaDb;
        FeConstants.enableInternalSchemaDb = true;
    }

    @Override
    protected void runAfterAll() throws Exception {
        FeConstants.enableInternalSchemaDb = originalEnableInternalSchemaDb;
    }

    @BeforeEach
    public void setUpConfig() {
        originalPlugins = Config.activate_lineage_plugin;
        Config.activate_lineage_plugin = new String[] {"dataworks"};
    }

    @AfterEach
    public void resetConfig() {
        Config.activate_lineage_plugin = originalPlugins;
    }

    @Test
    public void testSkipValuesInsert() throws Exception {
        String dbName = "lineage_skip_" + UUID.randomUUID().toString().replace("-", "");
        createDatabase(dbName);
        useDatabase(dbName);
        createTable("create table " + dbName + ".t1(k1 int, v1 varchar(16)) "
                + "distributed by hash(k1) buckets 1 properties('replication_num'='1');");

        LineageEvent event = buildLineageEvent(
                "insert into " + dbName + ".t1 values (1, 'a')");
        Assertions.assertNull(event);
    }

    @Test
    public void testSkipInternalSchemaInsert() throws Exception {
        String dbName = "lineage_src_" + UUID.randomUUID().toString().replace("-", "");
        createDatabase(dbName);
        useDatabase(dbName);
        createTable("create table " + dbName + ".src(k1 int) "
                + "distributed by hash(k1) buckets 1 properties('replication_num'='1');");

        InternalSchemaInitializer.createDb();
        InternalSchemaInitializer.createTbl();

        String sql = "insert into `internal`.`__internal_schema`.`column_statistics`"
                + "(`id`, `catalog_id`, `db_id`, `tbl_id`, `idx_id`, `col_id`, `part_id`,"
                + " `count`, `ndv`, `null_count`, `min`, `max`, `data_size_in_bytes`, `update_time`, `hot_value`)"
                + " select 'test-id', '0', '1', '2', '-1', 'col', null, 1, 1, 0,"
                + " '1', '1', 1, cast('2026-01-20 10:46:41' as datetime), '1:1.0'"
                + " from " + dbName + ".src limit 1";

        LineageEvent event = buildLineageEvent(sql);
        Assertions.assertNull(event);
    }

    private LineageEvent buildLineageEvent(String sql) throws Exception {
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        LogicalPlan parsedPlan = new NereidsParser().parseSingle(sql);
        InsertIntoTableCommand command = (InsertIntoTableCommand) parsedPlan;
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(parsedPlan, statementContext);
        logicalPlanAdapter.setOrigStmt(statementContext.getOriginStatement());
        StmtExecutor executor = new StmtExecutor(connectContext, logicalPlanAdapter);
        connectContext.setStartTime();
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));

        command.initPlan(connectContext, executor, false);
        Plan lineagePlan = command.getLineagePlan().orElse(null);
        Assertions.assertNotNull(lineagePlan);
        return LineageUtils.buildLineageEvent(lineagePlan, command.getClass(), connectContext, executor);
    }
}
