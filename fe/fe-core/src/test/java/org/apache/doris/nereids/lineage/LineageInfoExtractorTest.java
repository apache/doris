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

import org.apache.doris.common.Config;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.plugin.lineage.DataworksLineagePlugin;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.SetMultimap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class LineageInfoExtractorTest extends TestWithFeService {

    private String[] originalPlugins;
    private String originalScope;

    @BeforeEach
    public void setUpConfig() {
        originalPlugins = Config.activate_lineage_plugin;
        originalScope = Config.lineage_dataworks_enabled_scope;
        Config.activate_lineage_plugin = new String[] {"dataworks"};
        Config.lineage_dataworks_enabled_scope = "table,column";
    }

    @AfterEach
    public void resetConfig() {
        Config.activate_lineage_plugin = originalPlugins;
        Config.lineage_dataworks_enabled_scope = originalScope;
    }

    @Test
    public void testExtractLineageInfoForInsert() throws Exception {
        String dbName = createLineageTables();
        String sql = buildInsertSql(dbName);
        LineageInfo lineageInfo = buildLineageInfo(sql);

        Assertions.assertNotNull(lineageInfo.getTargetTable());
        Assertions.assertEquals("t2", lineageInfo.getTargetTable().getName());
        Assertions.assertNotNull(lineageInfo.getTargetColumns());
        Assertions.assertEquals(2, lineageInfo.getTargetColumns().size());
        Assertions.assertTrue(lineageInfo.getTableLineageSet().stream()
                .anyMatch(table -> "t1".equals(table.getName())));

        Assertions.assertFalse(lineageInfo.getDirectLineageMap().isEmpty());
        Set<LineageInfo.DirectLineageType> directTypes = new HashSet<>();
        for (SetMultimap<LineageInfo.DirectLineageType, Expression> map
                : lineageInfo.getDirectLineageMap().values()) {
            directTypes.addAll(map.keySet());
        }
        Assertions.assertTrue(directTypes.contains(LineageInfo.DirectLineageType.AGGREGATION));
        Assertions.assertTrue(directTypes.contains(LineageInfo.DirectLineageType.IDENTITY)
                || directTypes.contains(LineageInfo.DirectLineageType.TRANSFORMATION));

        if (!lineageInfo.getInDirectLineageMap().isEmpty()) {
            Set<LineageInfo.IndirectLineageType> indirectTypes = new HashSet<>();
            for (SetMultimap<LineageInfo.IndirectLineageType, Expression> map
                    : lineageInfo.getInDirectLineageMap().values()) {
                indirectTypes.addAll(map.keySet());
            }
            Assertions.assertTrue(indirectTypes.contains(LineageInfo.IndirectLineageType.FILTER));
            Assertions.assertTrue(indirectTypes.contains(LineageInfo.IndirectLineageType.GROUP_BY));
        }

        Assertions.assertEquals(dbName, lineageInfo.getDatabase());
        Assertions.assertNotNull(lineageInfo.getQueryText());
        Assertions.assertTrue(lineageInfo.getQueryText().toLowerCase().contains("insert into"));
    }

    @Test
    public void testIndirectLineageGroupByAndHavingFilter() throws Exception {
        String dbName = createLineageTables();
        String sql = "insert into " + dbName + ".t2 "
                + "select k1, sum(v1) as s from " + dbName + ".t1 "
                + "group by k1 having sum(v1) > 10";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        Set<LineageInfo.IndirectLineageType> sTypes = getIndirectTypes(lineageInfo, "s");
        Assertions.assertTrue(sTypes.contains(LineageInfo.IndirectLineageType.GROUP_BY));
        Assertions.assertTrue(sTypes.contains(LineageInfo.IndirectLineageType.FILTER));

        Set<LineageInfo.IndirectLineageType> k1Types = getIndirectTypes(lineageInfo, "k1");
        Assertions.assertFalse(k1Types.contains(LineageInfo.IndirectLineageType.GROUP_BY));
    }

    @Test
    public void testIndirectLineageJoin() throws Exception {
        String dbName = createOrderCustomerTables();
        String sql = "insert into " + dbName + ".join_out "
                + "select c.customer_name from " + dbName + ".orders o "
                + "join " + dbName + ".customers c on o.customer_id = c.id";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        Set<LineageInfo.IndirectLineageType> types = getIndirectTypes(lineageInfo, "customer_name");
        Assertions.assertTrue(types.contains(LineageInfo.IndirectLineageType.JOIN));
    }

    @Test
    public void testIndirectLineageSortAndWindow() throws Exception {
        String dbName = createOrderCustomerTables();
        String sortSql = "insert into " + dbName + ".sort_out "
                + "select order_id, amount from " + dbName + ".orders "
                + "order by created_at desc limit 10";
        LineageInfo sortInfo = buildLineageInfo(sortSql);
        Set<LineageInfo.IndirectLineageType> sortTypes = getIndirectTypes(sortInfo, "order_id");
        Assertions.assertTrue(sortTypes.contains(LineageInfo.IndirectLineageType.SORT));

        String windowSql = "insert into " + dbName + ".window_out "
                + "select order_id, row_number() over "
                + "(partition by customer_id order by created_at desc) as rn "
                + "from " + dbName + ".orders";
        LineageInfo windowInfo = buildLineageInfo(windowSql);
        Set<LineageInfo.IndirectLineageType> rnTypes = getIndirectTypes(windowInfo, "rn");
        Assertions.assertTrue(rnTypes.contains(LineageInfo.IndirectLineageType.WINDOW));
        Set<LineageInfo.IndirectLineageType> orderTypes = getIndirectTypes(windowInfo, "order_id");
        Assertions.assertFalse(orderTypes.contains(LineageInfo.IndirectLineageType.WINDOW));
    }

    @Test
    public void testDataworksPluginWritesEvent() throws Exception {
        String dbName = createLineageTables();
        String sql = buildInsertSql(dbName);
        LineageInfo lineageInfo = buildLineageInfo(sql);

        DataworksLineagePlugin plugin = new DataworksLineagePlugin();
        Logger logger = (Logger) LogManager.getLogger("lineage.dataworks");
        TestAppender appender = new TestAppender("lineageTestAppender");
        appender.start();
        Level oldLevel = logger.getLevel();
        boolean oldAdditive = logger.isAdditive();
        logger.setLevel(Level.INFO);
        logger.setAdditive(false);
        logger.addAppender(appender);
        try {
            Assertions.assertTrue(plugin.exec(lineageInfo));
        } finally {
            logger.removeAppender(appender);
            logger.setAdditive(oldAdditive);
            logger.setLevel(oldLevel);
            appender.stop();
        }

        Assertions.assertFalse(appender.getEvents().isEmpty());
        String message = appender.getEvents().get(0).getMessage().getFormattedMessage();
        JsonObject json = JsonParser.parseString(message).getAsJsonObject();
        Assertions.assertEquals(dbName, json.get("database").getAsString());
        Assertions.assertTrue(json.getAsJsonArray("tableLineages").size() > 0);
        Assertions.assertTrue(json.getAsJsonArray("columnLineages").size() > 0);
    }

    private String createLineageTables() throws Exception {
        String dbName = "lineage_db_" + UUID.randomUUID().toString().replace("-", "");
        createDatabase(dbName);
        useDatabase(dbName);
        createTable("create table " + dbName + ".t1(k1 int, v1 bigint) "
                + "distributed by hash(k1) buckets 1 properties('replication_num' = '1');");
        createTable("create table " + dbName + ".t2(k1 int, s bigint) "
                + "distributed by hash(k1) buckets 1 properties('replication_num' = '1');");
        return dbName;
    }

    private String createOrderCustomerTables() throws Exception {
        String dbName = "lineage_orders_" + UUID.randomUUID().toString().replace("-", "");
        createDatabase(dbName);
        useDatabase(dbName);
        createTable("create table " + dbName + ".orders(order_id int, customer_id int, amount bigint,"
                + " created_at datetime) distributed by hash(order_id) buckets 1"
                + " properties('replication_num' = '1');");
        createTable("create table " + dbName + ".customers(id int, customer_name varchar(32)) "
                + "distributed by hash(id) buckets 1 properties('replication_num' = '1');");
        createTable("create table " + dbName + ".join_out(customer_name varchar(32)) "
                + "distributed by hash(customer_name) buckets 1 properties('replication_num' = '1');");
        createTable("create table " + dbName + ".sort_out(order_id int, amount bigint) "
                + "distributed by hash(order_id) buckets 1 properties('replication_num' = '1');");
        createTable("create table " + dbName + ".window_out(order_id int, rn bigint) "
                + "distributed by hash(order_id) buckets 1 properties('replication_num' = '1');");
        return dbName;
    }

    private String buildInsertSql(String dbName) {
        return "insert into " + dbName + ".t2 "
                + "select k1, sum(v1) as s from " + dbName + ".t1 "
                + "where k1 > 10 group by k1";
    }

    private LineageInfo buildLineageInfo(String sql) throws Exception {
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

        LineageEvent event = LineageUtils.buildLineageEvent(lineagePlan, command.getClass(), connectContext, executor);
        Assertions.assertNotNull(event);
        Assertions.assertNotNull(event.getLineageInfo());
        return event.getLineageInfo();
    }

    private Set<LineageInfo.IndirectLineageType> getIndirectTypes(LineageInfo lineageInfo, String outputName) {
        Map<SlotReference, SetMultimap<LineageInfo.IndirectLineageType, Expression>> indirectMap =
                lineageInfo.getInDirectLineageMap();
        for (Map.Entry<SlotReference, SetMultimap<LineageInfo.IndirectLineageType, Expression>> entry
                : indirectMap.entrySet()) {
            if (entry.getKey().getName().equalsIgnoreCase(outputName)) {
                return new HashSet<>(entry.getValue().keySet());
            }
        }
        return Collections.emptySet();
    }

    private static class TestAppender extends AbstractAppender {
        private final List<LogEvent> events = new ArrayList<>();

        protected TestAppender(String name) {
            super(name, null, PatternLayout.createDefaultLayout(), true);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }

        public List<LogEvent> getEvents() {
            return events;
        }
    }
}
