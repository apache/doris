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

package org.apache.doris.planner;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class RepeatNodeTest {

    private static String runningDir = "fe/mocked/RepeatNodeTest/" + UUID.randomUUID() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();

        // disable bucket shuffle join
        Deencapsulation.setField(connectContext.getSessionVariable(), "enableBucketShuffleJoin", false);

        // create database
        String createDbStmtStr = "create database testdb;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);


        String createMycostSql =
                " CREATE TABLE `testdb`.`mycost` (\n" + "  `id` tinyint(4) NULL,\n" + "  `name` varchar(20) NULL,\n"
                        + "  `date` date NULL,\n" + "  `cost` bigint(20) SUM NULL\n" + ") ENGINE=OLAP\n"
                        + "AGGREGATE KEY(`id`, `name`, `date`)\n" + "COMMENT 'OLAP'\n" + "PARTITION BY RANGE(`date`)\n"
                        + "(PARTITION p2020 VALUES [('0000-01-01'), ('2021-01-01')),\n"
                        + "PARTITION p2021 VALUES [('2021-01-01'), ('2022-01-01')),\n"
                        + "PARTITION p2022 VALUES [('2022-01-01'), ('2023-01-01')))\n"
                        + "DISTRIBUTED BY HASH(`id`) BUCKETS 8\n" + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n" + "\"in_memory\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\"\n" + ");";

        Catalog.getCurrentCatalog()
                .createTable((CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createMycostSql, connectContext));

        String createMypeopleSql =
                " CREATE TABLE `testdb`.`mypeople` (\n" + "  `id` bigint(20) NULL,\n" + "  `name` varchar(20) NULL,\n"
                        + "  `sex` varchar(10) NULL,\n" + "  `age` int(11) NULL,\n" + "  `phone` char(15) NULL,\n"
                        + "  `address` varchar(50) NULL\n" + ") ENGINE=OLAP\n" + "DUPLICATE KEY(`id`, `name`)\n"
                        + "COMMENT 'OLAP'\n" + "DISTRIBUTED BY HASH(`id`) BUCKETS 8\n" + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n" + "\"in_memory\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\"\n" + ");";
        Catalog.getCurrentCatalog()
                .createTable((CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createMypeopleSql, connectContext));
    }

    @Test
    public void testNormal() throws Exception {
        String sql = "select id, name, sum(cost), grouping_id(id, name) from testdb.mycost group by cube(id, name);";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql);
        Assert.assertTrue(explainString.contains("exprs: `id`, `name`, `cost`"));
        Assert.assertTrue(explainString.contains(
                "output slots: ``id``, ``name``, ``cost``, ``GROUPING_ID``, ``GROUPING_PREFIX_`id`_`name```"));
    }

    @Test
    public void testExpr() throws Exception {
        String sql1 = "select if(c.id > 0, 1, 0) as id_, p.name, sum(c.cost) from testdb.mycost c "
                + "join testdb.mypeople p on c.id = p.id group by grouping sets((id_, name),());";
        String explainString1 = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql1);
        Assert.assertTrue(explainString1.contains(
                "output slots: `if(`c`.`id` > 0, 1, 0)`, ``p`.`name``, ``c`.`cost``, ``GROUPING_ID``"));

        String sql2 = "select (id + 1) id_, name, sum(cost) from testdb.mycost group by grouping sets((id_, name),());";
        String explainString2 = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql2);
        Assert.assertTrue(explainString2.contains("exprs: (`id` + 1), `name`, `cost`"));
        Assert.assertTrue(explainString2.contains(" output slots: `(`id` + 1)`, ``name``, ``cost``, ``GROUPING_ID``"));

        String sql3 = "select 1 as id_, name, sum(cost) from testdb.mycost group by grouping sets((id_, name),());";
        String explainString3 = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql3);
        Assert.assertTrue(explainString3.contains("exprs: 1, `name`, `cost`"));
        Assert.assertTrue(explainString3.contains("output slots: `1`, ``name``, ``cost``, ``GROUPING_ID``"));
    }
}
