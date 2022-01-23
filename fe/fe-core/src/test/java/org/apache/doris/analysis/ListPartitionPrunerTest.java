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
import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class ListPartitionPrunerTest extends PartitionPruneTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        runningDir = "fe/mocked/ListPartitionPrunerTest/" + UUID.randomUUID().toString() + "/";
        UtFrameUtils.createDorisCluster(runningDir);

        connectContext = UtFrameUtils.createDefaultCtx();

        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);

        String createSinglePartColWithSinglePartKey =
            "create table test.t1\n"
                + "(k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int)\n"
                + "partition by list(k1)\n"
                + "(\n"
                + "partition p1 values in (\"1\"),\n"
                + "partition p2 values in (\"2\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');";
        String createSinglePartColWithMultiPartKey =
            "create table test.t2\n"
                + "(k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int)\n"
                + "partition by list(k1)\n"
                + "(\n"
                + "partition p1 values in (\"1\", \"3\", \"5\"),\n"
                + "partition p2 values in (\"2\", \"4\", \"6\"),\n"
                + "partition p3 values in (\"7\", \"8\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');";
        String createMultiPartColWithSinglePartKey =
            "create table test.t3\n"
                + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                + "partition by list(k1, k2)\n"
                + "(\n"
                + "partition p1 values in ((\"1\", \"beijing\")),\n"
                + "partition p2 values in ((\"2\", \"beijing\"))\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');";
        String createMultiPartColWithMultiPartKey =
            "create table test.t4\n"
                + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                + "partition by list(k1, k2)\n"
                + "(\n"
                + "partition p1 values in ((\"1\", \"beijing\"), (\"2\", \"shanghai\")),\n"
                + "partition p2 values in ((\"2\", \"beijing\")),\n"
                + "partition p3 values in ((\"3\", \"tianjin\"), (\"1\", \"shanghai\"))\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');";

        createTable(createSinglePartColWithSinglePartKey);
        createTable(createSinglePartColWithMultiPartKey);
        createTable(createMultiPartColWithSinglePartKey);
        createTable(createMultiPartColWithMultiPartKey);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    private void initTestCases() {
        // Select by partition name
        addCase("select * from test.t1 partition p1;", "partitions=1/2", "partitions=1/2");
        addCase("select * from test.t2 partition (p2, p3);", "partitions=2/3", "partitions=2/3");
        addCase("select * from test.t3 partition (p1, p2);", "partitions=2/2", "partitions=2/2");
        addCase("select * from test.t4 partition p2;", "partitions=1/3", "partitions=1/3");

        // Single partition column
        addCase("select * from test.t2 where k1 < 7", "partitions=2/3", "partitions=2/3");
        addCase("select * from test.t2 where k1 = 1;", "partitions=1/3", "partitions=1/3");
        addCase("select * from test.t2 where k1 in (1, 2);", "partitions=2/3", "partitions=2/3");
        addCase("select * from test.t2 where k1 >= 6;", "partitions=2/3", "partitions=2/3");
        addCase("select * from test.t2 where k1 < 8 and k1 > 6;", "partitions=1/3", "partitions=1/3");
        addCase("select * from test.t2 where k2 = \"beijing\";", "partitions=3/3", "partitions=3/3");
        addCase("select * from test.t1 where k1 != 1", "partitions=2/2", "partitions=1/2");
        addCase("select * from test.t4 where k2 != \"beijing\"", "partitions=3/3", "partitions=2/3");

        // Multiple partition columns
        addCase("select * from test.t4 where k1 = 2;", "partitions=2/3", "partitions=2/3");
        addCase("select * from test.t4 where k2 = \"tianjin\";", "partitions=1/3", "partitions=1/3");
        addCase("select * from test.t4 where k1 = 1 and k2 = \"shanghai\";", "partitions=2/3", "partitions=1/3");
        addCase("select * from test.t4 where k1 in (1, 3) and k2 in (\"tianjin\", \"shanghai\");", "partitions=2/3", "partitions=1/3");
        addCase("select * from test.t4 where k1 in (1, 3);", "partitions=2/3", "partitions=2/3");
        addCase("select * from test.t4 where k2 in (\"tianjin\", \"shanghai\");", "partitions=2/3", "partitions=2/3");
        addCase("select * from test.t4 where k1 < 3;", "partitions=3/3", "partitions=3/3");
        addCase("select * from test.t4 where k1 > 2;", "partitions=1/3", "partitions=1/3");
        addCase("select * from test.t4 where k2 <\"shanghai\";", "partitions=2/3", "partitions=2/3");
        addCase("select * from test.t4 where k2 >=\"shanghai\";", "partitions=2/3", "partitions=2/3");
        addCase("select * from test.t4 where k1 > 1 and k2 < \"shanghai\";", "partitions=2/3", "partitions=1/3");
        addCase("select * from test.t4 where k1 >= 2 and k2 = \"shanghai\";", "partitions=2/3", "partitions=1/3");

        // Disjunctive predicates
        addCase("select * from test.t2 where k1=1 or k1=4", "partitions=3/3", "partitions=2/3");
        addCase("select * from test.t4 where k1=1 or k1=3", "partitions=3/3", "partitions=2/3");
        addCase("select * from test.t4 where k2=\"tianjin\" or k2=\"shanghai\"", "partitions=3/3", "partitions=2/3");
        addCase("select * from test.t4 where k1 > 1 or k2 < \"shanghai\"", "partitions=3/3", "partitions=3/3");
    }

    @Test
    public void testPartitionPrune() throws Exception {
        initTestCases();
        doTest();
    }
}
