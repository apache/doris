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

import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class ListPartitionPrunerTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static DorisAssert dorisAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Config.enable_batch_delete_by_default = true;
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createDorisCluster(runningDir);

        String createSinglePartColWithSinglePartKey = "create table test.t1\n"
                + "(k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int)\n"
                + "partition by list(k1)\n"
                + "(\n"
                + "partition p1 values in (\"1\"),\n"
                + "partition p2 values in (\"2\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');";
        String createSinglePartColWithMultiPartKey = "create table test.t2\n"
                + "(k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int)\n"
                + "partition by list(k1)\n"
                + "(\n"
                + "partition p1 values in (\"1\", \"3\", \"5\"),\n"
                + "partition p2 values in (\"2\", \"4\", \"6\"),\n"
                + "partition p3 values in (\"7\", \"8\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');";
        String createMultiPartColWithSinglePartKey = "create table test.t3\n"
                + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                + "partition by list(k1, k2)\n"
                + "(\n"
                + "partition p1 values in ((\"1\", \"beijing\")),\n"
                + "partition p2 values in ((\"2\", \"beijing\"))\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');";
        String createMultiPartColWithMultiPartKey = "create table test.t4\n"
                + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                + "partition by list(k1, k2)\n"
                + "(\n"
                + "partition p1 values in ((\"1\", \"beijing\"), (\"2\", \"shanghai\")),\n"
                + "partition p2 values in ((\"2\", \"beijing\")),\n"
                + "partition p3 values in ((\"3\", \"tianjin\"), (\"1\", \"shanghai\"))\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');";
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase("test").useDatabase("test");
        dorisAssert.withTable(createSinglePartColWithSinglePartKey)
                .withTable(createSinglePartColWithMultiPartKey)
                .withTable(createMultiPartColWithSinglePartKey)
                .withTable(createMultiPartColWithMultiPartKey);
    }

    @Test
    public void testSelectWithPartition() throws Exception {
        String sql = "select * from t1 partition p1;";
        dorisAssert.query(sql).explainContains("partitions=1/2");

        sql = "select * from t2 partition (p2, p3);";
        dorisAssert.query(sql).explainContains("partitions=2/3");

        sql = "select * from t3 partition (p1, p2);";
        dorisAssert.query(sql).explainContains("partitions=2/2");

        sql = "select * from t4 partition p2;";
        dorisAssert.query(sql).explainContains("partitions=1/3");
    }

    @Test
    public void testPartitionPrune() throws Exception {
        // single partition column
        String sql = "select * from t2 where k1 < 7";
        dorisAssert.query(sql).explainContains("partitions=2/3");

        sql = "select * from t2 where k1 = 1;";
        dorisAssert.query(sql).explainContains("partitions=1/3");

        sql = "select * from t2 where k1 in (1, 2);";
        dorisAssert.query(sql).explainContains("partitions=2/3");

        sql = "select * from t2 where k1 >= 6;";
        dorisAssert.query(sql).explainContains("partitions=2/3");

        sql = "select * from t2 where k1 < 8 and k1 > 6;";
        dorisAssert.query(sql).explainContains("partitions=1/3");

        sql = "select * from t2 where k2 = \"beijing\";";
        dorisAssert.query(sql).explainContains("partitions=3/3");

        // multi partition columns
        sql = "select * from t4 where k1 = 2;";
        dorisAssert.query(sql).explainContains("partitions=2/3");

        sql = "select * from t4 where k2 = \"tianjin\";";
        dorisAssert.query(sql).explainContains("partitions=1/3");

        sql = "select * from t4 where k1 = 1 and k2 = \"shanghai\";";
        dorisAssert.query(sql).explainContains("partitions=2/3");

        sql = "select * from t4 where k1 in (1, 3) and k2 in (\"tianjin\", \"shanghai\");";
        dorisAssert.query(sql).explainContains("partitions=2/3");

        sql = "select * from t4 where k1 in (1, 3);";
        dorisAssert.query(sql).explainContains("partitions=2/3");

        sql = "select * from t4 where k2 in (\"tianjin\", \"shanghai\");";
        dorisAssert.query(sql).explainContains("partitions=2/3");

        sql = "select * from t4 where k1 < 3;";
        dorisAssert.query(sql).explainContains("partitions=3/3");

        sql = "select * from t4 where k1 > 2;";
        dorisAssert.query(sql).explainContains("partitions=1/3");

        sql = "select * from t4 where k2 <\"shanghai\";";
        dorisAssert.query(sql).explainContains("partitions=2/3");

        sql = "select * from t4 where k2 >=\"shanghai\";";
        dorisAssert.query(sql).explainContains("partitions=2/3");

        sql = "select * from t4 where k1 > 1 and k2 < \"shanghai\";";
        dorisAssert.query(sql).explainContains("partitions=2/3");

        sql = "select * from t4 where k1 >= 2 and k2 = \"shanghai\";";
        dorisAssert.query(sql).explainContains("partitions=2/3");
    }

}
