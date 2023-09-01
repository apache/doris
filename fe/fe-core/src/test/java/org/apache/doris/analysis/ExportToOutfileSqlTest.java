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

import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.load.ExportJob;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.ExportCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.ParseSqlUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The `Export` sql finally generates the `Outfile` sql.
 * This test is to test whether the generated outfile sql is correct.
 */
public class ExportToOutfileSqlTest extends TestWithFeService {
    private String dbName = "testDb";
    private String tblName = "table1";

    /**
     * create a database and a table
     *
     * @throws Exception
     */
    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(dbName);
        useDatabase(dbName);
        createTable("create table " + tblName + "\n" + "(k1 int, k2 int, k3 int) "
                + "PARTITION BY RANGE(k1)\n" + "(\n"
                + "PARTITION p1 VALUES LESS THAN (\"20\"),\n"
                + "PARTITION p2 VALUES [(\"20\"),(\"30\")),\n"
                + "PARTITION p3 VALUES [(\"30\"),(\"40\")),\n"
                + "PARTITION p4 VALUES LESS THAN (\"50\")\n" + ")\n"
                + " distributed by hash(k1) buckets 10\n"
                + "properties(\"replication_num\" = \"1\");");
    }

    /**
     * test normal export, sql:
     *
     * EXPORT TABLE testDb.table1
     * TO "file:///tmp/exp_"
     *
     * @throws UserException
     */
    @Test
    public void testNormal() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1\n"
                + "TO \"file:///tmp/exp_\";";

        // This export sql should generate 1 array, and there should be 4 outfile sql in this array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10010, 10012, 10014, 10016, 10018, 10020, 10022, 10024, 10026, 10028) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql2 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10030, 10032, 10034, 10036, 10038, 10040, 10042, 10044, 10046, 10048) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql3 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10050, 10052, 10054, 10056, 10058, 10060, 10062, 10064, 10066, 10068) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql4 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10070, 10072, 10074, 10076, 10078, 10080, 10082, 10084, 10086, 10088) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(1, outfileSqlPerParallel.size());
        Assert.assertEquals(4, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
        Assert.assertEquals(outfileSql2, outfileSqlPerParallel.get(0).get(1));
        Assert.assertEquals(outfileSql3, outfileSqlPerParallel.get(0).get(2));
        Assert.assertEquals(outfileSql4, outfileSqlPerParallel.get(0).get(3));
    }

    /**
     * test normal parallelism, sql:
     *
     * EXPORT TABLE testDb.table1
     * TO "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "3"
     * );
     *
     * @throws UserException
     */
    @Test
    public void testNormalParallelism() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"4\"\n"
                + ");";

        // This export sql should generate 4 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10010, 10012, 10014, 10016, 10018, 10020, 10022, 10024, 10026, 10028) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql2 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10030, 10032, 10034, 10036, 10038, 10040, 10042, 10044, 10046, 10048) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql3 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10050, 10052, 10054, 10056, 10058, 10060, 10062, 10064, 10066, 10068) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql4 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10070, 10072, 10074, 10076, 10078, 10080, 10082, 10084, 10086, 10088) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(4, outfileSqlPerParallel.size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(1).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(2).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(3).size());

        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
        Assert.assertEquals(outfileSql2, outfileSqlPerParallel.get(1).get(0));
        Assert.assertEquals(outfileSql3, outfileSqlPerParallel.get(2).get(0));
        Assert.assertEquals(outfileSql4, outfileSqlPerParallel.get(3).get(0));
    }

    /**
     * test export single partition, sql:
     *
     * EXPORT TABLE testDb.table1 PARTITION (p1)
     * TO "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "4"
     * );
     *
     * @throws UserException
     */
    @Test
    public void testPartitionParallelism() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1)\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"4\"\n"
                + ");";

        // This export sql should generate 4 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10010, 10012, 10014) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql2 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10016, 10018, 10020) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql3 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10022, 10024) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql4 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10026, 10028) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(4, outfileSqlPerParallel.size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(1).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(2).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(3).size());

        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
        Assert.assertEquals(outfileSql2, outfileSqlPerParallel.get(1).get(0));
        Assert.assertEquals(outfileSql3, outfileSqlPerParallel.get(2).get(0));
        Assert.assertEquals(outfileSql4, outfileSqlPerParallel.get(3).get(0));
    }

    /**
     * test export multiple partition, sql:
     *
     * EXPORT TABLE testDb.table1 PARTITION (p1, p4)
     * TO "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "4"
     * );
     *
     * @throws UserException
     */
    @Test
    public void testMultiPartitionParallelism() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1, p4)\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"4\"\n"
                + ");";

        // This export sql should generate 4 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10010, 10012, 10014, 10016, 10018) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql2 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10020, 10022, 10024, 10026, 10028) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql3 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10070, 10072, 10074, 10076, 10078) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql4 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10080, 10082, 10084, 10086, 10088) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(4, outfileSqlPerParallel.size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(1).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(2).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(3).size());

        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
        Assert.assertEquals(outfileSql2, outfileSqlPerParallel.get(1).get(0));
        Assert.assertEquals(outfileSql3, outfileSqlPerParallel.get(2).get(0));
        Assert.assertEquals(outfileSql4, outfileSqlPerParallel.get(3).get(0));
    }

    /**
     * test parallelism less than tablets, sql:
     *
     * EXPORT TABLE testDb.table1 PARTITION (p1)
     * TO "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "20"
     * );
     *
     * @throws UserException
     */
    @Test
    public void testParallelismLessThanTablets() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1)\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"20\"\n"
                + ");";

        // This export sql should generate 10 array because parallelism is less than the number of tablets,
        // so set parallelism = num(tablets)
        // There should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10010) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql2 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10012) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql3 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10014) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql4 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10016) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql5 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10018) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql6 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10020) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql7 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10022) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql8 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10024) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql9 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10026) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql10 = "SELECT * FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10028) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";


        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(10, outfileSqlPerParallel.size());
        for (int i = 0; i < 10; ++i) {
            Assert.assertEquals(1, outfileSqlPerParallel.get(i).size());

        }

        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
        Assert.assertEquals(outfileSql2, outfileSqlPerParallel.get(1).get(0));
        Assert.assertEquals(outfileSql3, outfileSqlPerParallel.get(2).get(0));
        Assert.assertEquals(outfileSql4, outfileSqlPerParallel.get(3).get(0));
        Assert.assertEquals(outfileSql5, outfileSqlPerParallel.get(4).get(0));
        Assert.assertEquals(outfileSql6, outfileSqlPerParallel.get(5).get(0));
        Assert.assertEquals(outfileSql7, outfileSqlPerParallel.get(6).get(0));
        Assert.assertEquals(outfileSql8, outfileSqlPerParallel.get(7).get(0));
        Assert.assertEquals(outfileSql9, outfileSqlPerParallel.get(8).get(0));
        Assert.assertEquals(outfileSql10, outfileSqlPerParallel.get(9).get(0));
    }


    /**
     * test export specified columns, sql:
     *
     * export table testDb.table1 PARTITION (p1)
     * to "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "5",
     *     "columns" = "k3, k1, k2"
     * );
     * @throws UserException
     */
    @Test
    public void testColumns() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1)\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"5\",\n"
                + "\"columns\" = \"k3, k1, k2\""
                + ");";

        // This export sql should generate 4 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT k3, k1, k2 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10010, 10012) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql2 = "SELECT k3, k1, k2 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10014, 10016) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql3 = "SELECT k3, k1, k2 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10018, 10020) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql4 = "SELECT k3, k1, k2 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10022, 10024) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql5 = "SELECT k3, k1, k2 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10026, 10028) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(5, outfileSqlPerParallel.size());
        for (int i = 0; i < 5; ++i) {
            Assert.assertEquals(1, outfileSqlPerParallel.get(i).size());
        }


        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
        Assert.assertEquals(outfileSql2, outfileSqlPerParallel.get(1).get(0));
        Assert.assertEquals(outfileSql3, outfileSqlPerParallel.get(2).get(0));
        Assert.assertEquals(outfileSql4, outfileSqlPerParallel.get(3).get(0));
        Assert.assertEquals(outfileSql5, outfileSqlPerParallel.get(4).get(0));
    }

    /**
     * export table testDb.table1 PARTITION (p1) WHERE k1 < 3
     * to "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "5",
     *     "columns" = "k3, k2"
     * );
     *
     * @throws UserException
     */
    @Test
    public void testWhere() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1) WHERE k1 < 3\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"5\",\n"
                + "\"columns\" = \"k3, k2\""
                + ");";

        // This export sql should generate 4 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT k3, k2 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10010, 10012) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql2 = "SELECT k3, k2 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10014, 10016) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql3 = "SELECT k3, k2 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10018, 10020) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql4 = "SELECT k3, k2 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10022, 10024) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql5 = "SELECT k3, k2 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10026, 10028) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(5, outfileSqlPerParallel.size());
        for (int i = 0; i < 5; ++i) {
            Assert.assertEquals(1, outfileSqlPerParallel.get(i).size());
        }


        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
        Assert.assertEquals(outfileSql2, outfileSqlPerParallel.get(1).get(0));
        Assert.assertEquals(outfileSql3, outfileSqlPerParallel.get(2).get(0));
        Assert.assertEquals(outfileSql4, outfileSqlPerParallel.get(3).get(0));
        Assert.assertEquals(outfileSql5, outfileSqlPerParallel.get(4).get(0));
    }

    /**
     * export table testDb.table1 PARTITION (p1) where k1 < 3
     * to "file:///tmp/exp_"
     * PROPERTIES(
     *     "format" = "orc",
     *     "max_file_size" = "512MB",
     *     "parallelism" = "2",
     *     "columns" = "k3"
     * );
     * @throws UserException
     */
    @Test
    public void testFileProperties() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1) WHERE k1 < 3\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"format\" = \"orc\",\n"
                + "\"max_file_size\" = \"512MB\",\n"
                + "\"parallelism\" = \"2\",\n"
                + "\"columns\" = \"k3\""
                + ");";

        // This export sql should generate 2 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT k3 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10010, 10012, 10014, 10016, 10018) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS orc "
                + "PROPERTIES (\"max_file_size\" = \"512MB\""
                + ");";
        String outfileSql2 = "SELECT k3 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10020, 10022, 10024, 10026, 10028) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS orc "
                + "PROPERTIES (\"max_file_size\" = \"512MB\""
                + ");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(2, outfileSqlPerParallel.size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(1).size());

        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
        Assert.assertEquals(outfileSql2, outfileSqlPerParallel.get(1).get(0));
    }

    /**
     * export table testDb.table1 PARTITION (p1) where k1 < 3
     * to "hdfs:///tmp/exp_"
     * PROPERTIES(
     *     "format" = "orc",
     *     "max_file_size" = "512MB",
     *     "parallelism" = "2",
     *     "columns" = "k3"
     * )
     * WITH BROKER "broker1"
     * (
     * "username" = "doris",
     * "password"="pps"
     * );
     * @throws UserException
     */
    @Test
    public void testBrokerProperties() throws UserException {
        new MockUp<BrokerMgr>() {
            @Mock
            public boolean containsBroker(String brokerName) {
                return true;
            }

            @Mock
            public FsBroker getAnyBroker(String brokerName) {
                return new FsBroker("127.0.0.1", 9000);
            }
        };
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1) WHERE k1 < 3\n"
                + "TO \"hdfs:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"format\" = \"orc\",\n"
                + "\"max_file_size\" = \"512MB\",\n"
                + "\"parallelism\" = \"2\",\n"
                + "\"columns\" = \"k3\""
                + ")"
                + "WITH BROKER \"broker1\" \n"
                + "(\n"
                + "\"username\" = \"doris\",\n"
                + "\"password\"=\"pps\"\n"
                + ");";

        // This export sql should generate 2 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT k3 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10010, 10012, 10014, 10016, 10018) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"hdfs:///tmp/exp_\" FORMAT AS orc "
                + "PROPERTIES ("
                + "\"max_file_size\" = \"512MB\", "
                + "\"broker.name\" = \"broker1\", "
                + "\"broker.password\" = \"pps\", "
                + "\"broker.username\" = \"doris\""
                + ");";
        String outfileSql2 = "SELECT k3 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10020, 10022, 10024, 10026, 10028) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"hdfs:///tmp/exp_\" FORMAT AS orc "
                + "PROPERTIES ("
                + "\"max_file_size\" = \"512MB\", "
                + "\"broker.name\" = \"broker1\", "
                + "\"broker.password\" = \"pps\", "
                + "\"broker.username\" = \"doris\""
                + ");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(2, outfileSqlPerParallel.size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(1).size());

        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
        Assert.assertEquals(outfileSql2, outfileSqlPerParallel.get(1).get(0));
    }

    /**
     * export table testDb.table1 PARTITION (p1) where k1 < 3
     * to "s3:///tmp/exp_"
     * PROPERTIES(
     *     "format" = "orc",
     *     "max_file_size" = "512MB",
     *     "parallelism" = "2",
     *     "columns" = "k3"
     * )
     * WITH S3 (
     *   "s3.endpoint" = "https://cos.xxx.yyy.com",
     *   "s3.region" = "ap-beijing",
     *   "s3.secret_key"="ssss",
     *   "s3.access_key" = "aaaa"
     * );
     * @throws UserException
     */
    @Test
    public void testS3Properties() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1) WHERE k1 < 3\n"
                + "TO \"s3:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"format\" = \"orc\",\n"
                + "\"max_file_size\" = \"512MB\",\n"
                + "\"parallelism\" = \"2\",\n"
                + "\"columns\" = \"k3\""
                + ")"
                + "WITH S3 (\n"
                + "\"s3.endpoint\" = \"https://cos.xxx.yyy.com\",\n"
                + "\"s3.region\" = \"ap-beijing\",\n"
                + "\"s3.secret_key\"=\"ssss\",\n"
                + "\"s3.access_key\" = \"aaaa\"\n"
                + ");";

        // This export sql should generate 2 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT k3 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10010, 10012, 10014, 10016, 10018) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"s3:///tmp/exp_\" FORMAT AS orc "
                + "PROPERTIES (\"max_file_size\" = \"512MB\", "
                + "\"s3.region\" = \"ap-beijing\", "
                + "\"AWS_ENDPOINT\" = \"https://cos.xxx.yyy.com\", "
                + "\"AWS_REGION\" = \"ap-beijing\", "
                + "\"s3.endpoint\" = \"https://cos.xxx.yyy.com\", "
                + "\"s3.secret_key\" = \"ssss\", "
                + "\"s3.access_key\" = \"aaaa\", "
                + "\"AWS_SECRET_KEY\" = \"ssss\", "
                + "\"AWS_ACCESS_KEY\" = \"aaaa\""
                + ");";
        String outfileSql2 = "SELECT k3 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10020, 10022, 10024, 10026, 10028) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"s3:///tmp/exp_\" FORMAT AS orc "
                + "PROPERTIES (\"max_file_size\" = \"512MB\", "
                + "\"s3.region\" = \"ap-beijing\", "
                + "\"AWS_ENDPOINT\" = \"https://cos.xxx.yyy.com\", "
                + "\"AWS_REGION\" = \"ap-beijing\", "
                + "\"s3.endpoint\" = \"https://cos.xxx.yyy.com\", "
                + "\"s3.secret_key\" = \"ssss\", "
                + "\"s3.access_key\" = \"aaaa\", "
                + "\"AWS_SECRET_KEY\" = \"ssss\", "
                + "\"AWS_ACCESS_KEY\" = \"aaaa\""
                + ");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(2, outfileSqlPerParallel.size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(1).size());

        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
        Assert.assertEquals(outfileSql2, outfileSqlPerParallel.get(1).get(0));
    }

    /**
     * export table testDb.table1 PARTITION (p1) where k1 < 3
     * to "hdfs:///tmp/exp_"
     * PROPERTIES(
     *     "format" = "orc",
     *     "max_file_size" = "512MB",
     *     "parallelism" = "2",
     *     "columns" = "k3"
     * )
     * with HDFS (
     *     "fs.defaultFS"="hdfs://localhost:4007",
     *     "hadoop.username" = "hadoop"
     * );
     * @throws UserException
     */
    @Test
    public void testHdfsProperties() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1) WHERE k1 < 3\n"
                + "TO \"hdfs:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"format\" = \"orc\",\n"
                + "\"max_file_size\" = \"512MB\",\n"
                + "\"parallelism\" = \"2\",\n"
                + "\"columns\" = \"k3\""
                + ")"
                + "WITH HDFS (\n"
                + "\"fs.defaultFS\"=\"hdfs://localhost:4007\",\n"
                + "\"hadoop.username\" = \"hadoop\"\n"
                + ");";

        // This export sql should generate 2 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT k3 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10010, 10012, 10014, 10016, 10018) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"hdfs:///tmp/exp_\" FORMAT AS orc "
                + "PROPERTIES ("
                + "\"fs.defaultFS\" = \"hdfs://localhost:4007\", "
                + "\"max_file_size\" = \"512MB\", "
                + "\"hadoop.username\" = \"hadoop\""
                + ");";
        String outfileSql2 = "SELECT k3 FROM `default_cluster:testDb`.`table1` "
                + "TABLET(10020, 10022, 10024, 10026, 10028) "
                + "WHERE k1 < 3 "
                + "INTO OUTFILE \"hdfs:///tmp/exp_\" FORMAT AS orc "
                + "PROPERTIES ("
                + "\"fs.defaultFS\" = \"hdfs://localhost:4007\", "
                + "\"max_file_size\" = \"512MB\", "
                + "\"hadoop.username\" = \"hadoop\""
                + ");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(2, outfileSqlPerParallel.size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(1).size());

        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
        Assert.assertEquals(outfileSql2, outfileSqlPerParallel.get(1).get(0));
    }

    private LogicalPlan parseSql(String exportSql) {
        LogicalPlanAdapter logicalPlanAdapter = ParseSqlUtils.parseSingleStringSql(exportSql);
        return logicalPlanAdapter.getLogicalPlan();
    }

    // need open EnableNereidsPlanner
    private List<List<String>> getOutfileSqlPerParallel(String exportSql) throws UserException {
        ExportCommand exportCommand = (ExportCommand) parseSql(exportSql);
        List<List<String>> outfileSqlPerParallel = new ArrayList<>();
        try {
            Method getTableName = exportCommand.getClass().getDeclaredMethod("getTableName", ConnectContext.class);
            getTableName.setAccessible(true);

            Method convertPropertyKeyToLowercase = exportCommand.getClass().getDeclaredMethod(
                    "convertPropertyKeyToLowercase", Map.class);
            convertPropertyKeyToLowercase.setAccessible(true);

            Method checkAllParameter = exportCommand.getClass().getDeclaredMethod("checkAllParameter",
                    ConnectContext.class, TableName.class, Map.class);
            checkAllParameter.setAccessible(true);

            Method generateExportJob = exportCommand.getClass().getDeclaredMethod("generateExportJob",
                    ConnectContext.class, Map.class, TableName.class);
            generateExportJob.setAccessible(true);

            TableName tblName = (TableName) getTableName.invoke(exportCommand, connectContext);
            Map<String, String> lowercaseProperties = (Map<String, String>) convertPropertyKeyToLowercase.invoke(
                    exportCommand, exportCommand.getFileProperties());
            checkAllParameter.invoke(exportCommand, connectContext, tblName, lowercaseProperties);

            ExportJob job = (ExportJob) generateExportJob.invoke(exportCommand, connectContext, lowercaseProperties,
                    tblName);
            outfileSqlPerParallel = job.getOutfileSqlPerParallel();
        } catch (NoSuchMethodException e) {
            throw new UserException(e);
        } catch (InvocationTargetException e) {
            throw new UserException(e);
        } catch (IllegalAccessException e) {
            throw new UserException(e);
        }
        return outfileSqlPerParallel;
    }
}
