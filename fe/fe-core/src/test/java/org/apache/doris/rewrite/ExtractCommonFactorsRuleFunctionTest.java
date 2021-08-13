// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package org.apache.doris.rewrite;

import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExtractCommonFactorsRuleFunctionTest {
    private static String baseDir = "fe";
    private static String runningDir = baseDir + "/mocked/ExtractCommonFactorsRuleFunctionTest/"
            + UUID.randomUUID().toString() + "/";
    private static DorisAssert dorisAssert;
    private static final String DB_NAME = "db1";
    private static final String TABLE_NAME_1 = "tb1";
    private static final String TABLE_NAME_2 = "tb2";

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinDorisCluster(runningDir);
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        String createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME_1
                + " (k1 int, k2 int) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
        createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME_2
                + " (k1 int, k2 int) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UtFrameUtils.cleanDorisFeDir(baseDir);
    }

    @Test
    public void testWithoutRewritten() throws Exception {
        String query = "select * from tb1, tb2 where (tb1.k1 =1) or (tb2.k2=1)";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("CROSS JOIN"));
    }

    @Test
    public void testCommonFactors() throws Exception {
        String query = "select * from tb1, tb2 where (tb1.k1=tb2.k1 and tb1.k2 =1) or (tb1.k1=tb2.k1 and tb2.k2=1)";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("HASH JOIN"));
        Assert.assertEquals(1, StringUtils.countMatches(planString, "`tb1`.`k1` = `tb2`.`k1`"));
    }

    @Test
    public void testWideCommonFactorsWithEqualPredicate() throws Exception {
        String query = "select * from tb1, tb2 where (tb1.k1=1 and tb2.k1=1) or (tb1.k1 =2 and tb2.k1=2)";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("(`tb1`.`k1` = 1 OR `tb1`.`k1` = 2)"));
        Assert.assertTrue(planString.contains("(`tb2`.`k1` = 1 OR `tb2`.`k1` = 2)"));
        Assert.assertTrue(planString.contains("CROSS JOIN"));
    }

    @Test
    public void testWithoutWideCommonFactorsWhenInfinityRangePredicate() throws Exception {
        String query = "select * from tb1, tb2 where (tb1.k1>1 and tb2.k1=1) or (tb1.k1 <2 and tb2.k2=2)";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertFalse(planString.contains("(`tb1`.`k1` > 1 OR `tb1`.`k1` < 2)"));
        Assert.assertTrue(planString.contains("CROSS JOIN"));
    }

    @Test
    public void testWideCommonFactorsWithMergeRangePredicate() throws Exception {
        String query = "select * from tb1, tb2 where (tb1.k1 between 1 and 3 and tb2.k1=1) or (tb1.k1 <2 and tb2.k2=2)";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` <= 3"));
        Assert.assertTrue(planString.contains("CROSS JOIN"));
    }

    @Test
    public void testWideCommonFactorsWithIntersectRangePredicate() throws Exception {
        String query = "select * from tb1, tb2 where (tb1.k1 >1 and tb1.k1 <3 and tb1.k1 <5 and tb2.k1=1) "
                + "or (tb1.k1 <2 and tb2.k2=2)";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` < 5"));
        Assert.assertTrue(planString.contains("CROSS JOIN"));
    }

    @Test
    public void testWideCommonFactorsWithDuplicateRangePredicate() throws Exception {
        String query = "select * from tb1, tb2 where (tb1.k1 >1 and tb1.k1 >1 and tb1.k1 <5 and tb2.k1=1) "
                + "or (tb1.k1 <2 and tb2.k2=2)";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` < 5"));
        Assert.assertTrue(planString.contains("CROSS JOIN"));
    }

    @Test
    public void testWideCommonFactorsWithInPredicate() throws Exception {
        String query = "select * from tb1, tb2 where (tb1.k1 in (1) and tb2.k1 in(1)) "
                + "or (tb1.k1 in(2) and tb2.k1 in(2))";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` IN (1, 2)"));
        Assert.assertTrue(planString.contains("`tb2`.`k1` IN (1, 2)"));
        Assert.assertTrue(planString.contains("CROSS JOIN"));
    }

    @Test
    public void testWideCommonFactorsWithDuplicateInPredicate() throws Exception {
        String query = "select * from tb1, tb2 where (tb1.k1 in (1,2) and tb2.k1 in(1,2)) "
                + "or (tb1.k1 in(3) and tb2.k1 in(2))";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` IN (1, 2, 3)"));
        Assert.assertTrue(planString.contains("`tb2`.`k1` IN (1, 2)"));
        Assert.assertTrue(planString.contains("CROSS JOIN"));
    }

    @Test
    public void testWideCommonFactorsWithRangeAndIn() throws Exception {
        String query = "select * from tb1, tb2 where (tb1.k1 between 1 and 3 and tb2.k1 in(1,2)) "
                + "or (tb1.k1 between 2 and 4 and tb2.k1 in(3))";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` >= 1"));
        Assert.assertTrue(planString.contains("`tb1`.`k1` <= 4"));
        Assert.assertTrue(planString.contains("`tb2`.`k1` IN (1, 2, 3)"));
        Assert.assertTrue(planString.contains("CROSS JOIN"));
    }

    @Test
    public void testWideCommonFactorsAndCommonFactors() throws Exception {
        String query = "select * from tb1, tb2 where (tb1.k1 between 1 and 3 and tb1.k1=tb2.k1) "
                + "or (tb1.k1=tb2.k1 and tb1.k1 between 2 and 4)";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` >= 1"));
        Assert.assertTrue(planString.contains("`tb1`.`k1` <= 4"));
        Assert.assertTrue(planString.contains("`tb1`.`k1` = `tb2`.`k1`"));
        Assert.assertTrue(planString.contains("HASH JOIN"));
    }

    // TPC-H Q19
    @Test
    public void testComplexQuery() throws Exception {
        String createTableSQL = "CREATE TABLE `lineitem` (\n" +
                "  `l_orderkey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `l_partkey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `l_suppkey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `l_linenumber` int(11) NOT NULL COMMENT \"\",\n" +
                "  `l_quantity` decimal(15, 2) NOT NULL COMMENT \"\",\n" +
                "  `l_extendedprice` decimal(15, 2) NOT NULL COMMENT \"\",\n" +
                "  `l_discount` decimal(15, 2) NOT NULL COMMENT \"\",\n" +
                "  `l_tax` decimal(15, 2) NOT NULL COMMENT \"\",\n" +
                "  `l_returnflag` char(1) NOT NULL COMMENT \"\",\n" +
                "  `l_linestatus` char(1) NOT NULL COMMENT \"\",\n" +
                "  `l_shipdate` date NOT NULL COMMENT \"\",\n" +
                "  `l_commitdate` date NOT NULL COMMENT \"\",\n" +
                "  `l_receiptdate` date NOT NULL COMMENT \"\",\n" +
                "  `l_shipinstruct` char(25) NOT NULL COMMENT \"\",\n" +
                "  `l_shipmode` char(10) NOT NULL COMMENT \"\",\n" +
                "  `l_comment` varchar(44) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`l_orderkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ");";
        dorisAssert.withTable(createTableSQL);
        createTableSQL = "CREATE TABLE `part` (\n" +
                "  `p_partkey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `p_name` varchar(55) NOT NULL COMMENT \"\",\n" +
                "  `p_mfgr` char(25) NOT NULL COMMENT \"\",\n" +
                "  `p_brand` char(10) NOT NULL COMMENT \"\",\n" +
                "  `p_type` varchar(25) NOT NULL COMMENT \"\",\n" +
                "  `p_size` int(11) NOT NULL COMMENT \"\",\n" +
                "  `p_container` char(10) NOT NULL COMMENT \"\",\n" +
                "  `p_retailprice` decimal(15, 2) NOT NULL COMMENT \"\",\n" +
                "  `p_comment` varchar(23) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`p_partkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ");";
        dorisAssert.withTable(createTableSQL);
        String query = "select sum(l_extendedprice* (1 - l_discount)) as revenue "
                + "from lineitem, part "
                + "where ( p_partkey = l_partkey and p_brand = 'Brand#11' "
                + "and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') "
                + "and l_quantity >= 9 and l_quantity <= 9 + 10 "
                + "and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') "
                + "and l_shipinstruct = 'DELIVER IN PERSON' ) "
                + "or ( p_partkey = l_partkey and p_brand = 'Brand#21' "
                + "and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') "
                + "and l_quantity >= 20 and l_quantity <= 20 + 10 "
                + "and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') "
                + "and l_shipinstruct = 'DELIVER IN PERSON' ) "
                + "or ( p_partkey = l_partkey and p_brand = 'Brand#32' "
                + "and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') "
                + "and l_quantity >= 26 and l_quantity <= 26 + 10 "
                + "and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') "
                + "and l_shipinstruct = 'DELIVER IN PERSON' )";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("HASH JOIN"));
        Assert.assertTrue(planString.contains("`l_partkey` = `p_partkey`"));
        Assert.assertTrue(planString.contains("`l_shipmode` IN ('AIR', 'AIR REG')"));
        Assert.assertTrue(planString.contains("`l_shipinstruct` = 'DELIVER IN PERSON'"));
        Assert.assertTrue(planString.contains("((`l_quantity` >= 9 AND `l_quantity` <= 19) "
                + "OR (`l_quantity` >= 20 AND `l_quantity` <= 36))"));
        Assert.assertTrue(planString.contains("`p_size` >= 1"));
        Assert.assertTrue(planString.contains("(`p_brand` = 'Brand#11' OR `p_brand` = 'Brand#21' OR `p_brand` = 'Brand#32')"));
        Assert.assertTrue(planString.contains("`p_size` <= 15"));
        Assert.assertTrue(planString.contains("`p_container` IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', "
                + "'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')"));
    }

}
