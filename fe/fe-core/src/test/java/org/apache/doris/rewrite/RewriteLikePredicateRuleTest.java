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

package org.apache.doris.rewrite;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class RewriteLikePredicateRuleTest {
    private static final Logger LOG = LogManager.getLogger(RewriteLikePredicateRuleTest.class);
    private static String baseDir = "fe";
    private static String runningDir = baseDir + "/mocked/RewriteLikePredicateRuleTest/"
            + UUID.randomUUID().toString() + "/";
    private static DorisAssert dorisAssert;
    private static final String DB_NAME = "db1";
    private static final String TABLE_NAME_1 = "tb1";

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createDorisCluster(runningDir);
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        String createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME_1
                + " (k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 largeint, k6 date, k7 datetime, k8 float, k9 double) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UtFrameUtils.cleanDorisFeDir(baseDir);
    }

    @Test
    public void testRewriteLikePredicate() throws Exception {
        // tinyint
        String sql = "select * from tb1 where k1 like '%4%';";
        LOG.info("EXPLAIN:{}", dorisAssert.query(sql).explainQuery());
        dorisAssert.query(sql).explainContains("CAST(`k1` AS CHARACTER) LIKE '%4%'");

        // smallint
        sql = "select * from tb1 where k2 like '%4%';";
        LOG.info("EXPLAIN:{}", dorisAssert.query(sql).explainQuery());
        dorisAssert.query(sql).explainContains("CAST(`k2` AS CHARACTER) LIKE '%4%'");

        // int
        sql = "select * from tb1 where k3 like '%4%';";
        LOG.info("EXPLAIN:{}", dorisAssert.query(sql).explainQuery());
        dorisAssert.query(sql).explainContains("CAST(`k3` AS CHARACTER) LIKE '%4%'");

        // bigint
        sql = "select * from tb1 where k4 like '%4%';";
        LOG.info("EXPLAIN:{}", dorisAssert.query(sql).explainQuery());
        dorisAssert.query(sql).explainContains("CAST(`k4` AS CHARACTER) LIKE '%4%'");

        // largeint
        sql = "select * from tb1 where k5 like '%4%';";
        LOG.info("EXPLAIN:{}", dorisAssert.query(sql).explainQuery());
        dorisAssert.query(sql).explainContains("CAST(`k5` AS CHARACTER) LIKE '%4%'");
    }

    @Test(expected = AnalysisException.class)
    public void testRewriteLikePredicateDate() throws Exception {
        // date
        String sql = "select * from tb1 where k6 like '%4%';";
        LOG.info("EXPLAIN:{}", dorisAssert.query(sql).explainQuery());
        dorisAssert.query(sql).explainQuery();
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testRewriteLikePredicateDateTime() throws Exception {
        // datetime
        String sql = "select * from tb1 where k7 like '%4%';";
        LOG.info("EXPLAIN:{}", dorisAssert.query(sql).explainQuery());
        dorisAssert.query(sql).explainContains("left operand of LIKE must be of type STRING or FIXED_POINT_TYPE: `k7` LIKE '%4%'");
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testRewriteLikePredicateFloat() throws Exception {
        // date
        String sql = "select * from tb1 where k8 like '%4%';";
        LOG.info("EXPLAIN:{}", dorisAssert.query(sql).explainQuery());
        dorisAssert.query(sql).explainQuery();
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testRewriteLikePredicateDouble() throws Exception {
        // date
        String sql = "select * from tb1 where k9 like '%4%';";
        LOG.info("EXPLAIN:{}", dorisAssert.query(sql).explainQuery());
        dorisAssert.query(sql).explainQuery();
        Assert.fail("No exception throws.");
    }
}
