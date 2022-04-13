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
// under the License.package org.apache.doris.rewrite;

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.UUID;

public class RewriteSelfCmpRuleTest {
    private static final Logger LOG = LogManager.getLogger(RewriteSelfCmpRuleTest.class);

    private static String baseDir = "fe";
    private static String runningDir = baseDir + "/mocked/RewriteSelfCmpRuleTest/"
            + UUID.randomUUID() + "/";
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
                + " (k1 int, k2 int) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UtFrameUtils.cleanDorisFeDir(baseDir);
    }

    @Test
    public void testRewriteSelfCmp() throws Exception {
        ArrayList<Pair<String, Pair<Integer, String>>> cases = new ArrayList<Pair<String, Pair<Integer, String>>>();
        cases.add(new Pair<String, Pair<Integer, String>>(
                "explain select * from db1.tbl1 where db1.tbl1.k4 != db1.tbl1.k4",
                new Pair<Integer, String>(1, "EMPTYSET")));
        cases.add(new Pair<String, Pair<Integer, String>>(
                "explain select * from db1.tbl1 where db1.tbl1.k4 = db1.tbl1.k4",
                new Pair<Integer, String>(1, "IS NOT NULL")));
        cases.add(new Pair<String, Pair<Integer, String>>(
                "explain select * from db1.tbl1 where db1.tbl1.k4 <=> db1.tbl1.k4",
                new Pair<Integer, String>(0, "<=>")));
        cases.add(new Pair<String, Pair<Integer, String>>(
                "explain select * from db1.tbl1 where db1.tbl1.k4 <=> db1.tbl1.k4",
                new Pair<Integer, String>(0, "<=>")));

        for (Pair<String, Pair<Integer, String>> pair : cases) {
            String sql = pair.first;
            Integer n = pair.second.first;
            String result = pair.second.second;

            String planString = dorisAssert.query(sql).explainQuery();
            Assert.assertEquals(Integer.parseInt(n.toString()), StringUtils.countMatches(planString, result));
        }
    }
}
