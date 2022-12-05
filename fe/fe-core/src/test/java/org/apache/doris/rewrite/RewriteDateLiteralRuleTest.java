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
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.DorisAssert;

import org.junit.Assert;

public class RewriteDateLiteralRuleTest {
    private DorisAssert dorisAssert;
    private static final String DB_NAME = "rewritedaterule";
    private static final String TABLE_NAME_1 = "tb1";
    private static final String TABLE_NAME_2 = "tb2";

    public void before(ConnectContext ctx) throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
        dorisAssert = new DorisAssert(ctx);
        dorisAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        String createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME_1
                + " (k1 datetime, k2 int) "
                + "distributed by hash(k2) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
        createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME_2
                + " (k1 datetime(3), k2 int) "
                + "distributed by hash(k2) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
    }

    public void after() throws Exception {
        dorisAssert.dropDB(DB_NAME);
    }

    public void testWithIntFormatDate() throws Exception {
        String query = "select * from " + DB_NAME + ".tb1 where k1 > 20210301";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 00:00:00'"));
        query = "select k1 > 20210301 from " + DB_NAME + ".tb1";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 00:00:00'"));
        query = "select k1 > 20210301223344 from " + DB_NAME + ".tb1";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 22:33:44'"));
    }

    public void testWithIntFormatDateV2() throws Exception {
        String query = "select * from " + DB_NAME + ".tb2 where k1 > 20210301";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 00:00:00'"));
        query = "select k1 > 20210301 from " + DB_NAME + ".tb2";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 00:00:00'"));
        query = "select k1 > 20210301223344 from " + DB_NAME + ".tb2";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 22:33:44'"));
    }

    public void testWithStringFormatDate() throws Exception {
        String query = "select * from " + DB_NAME + ".tb1 where k1 > '2021030112334455'";
        String planString = dorisAssert.query(query).explainQuery();
        if (Config.enable_date_conversion) {
            Assert.assertTrue(planString.contains("`k1` > '2021-03-01 12:33:44.550000'"));
        } else {
            Assert.assertTrue(planString.contains("`k1` > '2021-03-01 12:33:44'"));
        }

        query = "select k1 > '20210301' from " + DB_NAME + ".tb1";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 00:00:00'"));

        query = "select k1 > '20210301233234.34' from " + DB_NAME + ".tb1";
        planString = dorisAssert.query(query).explainQuery();
        if (Config.enable_date_conversion) {
            Assert.assertTrue(planString.contains("`k1` > '2021-03-01 23:32:34.340000'"));
        } else {
            Assert.assertTrue(planString.contains("`k1` > '2021-03-01 23:32:34'"));
        }

        query = "select * from " + DB_NAME + ".tb1 where k1 > '2021-03-01'";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 00:00:00'"));

        query = "select k1 > '2021-03-01 11:22:33' from " + DB_NAME + ".tb1";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 11:22:33'"));

        query = "select k1 > '2021-03-01  16:22:33' from " + DB_NAME + ".tb1";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 16:22:33'"));

        query = "select k1 > '2021-03-01 11:22' from " + DB_NAME + ".tb1";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 11:22:00'"));

        query = "select k1 > '20210301T221133' from " + DB_NAME + ".tb1";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 22:11:33'"));

        query = "select k1 > '2021-03-01dd 11:22' from " + DB_NAME + ".tb1";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 00:00:00'"));

        query = "select k1 > '80-03-01 11:22' from " + DB_NAME + ".tb1";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '1980-03-01 11:22:00'"));

        query = "select k1 > '12-03-01 11:22' from " + DB_NAME + ".tb1";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2012-03-01 11:22:00'"));
    }

    public void testWithStringFormatDateV2() throws Exception {
        String query = "select * from " + DB_NAME + ".tb2 where k1 > '2021030112334455'";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 12:33:44.550000'"));

        query = "select k1 > '20210301' from " + DB_NAME + ".tb2";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 00:00:00'"));

        query = "select k1 > '20210301233234.34' from " + DB_NAME + ".tb2";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 23:32:34.340000'"));

        query = "select * from " + DB_NAME + ".tb2 where k1 > '2021-03-01'";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 00:00:00'"));

        query = "select k1 > '2021-03-01 11:22:33' from " + DB_NAME + ".tb2";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 11:22:33'"));

        query = "select k1 > '2021-03-01 16:22:33' from " + DB_NAME + ".tb2";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 16:22:33'"));

        query = "select k1 > '2021-03-01 11:22' from " + DB_NAME + ".tb2";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 11:22:00'"));

        query = "select k1 > '20210301T221133' from " + DB_NAME + ".tb2";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 22:11:33'"));

        query = "select k1 > '2021-03-01 11:22' from " + DB_NAME + ".tb2";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2021-03-01 11:22:00'"));

        query = "select k1 > '80-03-01 11:22' from " + DB_NAME + ".tb2";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '1980-03-01 11:22:00'"));

        query = "select k1 > '12-03-01 11:22' from " + DB_NAME + ".tb2";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k1` > '2012-03-01 11:22:00'"));
    }

    public void testWithDoubleFormatDate() throws Exception {
        String query = "select * from " + DB_NAME + ".tb1 where k1 > 20210301.22";
        String planString = dorisAssert.query(query).explainQuery();
        if (Config.enable_decimal_conversion) {
            Assert.assertTrue(planString.contains("`k1` > 20210301"));
        } else {
            Assert.assertTrue(planString.contains("`k1` > 2.021030122E7"));
        }

        query = "select k1 > 20210331.22 from " + DB_NAME + ".tb1";
        planString = dorisAssert.query(query).explainQuery();
        if (Config.enable_decimal_conversion) {
            Assert.assertTrue(planString.contains("`k1` > 20210331"));
        } else {
            Assert.assertTrue(planString.contains("`k1` > 2.021033122E7"));
        }
    }

    public void testWithDoubleFormatDateV2() throws Exception {
        String query = "select * from " + DB_NAME + ".tb2 where k1 > 20210301.22";
        String planString = dorisAssert.query(query).explainQuery();
        if (Config.enable_decimal_conversion) {
            Assert.assertTrue(planString.contains("`k1` > 20210301"));
        } else {
            Assert.assertTrue(planString.contains("`k1` > 2.021030122E7"));
        }

        query = "select k1 > 20210331.22 from " + DB_NAME + ".tb2";
        planString = dorisAssert.query(query).explainQuery();
        if (Config.enable_decimal_conversion) {
            Assert.assertTrue(planString.contains("`k1` > 20210331"));
        } else {
            Assert.assertTrue(planString.contains("`k1` > 2.021033122E7"));
        }
    }

    public void testWithInvalidFormatDate() throws Exception {
        String query = "select * from " + DB_NAME + ".tb1 where k1 > '2021030125334455'";
        try {
            dorisAssert.query(query).explainQuery();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains(
                    "Incorrect datetime value: '2021030125334455' in expression: `k1` > '2021030125334455'"));
        }

        query = "select k1 > '2021030125334455' from " + DB_NAME + ".tb1";
        String plainString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(plainString.contains("NULL"));

        query = "select * from " + DB_NAME + ".tb1 where k1 > '2021-03-32 23:33:55'";
        try {
            dorisAssert.query(query).explainQuery();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains(
                    "Incorrect datetime value: '2021-03-32 23:33:55' in expression: `k1` > '2021-03-32 23:33:55'"));
        }

        query = "select * from " + DB_NAME + ".tb1 where k1 > '2021-03- 03 23:33:55'";
        try {
            dorisAssert.query(query).explainQuery();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains(
                    "Incorrect datetime value: '2021-03- 03 23:33:55' in expression: `k1` > '2021-03- 03 23:33:55'"));
        }

        query = "select k1 > '2021-03- 03 23:33:55' from " + DB_NAME + ".tb1";
        plainString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(plainString.contains("NULL"));
    }

    public void testWithInvalidFormatDateV2() throws Exception {
        String query = "select * from " + DB_NAME + ".tb2 where k1 > '2021030125334455'";
        try {
            dorisAssert.query(query).explainQuery();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains(
                    "Incorrect datetime value: '2021030125334455' in expression: `k1` > '2021030125334455'"));
        }

        query = "select k1 > '2021030125334455' from " + DB_NAME + ".tb2";
        String plainString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(plainString.contains("NULL"));

        query = "select * from " + DB_NAME + ".tb2 where k1 > '2021-03-32 23:33:55'";
        try {
            dorisAssert.query(query).explainQuery();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains(
                    "Incorrect datetime value: '2021-03-32 23:33:55' in expression: `k1` > '2021-03-32 23:33:55'"));
        }

        query = "select * from " + DB_NAME + ".tb2 where k1 > '2021-03- 03 23:33:55'";
        try {
            dorisAssert.query(query).explainQuery();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains(
                    "Incorrect datetime value: '2021-03- 03 23:33:55' in expression: `k1` > '2021-03- 03 23:33:55'"));
        }

        query = "select k1 > '2021-03- 03 23:33:55' from " + DB_NAME + ".tb2";
        plainString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(plainString.contains("NULL"));
    }
}
