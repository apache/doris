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
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.DorisAssert;
import org.junit.Assert;


public class RewriteBinaryPredicatesRuleTest {
    private DorisAssert dorisAssert;
    private static final String DB_NAME = "RewriteBinaryPredicatesRule";
    private static final String TABLE_NAME_1 = "tb1";

    public void before(ConnectContext ctx) throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
        dorisAssert = new DorisAssert(ctx);
        dorisAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        String createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME_1
                + " (k1 int, k2 int) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
    }

    public void after() throws Exception {
        String dropDbSql = "drop database if exists " + DB_NAME;
        dorisAssert.dropDB(DB_NAME);
    }

    public void testRewriteBinaryPredicatesRule() throws Exception {
        String query = "select * from " + DB_NAME + ".tb1 where k2 = 0.0";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k2` = 0"));
        query = "select * from " + DB_NAME + ".tb1 where k2 >= 0.1";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k2` > 0"));
        query = "select * from " + DB_NAME + ".tb1 where k2 >= 0.0";
        planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`k2` >= 0"));
    }
}
