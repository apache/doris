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

import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class StmtRewriterTest {

    private static String baseDir = "fe";
    private static String runningDir = baseDir + "/mocked/StmtRewriterTest/"
            + UUID.randomUUID().toString() + "/";
    private static final String TABLE_NAME = "table1";
    private static final String DB_NAME = "db1";
    private static DorisAssert dorisAssert;

    @BeforeClass
    public static void beforeClass() throws Exception{
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinDorisCluster(runningDir);
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        String createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME + " (empid int, name varchar, " +
                "deptno int, salary int, commission int) "
                + "distributed by hash(empid) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
    }

    @Test
    public void testRewriteHavingClauseSubqueries() throws Exception {
        String subquery = "select avg(salary) from " + TABLE_NAME;
        String query = "select empid, sum(salary) from " + TABLE_NAME + " group by empid having sum(salary) > (" +
                subquery + ");";
        dorisAssert.query(query).explainContains("CROSS JOIN", "predicates: <slot 3> > <slot 8>");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UtFrameUtils.cleanDorisFeDir(baseDir);
    }
}
