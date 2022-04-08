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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class AggregateTest {

    private static String baseDir = "fe";
    private static String runningDir = baseDir + "/mocked/AggregateTest/"
            + UUID.randomUUID().toString() + "/";
    private static final String TABLE_NAME = "table1";
    private static final String DB_NAME = "db1";
    private static DorisAssert dorisAssert;

    @BeforeClass
    public static void beforeClass() throws Exception{
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createDorisCluster(runningDir);
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        String createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME + " (empid int, name varchar, " +
                "deptno int, salary int, commission int, time DATETIME) "
                + "distributed by hash(empid) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
    }

    /**
     * ISSUE-3492
     */
    @Test
    public void testCountDisintctAnalysisException() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();

        // NOT support mix distinct, one DistinctAggregationFunction has one column, the other DistinctAggregationFunction has some columns.
        do {
            String query = "select count(distinct empid), count(distinct salary), count(distinct empid, salary) from " + DB_NAME + "." + TABLE_NAME;
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("The query contains multi count distinct or sum distinct, each can't have multi columns."));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);

        // support multi DistinctAggregationFunction, but each DistinctAggregationFunction only has one column
        do {
            String query = "select count(distinct empid), count(distinct salary) from " + DB_NAME + "." + TABLE_NAME;
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (Exception e) {
                Assert.fail("should be query, no exception");
            }
        } while (false);

        // support 1 DistinctAggregationFunction with some columns
        do {
            String query = "select count(distinct salary, empid) from " + DB_NAME + "." + TABLE_NAME;
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (Exception e) {
                Assert.fail("should be query, no exception");
            }
        } while (false);
    }

    @Test
    public void testWindowFunnelAnalysisException() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();

        // normal.
        {
            String query = "select empid, window_funnel(1, 'default', time, empid = 1, empid = 2) from " +
                           DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
        }

        // less argument.
        do {
            String query = "select empid, window_funnel(1, 'default', time) from " +
                           DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("function must have at least four params"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while(false);

        // argument with wrong type.
        do {
            String query = "select empid, window_funnel('xx', 'default', time, empid = 1) from " +
                           DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("The window params of window_funnel function must be integer"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while(false);

        do {
            String query = "select empid, window_funnel(1, 1, time, empid = 1) from " +
                           DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("The mode params of window_funnel function must be integer"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while(false);


        do {
            String query = "select empid, window_funnel(1, '1', empid, '1') from " +
                           DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("The 3rd param of window_funnel function must be DATE or DATETIME"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while(false);

        do {
            String query = "select empid, window_funnel(1, '1', time, '1') from " +
                           DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("The 4th and subsequent params of window_funnel function must be boolean"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while(false);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UtFrameUtils.cleanDorisFeDir(baseDir);
    }
}
