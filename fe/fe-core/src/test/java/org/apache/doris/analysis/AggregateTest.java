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
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class AggregateTest extends TestWithFeService {
    private static final String TABLE_NAME = "table1";
    private static final String DB_NAME = "db1";
    private static DorisAssert dorisAssert;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        String createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME + " (empid int, name varchar, "
                + "deptno int, salary int, commission int, time_col DATETIME, timev2_col "
                + " DATETIME(3)) distributed by hash(empid) buckets 3"
                + " properties('replication_num' = '1');";
        createTable(createTableSQL);
    }

    /**
     * ISSUE-3492
     */
    @Test
    public void testCountDisintctAnalysisException() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();

        // NOT support mix distinct, one DistinctAggregationFunction has one column, the other DistinctAggregationFunction has some columns.
        do {
            String query = "select count(distinct empid), count(distinct salary), "
                    + "count(distinct empid, salary) from " + DB_NAME + "." + TABLE_NAME;
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains(
                        "The query contains multi count distinct or sum distinct, each can't have multi columns."));
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
    public void testRetentionAnalysisException() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();

        // normal.
        do {
            String query = "select empid, retention(empid = 1, empid = 2) from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
        } while (false);

        do {
            String query = "select empid, retention(empid = 1, empid = 2, empid = 3, empid = 4) from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
        } while (false);

        // less argument.
        do {
            String query = "select empid, retention() from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("function must have at least one param"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);

        // argument with wrong type.
        do {
            String query = "select empid, retention('xx', empid = 1) from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("All params of retention function must be boolean"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);

        do {
            String query = "select empid, retention(1) from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("All params of retention function must be boolean"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);

    }

    @Test
    public void testWindowFunnelAnalysisException() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();

        // normal.
        do {
            String query = "select empid, window_funnel(1, 'default', time_col, empid = 1, empid = 2) from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
        } while (false);

        do {
            String query = "select empid, window_funnel(1, 'default', timev2_col, empid = 1, empid = 2) from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
        } while (false);

        // less argument.
        do {
            String query = "select empid, window_funnel(1, 'default', time_col) from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("function must have at least four params"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);

        do {
            String query = "select empid, window_funnel(1, 'default', timev2_col) from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("function must have at least four params"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);

        // argument with wrong type.
        do {
            String query = "select empid, window_funnel('xx', 'default', time_col, empid = 1) from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(
                        e.getMessage().contains("The window params of window_funnel function must be integer"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);

        do {
            String query = "select empid, window_funnel('xx', 'default', timev2_col, empid = 1) from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(
                        e.getMessage().contains("The window params of window_funnel function must be integer"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);

        do {
            String query = "select empid, window_funnel(1, 1, time_col, empid = 1) from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(
                        e.getMessage().contains("The mode params of window_funnel function must be integer"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);

        do {
            String query = "select empid, window_funnel(1, 1, timev2_col, empid = 1) from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(
                        e.getMessage().contains("The mode params of window_funnel function must be integer"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);

        do {
            String query = "select empid, window_funnel(1, '1', empid, '1') from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(
                        e.getMessage().contains("The 3rd param of window_funnel function must be DATE or DATETIME"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);

        do {
            String query = "select empid, window_funnel(1, '1', time_col, '1') from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains(
                        "The 4th and subsequent params of window_funnel function must be boolean"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);

        do {
            String query = "select empid, window_funnel(1, '1', timev2_col, '1') from "
                    + DB_NAME + "." + TABLE_NAME + " group by empid";
            try {
                UtFrameUtils.parseAndAnalyzeStmt(query, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains(
                        "The 4th and subsequent params of window_funnel function must be boolean"));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);
    }
}
