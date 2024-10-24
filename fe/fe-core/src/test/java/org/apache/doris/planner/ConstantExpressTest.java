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

package org.apache.doris.planner;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class ConstantExpressTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/ConstantExpressTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.startFEServer(runningDir);
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.getSessionVariable().setEnableFoldConstantByBe(false);
    }

    private static void testConstantExpressResult(String sql, String result) throws Exception {
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("constant exprs: \n         " + result));
    }

    @Test
    public void testDate() throws Exception {
        testConstantExpressResult(
                "select date_format('2020-02-19 16:01:12','%H%i');",
                "'1601'");

        testConstantExpressResult(
                "select /*+ SET_VAR(enable_nereids_planner=false) */ date_format('2020-02-19 16:01:12','%Y%m%d');",
                "'20200219'");

        testConstantExpressResult(
                "select /*+ SET_VAR(enable_nereids_planner=false) */ date_format(date_sub('2018-07-24 07:16:19',1),'yyyyMMdd');",
                "'20180723'");

        testConstantExpressResult(
                "select year('2018-07-24')*12 + month('2018-07-24');",
                "24223");

        testConstantExpressResult(
                "select /*+ SET_VAR(enable_nereids_planner=false) */ date_format('2018-08-08 07:16:19', 'yyyyMMdd');",
                "'20180808'");

        testConstantExpressResult(
                "select /*+ SET_VAR(enable_nereids_planner=false) */ date_format('2018-08-08 07:16:19', 'yyyy-MM-dd HH:mm:ss');",
                "'2018-08-08 07:16:19'");

        testConstantExpressResult(
                "select datediff('2018-08-08','1970-01-01');",
                "17751");

        testConstantExpressResult(
                "select date_add('2018-08-08', 1);",
                "'2018-08-09'");

        testConstantExpressResult(
                "select date_add('2018-08-08', -1);",
                "'2018-08-07'");

        testConstantExpressResult(
                "select date_sub('2018-08-08 07:16:19',1);",
                "'2018-08-07 07:16:19'");

        testConstantExpressResult(
                "select year('2018-07-24');",
                "2018");

        testConstantExpressResult(
                "select month('2018-07-24');",
                "7");

        testConstantExpressResult(
                "select day('2018-07-24');",
                "24");

        testConstantExpressResult(
                "select UNIX_TIMESTAMP(\"1970-01-01 08:00:01\");",
                "1");

        testConstantExpressResult(
                "select now();",
                "");

        testConstantExpressResult(
                "select curdate();",
                "");

        testConstantExpressResult(
                "select current_timestamp();",
                "");

        testConstantExpressResult(
                "select curtime();",
                "");

        testConstantExpressResult(
                "select current_time();",
                "");

        testConstantExpressResult(
                "select current_date();",
                "");
    }

    @Test
    public void testCast() throws Exception {
        testConstantExpressResult(
                "select cast ('1' as int) ;",
                "1");

        testConstantExpressResult(
                "select cast ('2020-01-20' as date);",
                "'2020-01-20'");

        testConstantExpressResult(
                "select cast ('2020-01-20 00:00:00' as datetime);",
                "'2020-01-20 00:00:00'");

        testConstantExpressResult(
                "select cast ('2020-01-20 00:00:00' as datetime(0));",
                "'2020-01-20 00:00:00'");
    }

    @Test
    public void testArithmetic() throws Exception {
        testConstantExpressResult(
                "select 1 + 10;",
                "11");

        testConstantExpressResult(
                "select 1 - 10;",
                "-9");

        testConstantExpressResult(
                "select 1 * 10.0;",
                "10");
    }

    @Test
    public void testPredicate() throws Exception {
        testConstantExpressResult(
                "select 1 > 2",
                "FALSE");

        testConstantExpressResult(
                "select 1 = 1",
                "TRUE");
    }

    @Test
    public void testConstantInPredicate() throws Exception {
        connectContext.setDatabase("test");
        // for constant NOT IN PREDICATE
        String sql = "select 1 not in (1, 2);";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("FALSE"));

        sql = "select 1 not in (2, 3);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("TRUE"));

        sql = "select 1 not in (2, null);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("NULL"));

        sql = "select 1 not in (1, 2, null);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("FALSE"));

        sql = "select null not in (1, 2);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("NULL"));

        sql = "select null not in (null);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("NULL"));

        // for constant IN PREDICATE
        sql = "select 1 in (1, 2);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("TRUE"));

        sql = "select 1 in (2, 3);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("FALSE"));

        sql = "select 1 in (1, 2, NULL);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("TRUE"));

        sql = "select 1 in (2, NULL);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("NULL"));

        sql = "select null in (2);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("NULL"));

        sql = "select null in (null);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("NULL"));
    }

    @Test
    public void testTimestamp() throws Exception {
        testConstantExpressResult("select timestamp('2021-07-24 00:00:00')", "'2021-07-24 00:00:00'");
    }
}
