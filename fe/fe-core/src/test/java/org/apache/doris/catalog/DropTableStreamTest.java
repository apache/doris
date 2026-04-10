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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.DropStreamCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class DropTableStreamTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;

        createDatabase("test_stream");
        String createTableStr1 = "create table if not exists test_stream.tbl1\n" + "(k1 int, k2 int)\n" + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1'); ";
        createTable(createTableStr1);

        String createStreamStr1 =  "create stream test_stream.s1 on table test_stream.tbl1\n"
                + "properties('type' = 'default', 'show_initial_rows' = 'true'); ";
        createTable(createStreamStr1);
        String createStreamStr2 =  "create stream test_stream.s2 on table test_stream.tbl1\n"
                + "properties('type' = 'append_only', 'show_initial_rows' = 'true'); ";
        createTable(createStreamStr2);
    }

    private void dropStream(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof DropStreamCommand) {
            ((DropStreamCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    @Test
    public void testNormalDropStream() throws Exception {
        // test drop
        ExceptionChecker
                .expectThrowsNoException(() ->
                        dropStream("drop stream test_stream.s1;"));
        // test force drop
        ExceptionChecker
                .expectThrowsNoException(() ->
                        dropStream("drop stream test_stream.s2 force;"));

        // test if exist
        ExceptionChecker
                .expectThrowsNoException(() ->
                        dropStream("drop stream if exists test_stream.s3;"));
    }

    @Test
    public void testAbnormalDropStream() throws Exception {
        // test not exist
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Unknown table 's3' in test_stream",
                () -> dropStream("drop stream test_stream.s3;"));
    }

    @Override
    protected void runAfterAll() throws Exception {
        dropDatabase("test_stream");
    }
}
