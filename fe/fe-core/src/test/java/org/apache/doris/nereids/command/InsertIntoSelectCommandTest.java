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

package org.apache.doris.nereids.command;

import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class InsertIntoSelectCommandTest extends TestWithFeService {
    @Override
    public void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        createTables(
                "create table t1\n"
                        + "(k1 int, k2 int)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table t2\n"
                        + "(k1 int, k2 int)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');");
    }

    @Test
    public void testCommand() throws Exception {
        String sql = "insert into t2 select * from t1";
        connectContext.getSessionVariable().setEnableNereidsPlanner(true);
        connectContext.setQueryId(new TUniqueId(0x1234, 0x3467));
        StmtExecutor executor = new StmtExecutor(connectContext, sql);
        executor.execute(connectContext.queryId());
    }
}
