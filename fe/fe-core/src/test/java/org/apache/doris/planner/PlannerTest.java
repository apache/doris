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

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PlannerTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("db1");
        useDatabase("db1");
        createTable("create table db1.tbl1(k1 int) duplicate key(k1) distributed by hash(k1) buckets 1 "
                + "properties('replication_num' = '1');");
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
    }

    @Test
    public void testExplainStringNormal() throws Exception {
        String sql = "explain select k1 from db1.tbl1 limit 1";
        StmtExecutor executor = new StmtExecutor(connectContext, sql);
        executor.execute();
        Planner planner = executor.planner();
        String plan = planner.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertNotNull(plan);
    }

    @Test
    public void testExplainStringTreeAndGraph() throws Exception {
        String sql = "explain select k1 from db1.tbl1 limit 1";
        StmtExecutor executor = new StmtExecutor(connectContext, sql);
        executor.execute();
        Planner planner = executor.planner();
        Assertions.assertNotNull(planner.getExplainString(new ExplainOptions(false, true, false)));
        Assertions.assertNotNull(planner.getExplainString(new ExplainOptions(false, false, true)));
    }
}

