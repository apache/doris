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

import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReanalyzeTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.tbl1\n" + "(k1 int, k2 int, v1 int)\n" + "distributed by hash(k1)\n"
                + "properties(\"replication_num\" = \"1\");");
    }

    @Test
    public void testTurnoffVectorizedEngineWhenCannotChangeSlotToNullable() throws Exception {
        String sql = "explain select * from test.tbl1 t1"
                + " where (select count(*) from test.tbl1 t2 where t1.k1 = t2.k1) > 0";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        connectContext.setExecutor(stmtExecutor);
        stmtExecutor.execute();
        connectContext.setExecutor(null);
        Assertions.assertEquals(MysqlStateType.EOF, connectContext.getState().getStateType());
    }
}
