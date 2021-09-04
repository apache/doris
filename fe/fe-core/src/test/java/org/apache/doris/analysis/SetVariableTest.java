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

import org.apache.doris.catalog.Type;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class SetVariableTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/QueryPlanTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testSqlMode() throws Exception {
        String setStr = "set sql_mode = concat(@@sql_mode, 'STRICT_TRANS_TABLES');";
        connectContext.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, setStr);
        stmtExecutor.execute();
        Assert.assertEquals("STRICT_TRANS_TABLES",
                SqlModeHelper.decode(connectContext.getSessionVariable().getSqlMode()));

        String selectStr = "explain select @@sql_mode;";
        connectContext.getState().reset();
        stmtExecutor = new StmtExecutor(connectContext, selectStr);
        stmtExecutor.execute();
        Expr expr = stmtExecutor.getParsedStmt().getResultExprs().get(0);
        Assert.assertTrue(expr instanceof SlotRef);
        Assert.assertTrue(expr.getType() == Type.BIGINT);
    }

    @Test
    public void testExecMemLimit() throws Exception {
        String setStr = "set exec_mem_limit = @@exec_mem_limit * 10";
        connectContext.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, setStr);
        stmtExecutor.execute();
        Assert.assertEquals(21474836480L, connectContext.getSessionVariable().getMaxExecMemByte());
    }
}