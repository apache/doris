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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class KillQueryCommandTest extends TestWithFeService {
    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;

    private void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
        ConnectScheduler scheduler = new ConnectScheduler(10);
        connectContext.setQualifiedUser("root");
        new Expectations() {
            {
                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;

                scheduler.listConnection("root", anyBoolean);
                minTimes = 0;
                result = Lists.newArrayList(connectContext.toThreadInfo(false));
            }
        };
        connectContext.setConnectScheduler(scheduler);
    }

    @Test
    public void testKillQuery() throws Exception {
        runBefore();
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "select 1");
        stmtExecutor.execute();
        String queryId = DebugUtil.printId(stmtExecutor.getContext().queryId());
        KillQueryCommand command = new KillQueryCommand(queryId);
        Assertions.assertDoesNotThrow(() -> command.doRun(connectContext, stmtExecutor));
        Assertions.assertEquals(connectContext.getState().getStateType(), QueryState.MysqlStateType.OK);
    }
}
