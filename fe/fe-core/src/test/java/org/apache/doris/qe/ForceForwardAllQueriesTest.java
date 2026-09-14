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

package org.apache.doris.qe;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class ForceForwardAllQueriesTest extends TestWithFeService {

    @Test
    public void testSessionForceForwardAllQueries() throws Exception {
        Config.force_forward_all_queries = false;
        Config.enable_bdbje_debug_mode = false;

        Env env = Env.getCurrentEnv();
        FrontendNodeType originalFeType = env.getFeType();
        AtomicBoolean canRead = Deencapsulation.getField(env, "canRead");
        Deencapsulation.setField(env, "feType", FrontendNodeType.FOLLOWER);
        canRead.set(true);
        try {
            ConnectContext ctx = createDefaultCtx();
            ctx.setThreadLocalInfo();
            StatementBase parsedStmt = analyzeAndGetStmtByNereids("select 1", ctx);
            StmtExecutor executor = new StmtExecutor(ctx, parsedStmt);

            // neither session variable nor config enabled -> not forwarded
            ctx.getSessionVariable().forceForwardAllQueries = false;
            Config.force_forward_all_queries = false;
            boolean forward = Deencapsulation.invoke(executor, "shouldForwardToMaster");
            Assert.assertFalse(forward);

            // session variable enabled -> forwarded
            ctx.getSessionVariable().forceForwardAllQueries = true;
            forward = Deencapsulation.invoke(executor, "shouldForwardToMaster");
            Assert.assertTrue(forward);

            // session variable disabled but config enabled -> forwarded
            ctx.getSessionVariable().forceForwardAllQueries = false;
            Config.force_forward_all_queries = true;
            forward = Deencapsulation.invoke(executor, "shouldForwardToMaster");
            Assert.assertTrue(forward);

            // both disabled -> not forwarded
            Config.force_forward_all_queries = false;
            forward = Deencapsulation.invoke(executor, "shouldForwardToMaster");
            Assert.assertFalse(forward);
        } finally {
            Deencapsulation.setField(env, "feType", originalFeType);
            canRead.set(false);
            Config.force_forward_all_queries = false;
        }
    }
}
