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

package org.apache.doris.nereids.preprocess;

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SelectHintTest {

    @BeforeAll
    public static void init() {
        ConnectContext ctx = new ConnectContext();
        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return ctx;
            }
        };
    }

    @Test
    public void testFallbackToOriginalPlanner() throws Exception {
        String sql = " SELECT /*+ SET_VAR(enable_nereids_planner=\"false\") */ 1";

        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        StatementContext statementContext = new StatementContext(ctx, new OriginStatement(sql, 0));
        SessionVariable sv = ctx.getSessionVariable();
        Assertions.assertNotNull(sv);
        sv.setEnableNereidsPlanner(true);
        sv.enableFallbackToOriginalPlanner = false;
        Assertions.assertThrows(AnalysisException.class, () -> new NereidsPlanner(statementContext)
                .plan(new NereidsParser().parseSingle(sql), PhysicalProperties.ANY));

        // manually recover sv
        sv.setEnableNereidsPlanner(true);
        sv.enableFallbackToOriginalPlanner = false;
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);

        new Expectations(stmtExecutor) {
            {
                stmtExecutor.executeByLegacy((TUniqueId) any);
            }
        };

        stmtExecutor.execute();

        Assertions.assertTrue(sv.isEnableNereidsPlanner());
        Assertions.assertFalse(sv.enableFallbackToOriginalPlanner);
    }
}
