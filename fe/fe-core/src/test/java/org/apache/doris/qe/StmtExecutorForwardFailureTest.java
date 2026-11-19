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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.SetOptionsCommand;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

public class StmtExecutorForwardFailureTest extends TestWithFeService {
    @Test
    public void testFailedForwardDoesNotModifyReadOnlyVariable() throws Exception {
        String originalVersionComment = GlobalVariable.versionComment;
        String sql = "set global version_comment = 'rejected_forward'";
        StatementBase statement = new NereidsParser().parseSQL(sql).get(0);
        statement.setOrigStmt(new OriginStatement(sql, 0));
        StmtExecutor executor = new StmtExecutor(connectContext, statement);
        SetOptionsCommand command = (SetOptionsCommand) ((LogicalPlanAdapter) statement).getLogicalPlan();
        DdlException rejected = Assertions.assertThrows(DdlException.class,
                () -> command.run(connectContext, executor));
        Assertions.assertTrue(rejected.getMessage().contains("read only"));
        ByteBuffer errorPacket = ByteBuffer.wrap(new byte[] {(byte) 0xff, 1, 2});

        try (MockedConstruction<MasterOpExecutor> masters = Mockito.mockConstruction(MasterOpExecutor.class,
                (master, construction) -> {
                    Mockito.when(master.getProxyStatus()).thenReturn(QueryState.MysqlStateType.ERR.name());
                    Mockito.when(master.getErrMsg()).thenReturn(rejected.getMessage());
                    Mockito.when(master.getOutputPacket()).thenReturn(errorPacket);
                })) {
            Deencapsulation.invoke(executor, "forwardToMaster");

            Assertions.assertEquals(1, masters.constructed().size());
            Assertions.assertEquals(originalVersionComment, GlobalVariable.versionComment);
            Assertions.assertEquals(QueryState.MysqlStateType.ERR.name(), executor.getProxyStatus());
            Assertions.assertEquals(rejected.getMessage(), executor.getProxyErrMsg());
            Assertions.assertSame(errorPacket, executor.getOutputPacket());
        } finally {
            GlobalVariable.versionComment = originalVersionComment;
        }
    }

    @Test
    public void testSuccessfulForwardUpdatesSessionVariable() throws Exception {
        int originalTimeout = connectContext.getSessionVariable().getQueryTimeoutS();
        String sql = "set global query_timeout = 123";
        StatementBase statement = new NereidsParser().parseSQL(sql).get(0);
        statement.setOrigStmt(new OriginStatement(sql, 0));
        StmtExecutor executor = new StmtExecutor(connectContext, statement);

        try (MockedConstruction<MasterOpExecutor> masters = Mockito.mockConstruction(MasterOpExecutor.class,
                (master, construction) -> Mockito.when(master.getProxyStatus())
                        .thenReturn(QueryState.MysqlStateType.OK.name()))) {
            Deencapsulation.invoke(executor, "forwardToMaster");

            Assertions.assertEquals(1, masters.constructed().size());
            Assertions.assertEquals(123, connectContext.getSessionVariable().getQueryTimeoutS());
            Assertions.assertEquals(QueryState.MysqlStateType.OK.name(), executor.getProxyStatus());
        } finally {
            connectContext.getSessionVariable().setQueryTimeoutS(originalTimeout);
        }
    }
}
