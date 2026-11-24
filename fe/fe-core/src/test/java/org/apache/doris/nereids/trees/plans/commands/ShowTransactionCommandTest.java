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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class ShowTransactionCommandTest extends TestWithFeService {
    private Env env;
    private AccessControllerManager accessControllerManager;
    private ConnectContext connectContext;

    private void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
        connectContext.setSkipAuth(true);
        if (!Mockito.mockingDetails(accessControllerManager).isSpy()) {
            AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
            Mockito.doReturn(true).when(spyAcm).checkDbPriv(
                    Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.any(PrivPredicate.class));
            Deencapsulation.setField(env, "accessManager", spyAcm);
        }
    }

    @Test
    public void testValidate() throws Exception {
        runBefore();
        // test where is null
        ShowTransactionCommand command = new ShowTransactionCommand("test", null);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));

        Expression equalTo = new EqualTo(new UnboundSlot(Lists.newArrayList("ID")), new IntegerLiteral(4005));
        ShowTransactionCommand command2 = new ShowTransactionCommand("test", equalTo);
        Assertions.assertDoesNotThrow(() -> command2.validate(connectContext));

        Expression equalTo2 = new EqualTo(new UnboundSlot(Lists.newArrayList("LABEL")), new StringLiteral("label_name"));
        ShowTransactionCommand command3 = new ShowTransactionCommand("test", equalTo2);
        Assertions.assertDoesNotThrow(() -> command3.validate(connectContext));

        Expression equalTo3 = new EqualTo(new UnboundSlot(Lists.newArrayList("STATUS")), new StringLiteral("prepare"));
        ShowTransactionCommand command4 = new ShowTransactionCommand("test", equalTo3);
        Assertions.assertDoesNotThrow(() -> command4.validate(connectContext));
        Assertions.assertEquals(TransactionStatus.PREPARE, Deencapsulation.getField(command4, "status"));

        Expression equalTo4 = new EqualTo(new UnboundSlot(Lists.newArrayList("STATUS")), new StringLiteral("unknow"));
        ShowTransactionCommand command5 = new ShowTransactionCommand("test", equalTo4);
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> command5.validate(connectContext));
        Assertions.assertTrue(ex.getMessage().contains("status should be"));

        Expression equalToVarchar = new EqualTo(new UnboundSlot(Lists.newArrayList("STATUS")),
                new VarcharLiteral("visible"));
        ShowTransactionCommand command6 = new ShowTransactionCommand("test", equalToVarchar);
        Assertions.assertDoesNotThrow(() -> command6.validate(connectContext));
        Assertions.assertEquals(TransactionStatus.VISIBLE, Deencapsulation.getField(command6, "status"));
    }

    @Test
    public void testValidateParsedStatus() throws Exception {
        runBefore();
        NereidsParser parser = new NereidsParser();

        assertParsedStatus(parser, "SHOW TRANSACTION FROM test WHERE STATUS = 'visible'",
                TransactionStatus.VISIBLE);
        assertParsedStatus(parser, "SHOW TRANSACTION FROM test WHERE STATUS = 'VISIBLE'",
                TransactionStatus.VISIBLE);
        assertParsedStatus(parser, "SHOW TRANSACTION FROM test WHERE STATUS = 'PREPARE'",
                TransactionStatus.PREPARE);
        assertParsedStatus(parser, "SHOW TRANSACTION FROM test WHERE STATUS = 'precommitted'",
                TransactionStatus.PRECOMMITTED);
        assertParsedStatus(parser, "SHOW TRANSACTION FROM test WHERE STATUS = 'committed'",
                TransactionStatus.COMMITTED);
        assertParsedStatus(parser, "SHOW TRANSACTION FROM test WHERE STATUS = 'aborted'",
                TransactionStatus.ABORTED);

        for (String invalidStatus : Lists.newArrayList("unknow", "unknown")) {
            String sql = "SHOW TRANSACTION FROM test WHERE STATUS = '" + invalidStatus + "'";
            LogicalPlan plan = parser.parseSingle(sql);
            Assertions.assertInstanceOf(ShowTransactionCommand.class, plan);
            ShowTransactionCommand command = (ShowTransactionCommand) plan;
            AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                    () -> command.validate(connectContext));
            Assertions.assertTrue(ex.getMessage().contains("status should be"), ex.getMessage());
        }
    }

    private void assertParsedStatus(NereidsParser parser, String sql, TransactionStatus expectedStatus) {
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(ShowTransactionCommand.class, plan);
        ShowTransactionCommand command = (ShowTransactionCommand) plan;
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
        Assertions.assertEquals(expectedStatus, Deencapsulation.getField(command, "status"));
    }
}
