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
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowTransactionCommandTest {
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private ConnectContext connectContext;

    private void runBefore() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkDbPriv(connectContext, anyString, anyString, PrivPredicate.LOAD);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidate() {
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

        Expression equalTo4 = new EqualTo(new UnboundSlot(Lists.newArrayList("STATUS")), new StringLiteral("unknow"));
        ShowTransactionCommand command5 = new ShowTransactionCommand("test", equalTo4);
        Assertions.assertThrows(AnalysisException.class, () -> command5.validate(connectContext));
    }
}
