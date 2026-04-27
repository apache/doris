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

import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.load.StopRoutineLoadCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class StopRoutineLoadCommandTest {

    private ConnectContext connectContext;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() {
        connectContext = Mockito.mock(ConnectContext.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(connectContext);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connectContext.getState()).thenReturn(new QueryState());
    }

    @AfterEach
    public void tearDown() {
        if (ctxMockedStatic != null) {
            ctxMockedStatic.close();
        }
    }

    @Test
    public void test() throws Exception {
        LabelNameInfo labelNameInfo = new LabelNameInfo("testDB", "label0");
        StopRoutineLoadCommand command = new StopRoutineLoadCommand(labelNameInfo);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }
}
