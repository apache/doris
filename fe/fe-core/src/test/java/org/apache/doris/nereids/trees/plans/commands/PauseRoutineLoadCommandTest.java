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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.load.PauseRoutineLoadCommand;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PauseRoutineLoadCommandTest {

    @Test
    public void test() throws Exception {
        ConnectContext ctx = new ConnectContext();
        LabelNameInfo labelNameInfo = new LabelNameInfo("testDB", "label0");
        PauseRoutineLoadCommand command = new PauseRoutineLoadCommand(labelNameInfo);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));

        PauseRoutineLoadCommand command2 = new PauseRoutineLoadCommand();
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(ctx),
                "No database selected");

        ctx.setDatabase("testDB");
        PauseRoutineLoadCommand command3 = new PauseRoutineLoadCommand();
        Assertions.assertDoesNotThrow(() -> command3.validate(ctx));
    }
}
