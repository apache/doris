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
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowRoutineLoadTaskCommandTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_db");
    }

    @Test
    public void testValidate() {
        Expression where = new EqualTo(new UnboundSlot(Lists.newArrayList("test")),
                new StringLiteral("test"));
        ShowRoutineLoadTaskCommand command = new ShowRoutineLoadTaskCommand("test_db", where);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));

        Expression where2 = new EqualTo(new UnboundSlot(Lists.newArrayList("JobName")),
                new StringLiteral("1111"));
        ShowRoutineLoadTaskCommand command2 = new ShowRoutineLoadTaskCommand("test_db", where2);
        Assertions.assertDoesNotThrow(() -> command2.validate(connectContext));

        Expression where3 = new EqualTo(new UnboundSlot(Lists.newArrayList("TaskId")),
                new IntegerLiteral(1111));
        ShowRoutineLoadTaskCommand command3 = new ShowRoutineLoadTaskCommand("test_db", where3);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext));

        //test whereClause is null
        ShowRoutineLoadTaskCommand command4 = new ShowRoutineLoadTaskCommand("test_db", null);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext));
    }
}
