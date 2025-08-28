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
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowWarmUpCommandTest {

    @Test
    public void testValidate() {
        //test whereClause ia null
        ShowWarmUpCommand command = new ShowWarmUpCommand(null);
        Assertions.assertDoesNotThrow(() -> command.validate());
    }

    @Test
    public void testValidate2() {
        //test whereClause is equalTo
        Expression where = new EqualTo(new UnboundSlot(Lists.newArrayList("test")),
                new StringLiteral("test"));
        ShowWarmUpCommand command = new ShowWarmUpCommand(where);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate());
    }

    @Test
    public void testValidate3() {
        //test whereClause is equalTo
        Expression where = new EqualTo(new UnboundSlot(Lists.newArrayList("id")),
                new IntegerLiteral(111));
        ShowWarmUpCommand command = new ShowWarmUpCommand(where);
        Assertions.assertDoesNotThrow(() -> command.validate());
    }

    @Test
    public void testValidate4() {
        //test whereClause is equalTo BigIntLiteral
        Expression where = new EqualTo(new UnboundSlot(Lists.newArrayList("id")),
                new BigIntLiteral(1754626195722L));
        ShowWarmUpCommand command = new ShowWarmUpCommand(where);
        Assertions.assertDoesNotThrow(() -> command.validate());
    }
}
