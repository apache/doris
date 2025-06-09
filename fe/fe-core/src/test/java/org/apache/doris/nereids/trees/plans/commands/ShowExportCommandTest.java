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
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowExportCommandTest extends TestWithFeService {
    @Test
    void testValidate() throws UserException {
        ShowExportCommand se = new ShowExportCommand("internal", "test", null, null, -1);
        se.validate(connectContext);

        se = new ShowExportCommand("", "test", null, null, -1);
        se.validate(connectContext);

        se = new ShowExportCommand("", "test", null, null, -1);
        se.validate(connectContext);

        Expression where1 = new EqualTo(new UnboundSlot(Lists.newArrayList("LABEL")),
                new StringLiteral("xxx"));
        se = new ShowExportCommand("", "test", where1, null, -1);
        se.validate(connectContext);

        Expression where2 = new EqualTo(new UnboundSlot(Lists.newArrayList("id")),
                new IntegerLiteral(123));
        se = new ShowExportCommand("", "test", where2, null, -1);
        se.validate(connectContext);

        Expression where3 = new EqualTo(new UnboundSlot(Lists.newArrayList("state")),
                new StringLiteral("FINISHED"));
        se = new ShowExportCommand("", "test", where3, null, -1);
        se.validate(connectContext);

        Expression where4 = new Like(new UnboundSlot(Lists.newArrayList("LABEL")),
                new StringLiteral("xx%"));
        se = new ShowExportCommand("", "test", where4, null, -1);
        se.validate(connectContext);

        Expression where5 = new EqualTo(new UnboundSlot(Lists.newArrayList("state1")),
                new StringLiteral("xxx"));
        se = new ShowExportCommand("", "test", where5, null, -1);
        ShowExportCommand finalSe1 = se;
        Assertions.assertThrows(AnalysisException.class, () -> finalSe1.validate(connectContext));

        Expression where6 = new Like(new UnboundSlot(Lists.newArrayList("id")),
                new StringLiteral("xxx"));
        se = new ShowExportCommand("", "test", where6, null, -1);
        ShowExportCommand finalSe2 = se;
        Assertions.assertThrows(AnalysisException.class, () -> finalSe2.validate(connectContext));

        Expression where7 = new EqualTo(new UnboundSlot(Lists.newArrayList("id")),
                new StringLiteral("xxx"));
        se = new ShowExportCommand("", "test", where7, null, -1);
        ShowExportCommand finalSe3 = se;
        Assertions.assertThrows(AnalysisException.class, () -> finalSe3.validate(connectContext));

        Expression equalTo1 = new EqualTo(new UnboundSlot(Lists.newArrayList("STATE")),
                new StringLiteral("PENDING"));
        Expression equalTo2 = new EqualTo(new UnboundSlot(Lists.newArrayList("LABEL")),
                new StringLiteral("xxx"));
        Expression where8 = new And(equalTo1, equalTo2);
        se = new ShowExportCommand("", "test", where8, null, -1);
        ShowExportCommand finalSe4 = se;
        Assertions.assertThrows(AnalysisException.class, () -> finalSe4.validate(connectContext));

        Expression where9 = new EqualTo(new UnboundSlot(Lists.newArrayList("state")),
                new StringLiteral("FINISHED"));
        se = new ShowExportCommand("", "", where9, null, -1);
        ShowExportCommand finalSe5 = se;
        Assertions.assertThrows(AnalysisException.class, () -> finalSe5.validate(connectContext));

        Expression where10 = new EqualTo(new UnboundSlot(Lists.newArrayList("state")),
                new StringLiteral("xxx"));
        se = new ShowExportCommand("", "test", where10, null, -1);
        ShowExportCommand finalSe6 = se;
        Assertions.assertThrows(AnalysisException.class, () -> finalSe6.validate(connectContext));

        Expression where11 = new EqualTo(new UnboundSlot(Lists.newArrayList("JobId")),
                new IntegerLiteral(123));
        se = new ShowExportCommand("", "test", where11, null, -1);
        se.validate(connectContext);
    }
}
