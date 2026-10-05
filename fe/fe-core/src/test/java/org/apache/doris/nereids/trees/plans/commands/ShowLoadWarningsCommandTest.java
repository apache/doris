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
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowLoadWarningsCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    @Test
    void testHandleShowLoadWarnings() throws Exception {
        Expression where1 = new EqualTo(new UnboundSlot(Lists.newArrayList("label")), new StringLiteral("xxx"));
        ShowLoadWarningsCommand sl = new ShowLoadWarningsCommand("test", where1, -1, 0, null);
        ShowLoadWarningsCommand finalSl1 = sl;
        Assertions.assertThrows(AnalysisException.class, () -> finalSl1.handleShowLoadWarnings(connectContext, null));

        Expression where2 = new EqualTo(new UnboundSlot(Lists.newArrayList("load_job_id")),
                new BigIntLiteral(1748399001963L));
        sl = new ShowLoadWarningsCommand("test", where2, -1, 0, null);
        ShowLoadWarningsCommand finalSl2 = sl;
        Assertions.assertThrows(AnalysisException.class, () -> finalSl2.handleShowLoadWarnings(connectContext, null));

        Expression where3 = new And(where1, where2);
        sl = new ShowLoadWarningsCommand("test", where3, -1, 0, null);
        ShowLoadWarningsCommand finalSl3 = sl;
        Assertions.assertThrows(AnalysisException.class, () -> finalSl3.handleShowLoadWarnings(connectContext, null));

        Expression where4 = new EqualTo(new UnboundSlot(Lists.newArrayList("abc")),
                new BigIntLiteral(1748399001963L));
        sl = new ShowLoadWarningsCommand("test", where4, -1, 0, null);
        ShowLoadWarningsCommand finalSl4 = sl;
        Assertions.assertThrows(AnalysisException.class, () -> finalSl4.handleShowLoadWarnings(connectContext, null));

        Expression where5 = new EqualTo(new UnboundSlot(Lists.newArrayList("jobid")),
                new BigIntLiteral(1748399001963L));
        sl = new ShowLoadWarningsCommand("test", where5, -1, 0, null);
        ShowLoadWarningsCommand finalSl5 = sl;
        Assertions.assertThrows(AnalysisException.class, () -> finalSl5.handleShowLoadWarnings(connectContext, null));
    }

    @Test
    void testValidate() throws Exception {
        Expression where = new EqualTo(new UnboundSlot(Lists.newArrayList("jobid")),
                new BigIntLiteral(1748399001963L));
        ShowLoadWarningsCommand sl = new ShowLoadWarningsCommand("test", where, -1, 0, null);
        sl.validate(connectContext);

        Expression where1 = new EqualTo(new UnboundSlot(Lists.newArrayList("load_job_id")),
                new BigIntLiteral(1748399001963L));
        sl = new ShowLoadWarningsCommand("test", where1, -1, 0, null);
        sl.validate(connectContext);
    }
}
