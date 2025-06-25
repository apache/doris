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

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ShowBuildIndexCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    @Test
    void testHandleShowBuildIndex() throws Exception {
        // test where is null but db is not null
        ShowBuildIndexCommand sa = new ShowBuildIndexCommand("test", null, null, -1, -1);
        sa.handleShowBuildIndex(connectContext, null);

        // different limit and offset
        sa = new ShowBuildIndexCommand("test", null, null, 1, 0);
        sa.handleShowBuildIndex(connectContext, null);
        sa = new ShowBuildIndexCommand("test", null, null, 2, 1);
        sa.handleShowBuildIndex(connectContext, null);

        // order by
        UnboundSlot key = new UnboundSlot(Lists.newArrayList("JobId"));
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        sa = new ShowBuildIndexCommand("test", null, orderKeys, 1, 0);
        sa.handleShowBuildIndex(connectContext, null);

        Expression where1 = new EqualTo(new UnboundSlot(Lists.newArrayList("TableName")),
                new StringLiteral("xxx"));
        sa = new ShowBuildIndexCommand("test", where1, null, 1, 0);
        sa.handleShowBuildIndex(connectContext, null);

        Expression where2 = new Not(where1);
        sa = new ShowBuildIndexCommand("test", where2, null, 1, 0);
        sa.handleShowBuildIndex(connectContext, null);

        Expression where3 = new EqualTo(new UnboundSlot(Lists.newArrayList("State")),
                new StringLiteral("FINISHED"));
        sa = new ShowBuildIndexCommand("test", where3, null, 1, 0);
        sa.handleShowBuildIndex(connectContext, null);

        Expression where4 = new EqualTo(new UnboundSlot(Lists.newArrayList("CreateTime")),
                new StringLiteral("2025-06-04 21:53:53"));
        sa = new ShowBuildIndexCommand("test", where4, null, 1, 0);
        sa.handleShowBuildIndex(connectContext, null);

        Expression where5 = new EqualTo(new UnboundSlot(Lists.newArrayList("FinishTime")),
                new StringLiteral("2025-06-04 21:53:54"));
        sa = new ShowBuildIndexCommand("test", where5, null, 1, 0);
        sa.handleShowBuildIndex(connectContext, null);

        Expression where6 = new LessThan(new UnboundSlot(Lists.newArrayList("FinishTime")),
                new StringLiteral("2025-06-04 21:53:54"));
        sa = new ShowBuildIndexCommand("test", where6, null, 1, 0);
        sa.handleShowBuildIndex(connectContext, null);

        Expression where7 = new LessThanEqual(new UnboundSlot(Lists.newArrayList("FinishTime")),
                new StringLiteral("2025-06-04 21:53:54"));
        sa = new ShowBuildIndexCommand("test", where7, null, 1, 0);
        sa.handleShowBuildIndex(connectContext, null);

        Expression where8 = new GreaterThanEqual(new UnboundSlot(Lists.newArrayList("FinishTime")),
                new StringLiteral("2025-06-04 21:53:54"));
        sa = new ShowBuildIndexCommand("test", where8, null, 1, 0);
        sa.handleShowBuildIndex(connectContext, null);

        Expression where9 = new GreaterThan(new UnboundSlot(Lists.newArrayList("FinishTime")),
                new StringLiteral("2025-06-04 21:53:54"));
        sa = new ShowBuildIndexCommand("test", where9, null, 1, 0);
        sa.handleShowBuildIndex(connectContext, null);

        Expression where10 = new EqualTo(new UnboundSlot(Lists.newArrayList("PartitionName")),
                new StringLiteral("xxx"));
        sa = new ShowBuildIndexCommand("test", where10, null, 1, 0);
        sa.handleShowBuildIndex(connectContext, null);
    }
}
