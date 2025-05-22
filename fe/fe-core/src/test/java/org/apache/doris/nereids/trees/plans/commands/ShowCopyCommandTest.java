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
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ShowCopyCommandTest extends TestWithFeService {
    public static final ImmutableList<String> LOAD_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("Label").add("State").add("Progress")
            .add("Type").add("EtlInfo").add("TaskInfo").add("ErrorMsg").add("CreateTime")
            .add("EtlStartTime").add("EtlFinishTime").add("LoadStartTime").add("LoadFinishTime")
            .add("URL").add("JobDetails").add("TransactionId").add("ErrorTablets").add("User").add("Comment")
            .build();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    @Test
    public void testValidate() throws AnalysisException {
        // test where is null but db is not null
        ShowCopyCommand command = new ShowCopyCommand("test", null, null, -1, -1);
        Assertions.assertTrue(command.validate(connectContext));

        // test where is not null
        Expression where1 = new EqualTo(new UnboundSlot(Lists.newArrayList("LABEL")),
                new StringLiteral("xxx"));
        command = new ShowCopyCommand("test", null, where1, -1, -1);
        Assertions.assertTrue(command.validate(connectContext));
        Assertions.assertTrue(command.isAccurateMatch());

        // test where is And, child(0) or child(1) is EqualTo
        Expression equalTo1 = new EqualTo(new UnboundSlot(Lists.newArrayList("STATE")),
                new StringLiteral("PENDING"));
        Expression equalTo2 = new EqualTo(new UnboundSlot(Lists.newArrayList("LABEL")),
                new StringLiteral("test_label"));
        Expression where3 = new And(equalTo1, equalTo2);
        command = new ShowCopyCommand("test", null, where3, -1, -1);
        Assertions.assertTrue(command.validate(connectContext));

        // test where is And, child(0) or child(1) is ComparisonPredicate
        Expression equalTo = new EqualTo(new UnboundSlot(Lists.newArrayList("STATE")),
                new StringLiteral("PENDING"));
        Expression like = new Like(new UnboundSlot(Lists.newArrayList("LABEL")),
                new StringLiteral("xxx%"));
        Expression where4 = new And(equalTo, like);
        command = new ShowCopyCommand("test", null, where4, -1, -1);
        Assertions.assertTrue(command.validate(connectContext));

        // test where is Or
        Expression equalTo4 = new EqualTo(new UnboundSlot(Lists.newArrayList("STATE")),
                new StringLiteral("STATE"));
        Expression equalTo5 = new EqualTo(new UnboundSlot(Lists.newArrayList("LABEL")),
                new StringLiteral("xxx"));
        Expression where5 = new Or(equalTo4, equalTo5);
        command = new ShowCopyCommand("test", null, where5, -1, -1);
        ShowCopyCommand command1 = command;
        Assertions.assertThrows(AnalysisException.class, () -> command1.validate(connectContext));
    }

    @Test
    public void testProcessOrderBy() throws AnalysisException {
        UnboundSlot key = new UnboundSlot(Lists.newArrayList("LABEL"));
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        ShowCopyCommand command = new ShowCopyCommand("test", orderKeys, null, -1, -1);
        ArrayList<OrderByPair> orderByPairs = command.getOrderByPairs(orderKeys, LOAD_TITLE_NAMES);
        OrderByPair op = orderByPairs.get(0);
        Assertions.assertFalse(op.isDesc());
        Assertions.assertEquals(1, op.getIndex());
    }

    @Test
    public void testApplyLimit() {
        UnboundSlot key = new UnboundSlot(Lists.newArrayList("LABEL"));
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        long limit = 1;
        long offset = 1;
        ShowCopyCommand command = new ShowCopyCommand("test", orderKeys, null, limit, offset);
        List<List<String>> rows = new ArrayList<>();
        List<String> row1 = new ArrayList<>();
        List<String> row2 = new ArrayList<>();
        row1.add("a");
        row1.add("b");
        row2.add("x");
        row2.add("y");
        rows.add(row1);
        rows.add(row2);
        rows = command.applyLimit(limit, offset, rows);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("x", rows.get(0).get(0));
        Assertions.assertEquals("y", rows.get(0).get(1));
    }
}
