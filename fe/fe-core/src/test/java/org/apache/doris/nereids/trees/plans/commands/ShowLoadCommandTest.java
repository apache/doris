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

public class ShowLoadCommandTest extends TestWithFeService {
    public static final ImmutableList<String> LOAD_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("Label").add("State").add("Progress")
            .add("Type").add("EtlInfo").add("TaskInfo").add("ErrorMsg").add("CreateTime")
            .add("EtlStartTime").add("EtlFinishTime").add("LoadStartTime").add("LoadFinishTime")
            .add("URL").add("JobDetails").add("TransactionId").add("ErrorTablets").add("User").add("Comment")
            .build();

    // STREAM_LOAD_TITLE_NAMES copy from org.apache.doris.analysis.org.apache.doris.analysis
    public static final ImmutableList<String> STREAM_LOAD_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Label").add("Db").add("Table")
            .add("ClientIp").add("Status").add("Message").add("Url").add("TotalRows")
            .add("LoadedRows").add("FilteredRows").add("UnselectedRows").add("LoadBytes")
            .add("StartTime").add("FinishTime").add("User").add("Comment")
            .build();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    @Test
    void testValidate() throws UserException {
        // test where is null but db is not null
        ShowLoadCommand sl = new ShowLoadCommand(null, null, -1, -1, "test");
        Assertions.assertTrue(sl.validate(connectContext));

        // test where is not null
        Expression where1 = new EqualTo(new UnboundSlot(Lists.newArrayList("LABEL")),
                new StringLiteral("xxx"));
        sl = new ShowLoadCommand(where1, null, -1, -1, "test");
        Assertions.assertTrue(sl.validate(connectContext));
        Assertions.assertTrue(sl.isAccurateMatch());

        // test where is And, child(0) or child(1) is ComparisonPredicate
        Expression equalTo = new EqualTo(new UnboundSlot(Lists.newArrayList("STATE")),
                new StringLiteral("PENDING"));
        Expression like = new Like(new UnboundSlot(Lists.newArrayList("LABEL")),
                new StringLiteral("xxx%"));
        Expression where3 = new And(equalTo, like);
        sl = new ShowLoadCommand(where3, null, -1, -1, "test");
        Assertions.assertTrue(sl.validate(connectContext));

        // test where is Or
        Expression equalTo4 = new EqualTo(new UnboundSlot(Lists.newArrayList("STATE")),
                new StringLiteral("STATE"));
        Expression equalTo5 = new EqualTo(new UnboundSlot(Lists.newArrayList("LABEL")),
                new StringLiteral("xxx"));
        Expression where4 = new Or(equalTo4, equalTo5);
        sl = new ShowLoadCommand(where4, null, -1, -1, "test");
        ShowLoadCommand finalSr3 = sl;
        Assertions.assertThrows(AnalysisException.class, () -> finalSr3.validate(connectContext));

        // test where is And, child(0) or child(1) is EqualTo
        Expression equalTo1 = new EqualTo(new UnboundSlot(Lists.newArrayList("STATE")),
                new StringLiteral("PENDING"));
        Expression equalTo2 = new EqualTo(new UnboundSlot(Lists.newArrayList("LABEL")),
                new StringLiteral("xxx"));
        Expression where8 = new And(equalTo1, equalTo2);
        sl = new ShowLoadCommand(where8, null, -1, -1, "test");
        Assertions.assertTrue(sl.validate(connectContext));

        Expression equalTo6 = new EqualTo(new UnboundSlot(Lists.newArrayList("STATE1")),
                new StringLiteral("STATE1"));
        Expression equalTo7 = new EqualTo(new UnboundSlot(Lists.newArrayList("LABEL1")),
                new StringLiteral("LABEL1"));
        Expression where9 = new And(equalTo6, equalTo7);
        sl = new ShowLoadCommand(where9, null, -1, -1, "test");
        Assertions.assertFalse(sl.validate(connectContext));

        // test for strem load
        Expression where10 = new EqualTo(new UnboundSlot(Lists.newArrayList("LABEL")),
                new StringLiteral("xxx"));
        sl = new ShowLoadCommand(where10, null, -1, -1, "test", true);
        Assertions.assertTrue(sl.validate(connectContext));
        Assertions.assertTrue(sl.isAccurateMatch());
    }

    @Test
    public void testProcessOrderBy() throws AnalysisException {
        UnboundSlot key = new UnboundSlot(Lists.newArrayList("LABEL"));
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        ShowLoadCommand sl = new ShowLoadCommand(null, orderKeys, -1, -1, "test");
        ImmutableList<String> titles = sl.isStreamLoad() ? STREAM_LOAD_TITLE_NAMES : LOAD_TITLE_NAMES;
        ArrayList<OrderByPair> orderByPairs = sl.getOrderByPairs(orderKeys, titles);
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
        ShowLoadCommand sl = new ShowLoadCommand(null, orderKeys, limit, offset, "test");
        List<List<String>> rows = new ArrayList<>();
        List<String> row1 = new ArrayList<>();
        List<String> row2 = new ArrayList<>();
        row1.add("a");
        row1.add("b");
        row2.add("x");
        row2.add("y");
        rows.add(row1);
        rows.add(row2);
        rows = sl.applyLimit(limit, offset, rows);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("x", rows.get(0).get(0));
        Assertions.assertEquals("y", rows.get(0).get(1));
    }
}
