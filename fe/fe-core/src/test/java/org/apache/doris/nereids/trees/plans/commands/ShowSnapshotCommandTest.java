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

import org.apache.doris.common.UserException;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowSnapshotCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
    }

    @Override
    public void createTable(String sql) throws Exception {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateTableCommand);
        ((CreateTableCommand) plan).run(connectContext, null);
    }

    @Test
    void testValidate() throws UserException {
        // test where is null
        ShowSnapshotCommand ss = new ShowSnapshotCommand("test", null);
        Assertions.assertTrue(ss.validate(connectContext));

        // test where is ComparisonPredicate, child(0) is UnboundSlot and child(1) is StringLiteral
        Expression where1 = new EqualTo(new UnboundSlot(Lists.newArrayList("snapshot")),
                new StringLiteral("mysql_edu_mall_mall_source_occupy_1741669209_174166921"));
        ss = new ShowSnapshotCommand("test", where1);
        Assertions.assertTrue(ss.validate(connectContext));

        // test where is ComparisonPredicate, child(0) is UnboundSlot but value is empty
        Expression where2 = new EqualTo(new UnboundSlot(Lists.newArrayList("snapshot")),
                new StringLiteral(""));
        ss = new ShowSnapshotCommand("test", where2);
        Assertions.assertFalse(ss.validate(connectContext));

        // test where is ComparisonPredicate, child(0) is UnboundSlot but key is timestamp. It should return false
        Expression where3 = new EqualTo(new UnboundSlot(Lists.newArrayList("timestamp")),
                new StringLiteral("2025-01-01 01:01:01"));
        ss = new ShowSnapshotCommand("test", where3);
        Assertions.assertFalse(ss.validate(connectContext));

        // test where is ComparisonPredicate, child(0) is UnboundSlot but value is empty
        Expression where4 = new EqualTo(new UnboundSlot(Lists.newArrayList("timestamp")),
                new StringLiteral(""));
        ss = new ShowSnapshotCommand("test", where4);
        Assertions.assertFalse(ss.validate(connectContext));

        // test where is ComparisonPredicate, child(0) is UnboundSlot but key is snapshotType
        Expression where5 = new EqualTo(new UnboundSlot(Lists.newArrayList("snapshotType")),
                new StringLiteral("remote"));
        ss = new ShowSnapshotCommand("test", where5);
        Assertions.assertTrue(ss.validate(connectContext));

        // test where is ComparisonPredicate, child(0) is UnboundSlot but value is empty
        Expression where6 = new EqualTo(new UnboundSlot(Lists.newArrayList("snapshotType")),
                new StringLiteral(""));
        ss = new ShowSnapshotCommand("test", where6);
        Assertions.assertFalse(ss.validate(connectContext));

        // test where is ComparisonPredicate, child(0) is UnboundSlot, key is snapshotType but value is other word
        Expression where7 = new EqualTo(new UnboundSlot(Lists.newArrayList("snapshotType")),
                new StringLiteral("xxx"));
        ss = new ShowSnapshotCommand("test", where7);
        Assertions.assertFalse(ss.validate(connectContext));

        // test where is And, child(0) and child(1) both ComparisonPredicate
        Expression equalTo1 = new EqualTo(new UnboundSlot(Lists.newArrayList("snapshot")),
                new StringLiteral("mysql_edu_mall_mall_source_occupy_1741669209_174166921"));
        Expression equalTo2 = new EqualTo(new UnboundSlot(Lists.newArrayList("timestamp")),
                new StringLiteral("2018-04-18-19-19-10"));
        Expression where8 = new And(equalTo1, equalTo2);
        ss = new ShowSnapshotCommand("test", where8);
        Assertions.assertTrue(ss.validate(connectContext));

        // test where is And, child(0) or child(1) is ComparisonPredicate
        Expression equalTo3 = new EqualTo(new UnboundSlot(Lists.newArrayList("snapshot")),
                new StringLiteral("mysql_edu_mall_mall_source_occupy_1741669209_174166921"));
        Expression like = new Like(new UnboundSlot(Lists.newArrayList("timestamp")),
                new StringLiteral("2018-04-18-19-19-10%"));
        Expression where9 = new And(equalTo3, like);
        ss = new ShowSnapshotCommand("test", where9);
        Assertions.assertFalse(ss.validate(connectContext));

        // test where is Or
        Expression equalTo4 = new EqualTo(new UnboundSlot(Lists.newArrayList("snapshot")),
                new StringLiteral("mysql_edu_mall_mall_source_occupy_1741669209_174166921"));
        Expression equalTo5 = new EqualTo(new UnboundSlot(Lists.newArrayList("timestamp")),
                new StringLiteral("2018-04-18-19-19-10"));
        Expression where10 = new Or(equalTo4, equalTo5);
        ss = new ShowSnapshotCommand("test", where10);
        Assertions.assertFalse(ss.validate(connectContext));

    }
}
