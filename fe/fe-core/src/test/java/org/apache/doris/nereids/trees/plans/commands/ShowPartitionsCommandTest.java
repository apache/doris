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
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ShowPartitionsCommandTest extends TestWithFeService {
    private Auth auth;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    @Test
    void testValidate() throws UserException {
        TableNameInfo tbl = new TableNameInfo("internal", "test", "tbl");

        ShowPartitionsCommand sp = new ShowPartitionsCommand(tbl, null, null, -1, -1, false);
        // because validate method will return void or throw error, so it's successful when no error thrown
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, null, null, 1, 0, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, null, null, 2, 1, false);
        sp.validate(connectContext);
        UnboundSlot key = new UnboundSlot(Lists.newArrayList("PartitionName"));
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        sp = new ShowPartitionsCommand(tbl, null, orderKeys, 2, 1, false);
        sp.validate(connectContext);

        Expression where1 = new EqualTo(new UnboundSlot(Lists.newArrayList("PartitionId")),
                new BigIntLiteral(1748399001963L));
        sp = new ShowPartitionsCommand(tbl, where1, null, -1, -1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where1, null, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where1, orderKeys, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where1, orderKeys, -1, -1, false);
        sp.validate(connectContext);

        Expression where2 = new Not(where1);
        sp = new ShowPartitionsCommand(tbl, where2, null, -1, -1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where2, null, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where2, orderKeys, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where2, orderKeys, -1, -1, false);
        sp.validate(connectContext);

        Expression where3 = new EqualTo(new UnboundSlot(Lists.newArrayList("PartitionName")),
                new StringLiteral("xxx"));
        sp = new ShowPartitionsCommand(tbl, where3, null, -1, -1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where3, null, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where3, orderKeys, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where3, orderKeys, -1, -1, false);
        sp.validate(connectContext);

        Expression where4 = new EqualTo(new UnboundSlot(Lists.newArrayList("State")),
                new StringLiteral("xxx"));
        sp = new ShowPartitionsCommand(tbl, where4, null, -1, -1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where4, null, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where4, orderKeys, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where4, orderKeys, -1, -1, false);
        sp.validate(connectContext);

        Expression where5 = new EqualTo(new UnboundSlot(Lists.newArrayList("Buckets")),
                new IntegerLiteral(16));
        sp = new ShowPartitionsCommand(tbl, where5, null, -1, -1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where5, null, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where5, orderKeys, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where5, orderKeys, -1, -1, false);
        sp.validate(connectContext);

        Expression where6 = new EqualTo(new UnboundSlot(Lists.newArrayList("ReplicationNum")),
                new IntegerLiteral(3));
        sp = new ShowPartitionsCommand(tbl, where6, null, -1, -1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where6, null, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where6, orderKeys, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where6, orderKeys, -1, -1, false);
        sp.validate(connectContext);

        Expression where7 = new Like(new UnboundSlot(Lists.newArrayList("State")),
                new StringLiteral("xxx"));
        sp = new ShowPartitionsCommand(tbl, where7, null, -1, -1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where7, null, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where7, orderKeys, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where7, orderKeys, -1, -1, false);
        sp.validate(connectContext);

        Expression where8 = new GreaterThan(new UnboundSlot(Lists.newArrayList("Buckets")),
                new IntegerLiteral(16));
        sp = new ShowPartitionsCommand(tbl, where8, null, -1, -1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where8, null, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where8, orderKeys, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where8, orderKeys, -1, -1, false);
        sp.validate(connectContext);

        Expression where9 = new GreaterThanEqual(new UnboundSlot(Lists.newArrayList("Buckets")),
                new IntegerLiteral(16));
        sp = new ShowPartitionsCommand(tbl, where9, null, -1, -1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where9, null, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where9, orderKeys, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where9, orderKeys, -1, -1, false);
        sp.validate(connectContext);

        Expression where10 = new LessThan(new UnboundSlot(Lists.newArrayList("Buckets")),
                new IntegerLiteral(16));
        sp = new ShowPartitionsCommand(tbl, where10, null, -1, -1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where10, null, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where10, orderKeys, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where10, orderKeys, -1, -1, false);
        sp.validate(connectContext);

        Expression where11 = new LessThanEqual(new UnboundSlot(Lists.newArrayList("Buckets")),
                new IntegerLiteral(16));
        sp = new ShowPartitionsCommand(tbl, where11, null, -1, -1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where11, null, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where11, orderKeys, 1, 1, false);
        sp.validate(connectContext);
        sp = new ShowPartitionsCommand(tbl, where11, orderKeys, -1, -1, false);
        sp.validate(connectContext);

        Expression equalTo1 = new EqualTo(new UnboundSlot(Lists.newArrayList("STATE")),
                new StringLiteral("xxx"));
        Expression equalTo2 = new EqualTo(new UnboundSlot(Lists.newArrayList("PartitionName")),
                new StringLiteral("yyy"));
        Expression where12 = new And(equalTo1, equalTo2);
        sp = new ShowPartitionsCommand(tbl, where12, null, -1, -1, false);
        sp.validate(connectContext);

        Expression equalTo3 = new EqualTo(new UnboundSlot(Lists.newArrayList("STATE")),
                new StringLiteral("xxx"));
        Expression equalTo4 = new EqualTo(new UnboundSlot(Lists.newArrayList("PartitionName")),
                new StringLiteral("yyy"));
        Expression equalTo5 = new EqualTo(new UnboundSlot(Lists.newArrayList("Buckets")),
                new IntegerLiteral(16));
        List<Expression> and = new ArrayList<>();
        and.add(equalTo3);
        and.add(equalTo4);
        and.add(equalTo5);
        Expression where13 = new And(and);
        sp = new ShowPartitionsCommand(tbl, where13, null, -1, -1, false);
        sp.validate(connectContext);

        Expression where14 = new EqualTo(new UnboundSlot(Lists.newArrayList("VisibleVersion")),
                new IntegerLiteral(1));
        sp = new ShowPartitionsCommand(tbl, where14, null, -1, -1, false);
        ShowPartitionsCommand finalSp = sp;
        Assertions.assertThrows(AnalysisException.class, () -> finalSp.validate(connectContext));

        Expression equalTo6 = new EqualTo(new UnboundSlot(Lists.newArrayList("STATE")),
                new StringLiteral("xxx"));
        Expression equalTo7 = new EqualTo(new UnboundSlot(Lists.newArrayList("PartitionName")),
                new StringLiteral("yyy"));
        Expression where15 = new Or(equalTo6, equalTo7);
        sp = new ShowPartitionsCommand(tbl, where15, null, -1, -1, false);
        ShowPartitionsCommand finalSp1 = sp;
        Assertions.assertThrows(AnalysisException.class, () -> finalSp1.validate(connectContext));

        Expression where16 = new GreaterThanEqual(new UnboundSlot(Lists.newArrayList("STATE")),
                new IntegerLiteral(16));
        sp = new ShowPartitionsCommand(tbl, where16, null, -1, -1, false);
        ShowPartitionsCommand finalSp2 = sp;
        Assertions.assertThrows(AnalysisException.class, () -> finalSp2.validate(connectContext));
    }
}
