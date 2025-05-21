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

public class ShowResourcesCommandTest extends TestWithFeService {
    // RESOURCE_PROC_NODE_TITLE_NAMES copy from org.apache.doris.catalog.ResourceMgr
    public static final ImmutableList<String> RESOURCE_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("ResourceType").add("Item").add("Value")
            .build();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    @Test
    void testValidate() throws UserException {
        // test where is null and like is null
        ShowResourcesCommand sr = new ShowResourcesCommand(null, null, null, -1, -1);
        Assertions.assertTrue(sr.validate(connectContext));

        // test where is null, like is not null
        sr = new ShowResourcesCommand(null, "%m%", null, -1, -1);
        Assertions.assertTrue(sr.validate(connectContext));

        // test where is not null ,like is not null
        Expression where1 = new EqualTo(new UnboundSlot(Lists.newArrayList("Name")),
                new StringLiteral("es_resource"));
        sr = new ShowResourcesCommand(where1, "%es%", null, -1, -1);
        Assertions.assertTrue(sr.validate(connectContext));

        // test where is not null , like is null
        Expression where2 = new EqualTo(new UnboundSlot(Lists.newArrayList("Name")),
                new StringLiteral("es_resource"));
        sr = new ShowResourcesCommand(where2, null, null, -1, -1);
        Assertions.assertTrue(sr.validate(connectContext));
        Assertions.assertTrue(sr.isAccurateMatch());

        // test where is And, child(0) or child(1) is ComparisonPredicate
        Expression equalTo = new EqualTo(new UnboundSlot(Lists.newArrayList("ResourceType")),
                new StringLiteral("es"));
        Expression like = new Like(new UnboundSlot(Lists.newArrayList("Name")),
                new StringLiteral("es_resource%"));
        Expression where3 = new And(equalTo, like);
        sr = new ShowResourcesCommand(where3, null, null, -1, -1);
        Assertions.assertTrue(sr.validate(connectContext));

        // test where is Or
        Expression equalTo4 = new EqualTo(new UnboundSlot(Lists.newArrayList("ResourceType")),
                new StringLiteral("mysql"));
        Expression equalTo5 = new EqualTo(new UnboundSlot(Lists.newArrayList("Name")),
                new StringLiteral("es_resource"));
        Expression where4 = new Or(equalTo4, equalTo5);
        sr = new ShowResourcesCommand(where4, null, null, -1, -1);
        ShowResourcesCommand finalSr = sr;
        Assertions.assertThrows(AnalysisException.class, () -> finalSr.validate(connectContext));

        // test where is And, child(0) or child(1) is EqualTo
        Expression equalTo1 = new EqualTo(new UnboundSlot(Lists.newArrayList("Name")),
                new StringLiteral("mysql"));
        Expression equalTo2 = new EqualTo(new UnboundSlot(Lists.newArrayList("ResourceType")),
                new StringLiteral("jdbc"));
        Expression where8 = new And(equalTo1, equalTo2);
        sr = new ShowResourcesCommand(where8, null, null, -1, -1);
        Assertions.assertTrue(sr.validate(connectContext));

        // test where is And, child(0) or child(1) is EqualTo, but not Name or ResourceType
        Expression equalTo6 = new EqualTo(new UnboundSlot(Lists.newArrayList("Name11")),
                new StringLiteral("mysql"));
        Expression equalTo7 = new EqualTo(new UnboundSlot(Lists.newArrayList("ResourceType11")),
                new StringLiteral("jdbc"));
        Expression where9 = new And(equalTo6, equalTo7);
        sr = new ShowResourcesCommand(where9, null, null, -1, -1);
        ShowResourcesCommand finalSr2 = sr;
        Assertions.assertThrows(AnalysisException.class, () -> finalSr2.validate(connectContext));
    }

    @Test
    public void testProcessOrderBy() throws AnalysisException {
        UnboundSlot key = new UnboundSlot(Lists.newArrayList("Name"));
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        ShowResourcesCommand sr = new ShowResourcesCommand(null, null, orderKeys, -1, -1);
        ArrayList<OrderByPair> orderByPairs = sr.getOrderByPairs(orderKeys, RESOURCE_PROC_NODE_TITLE_NAMES);
        OrderByPair op = orderByPairs.get(0);
        Assertions.assertFalse(op.isDesc());
        Assertions.assertEquals(0, op.getIndex());
    }
}
