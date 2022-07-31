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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.BigIntType;

import com.google.common.collect.ImmutableList;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class JoinCommuteTest {
    @Test
    public void testInnerJoinCommute(@Mocked PlannerContext plannerContext) {
        Table table1 = new Table(0L, "table1", Table.TableType.OLAP,
                ImmutableList.of(new Column("id", Type.INT, true, AggregateType.NONE, "0", "")));
        LogicalOlapScan scan1 = new LogicalOlapScan(table1, ImmutableList.of());

        Table table2 = new Table(0L, "table2", Table.TableType.OLAP,
                ImmutableList.of(new Column("id", Type.INT, true, AggregateType.NONE, "0", "")));
        LogicalOlapScan scan2 = new LogicalOlapScan(table2, ImmutableList.of());

        Expression onCondition = new EqualTo(
                new SlotReference("id", new BigIntType(), true, ImmutableList.of("table1")),
                new SlotReference("id", new BigIntType(), true, ImmutableList.of("table2")));
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> join = new LogicalJoin<>(
                JoinType.INNER_JOIN, Optional.of(onCondition), scan1, scan2);

        Rule rule = new JoinCommute(true).build();

        List<Plan> transform = rule.transform(join, plannerContext);
        Assertions.assertEquals(1, transform.size());
        Plan newJoin = transform.get(0);

        Assertions.assertEquals(join.child(0), newJoin.child(1));
        Assertions.assertEquals(join.child(1), newJoin.child(0));
    }

}
