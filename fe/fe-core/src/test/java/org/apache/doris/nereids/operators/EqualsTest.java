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

package org.apache.doris.nereids.operators;

import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.operators.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.operators.plans.logical.LogicalFilter;
import org.apache.doris.nereids.operators.plans.logical.LogicalJoin;
import org.apache.doris.nereids.operators.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.operators.plans.logical.LogicalSort;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.BigIntType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class EqualsTest {
    @Test
    public void testLogicalAggregate() {
        List<Expression> groupByExprList = Lists.newArrayList();
        SlotReference slotReference = new SlotReference("a", new BigIntType(), true, Lists.newArrayList());
        List<NamedExpression> outputExpressionList = ImmutableList.of(slotReference);

        LogicalAggregate logicalAggregate = new LogicalAggregate(groupByExprList, outputExpressionList);
        Assert.assertEquals(logicalAggregate, logicalAggregate);
    }

    @Test
    public void testLogicalFilter() {
        Expression predicate = new EqualTo<>(Literal.of(1), Literal.of(2));
        LogicalFilter logicalFilter = new LogicalFilter(predicate);
        Assert.assertEquals(logicalFilter, logicalFilter);
    }

    @Test
    public void testLogicalJoin() {
        LogicalJoin innerJoin = new LogicalJoin(JoinType.INNER_JOIN);
        Assert.assertEquals(innerJoin, innerJoin);
    }

    @Test
    public void testLogicalOlapScan() {
        Table table = new Table(TableType.OLAP);
        LogicalOlapScan olapScan = new LogicalOlapScan(table, Lists.newArrayList());
        Assert.assertEquals(olapScan, olapScan);
    }

    @Test
    public void testLogicalProject() {
        // TODO: Depend on NamedExpression Equals
        SlotReference aSlot = new SlotReference("a", new BigIntType(), true, Lists.newArrayList());
        List<NamedExpression> projects = ImmutableList.of(aSlot);
        LogicalProject logicalProject = new LogicalProject(projects);
        Assert.assertEquals(logicalProject, logicalProject);

        LogicalProject logicalProjectWithSameSlot = new LogicalProject(ImmutableList.of(aSlot));
        Assert.assertEquals(logicalProject, logicalProjectWithSameSlot);

        SlotReference a1Slot = new SlotReference("a", new BigIntType(), true, Lists.newArrayList());
        LogicalProject a1LogicalProject = new LogicalProject(ImmutableList.of(a1Slot));
        Assert.assertNotEquals(logicalProject, a1LogicalProject);

        SlotReference bSlot = new SlotReference("b", new BigIntType(), true, Lists.newArrayList());
        LogicalProject bLogicalProject1 = new LogicalProject(ImmutableList.of(bSlot));
        Assert.assertNotEquals(logicalProject, bLogicalProject1);
    }

    @Test
    public void testLogicalSort() {
        // TODO: Depend on List<OrderKey> Equals
        List<OrderKey> orderKeyList = Lists.newArrayList();
        LogicalSort logicalSort = new LogicalSort(orderKeyList);
        Assert.assertEquals(logicalSort, logicalSort);

        List<OrderKey> orderKeyListClone = Lists.newArrayList();
        LogicalSort logicalSortClone = new LogicalSort(orderKeyListClone);
        Assert.assertNotEquals(logicalSort, logicalSortClone);
    }
}
