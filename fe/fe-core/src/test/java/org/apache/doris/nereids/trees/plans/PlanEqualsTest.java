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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHeapSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.BigIntType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

// TODO: need more detailed test
public class PlanEqualsTest {
    /* *************************** Logical *************************** */
    @Test
    public void testLogicalAggregate(@Mocked Plan child) {
        List<Expression> groupByExprList = Lists.newArrayList();
        List<NamedExpression> outputExpressionList = ImmutableList.of(
                new SlotReference("a", new BigIntType(), true, Lists.newArrayList())
        );

        LogicalAggregate one = new LogicalAggregate(groupByExprList, outputExpressionList, child);
        Assert.assertEquals(one, one);

        List<Expression> groupByExprList1 = Lists.newArrayList();
        List<NamedExpression> outputExpressionList1 = ImmutableList.of(
                new SlotReference("b", new BigIntType(), true, Lists.newArrayList())
        );

        LogicalAggregate two = new LogicalAggregate(groupByExprList1, outputExpressionList1, child);
        Assert.assertNotEquals(one, two);
    }

    @Test
    public void testLogicalFilter(@Mocked Plan child) {
        Expression predicate = new EqualTo(Literal.of(1), Literal.of(2));
        LogicalFilter logicalFilter = new LogicalFilter(predicate, child);
        Assert.assertEquals(logicalFilter, logicalFilter);

        Expression predicate1 = new EqualTo(Literal.of(1), Literal.of(1));
        LogicalFilter logicalFilter1 = new LogicalFilter(predicate1, child);
        Assert.assertNotEquals(logicalFilter, logicalFilter1);
    }

    @Test
    public void testLogicalJoin(@Mocked Plan left, @Mocked Plan right) {
        Expression condition = new EqualTo(
                new SlotReference("a", new BigIntType(), true, Lists.newArrayList()),
                new SlotReference("b", new BigIntType(), true, Lists.newArrayList())
        );
        LogicalJoin innerJoin = new LogicalJoin(JoinType.INNER_JOIN, Optional.of(condition), left, right);
        Assert.assertEquals(innerJoin, innerJoin);

        // Notice: condition1 != condition, so following is `assertNotEquals`.
        // Because SlotReference.exprId is UUID.
        Expression condition1 = new EqualTo(
                new SlotReference("a", new BigIntType(), true, Lists.newArrayList()),
                new SlotReference("b", new BigIntType(), true, Lists.newArrayList())
        );
        LogicalJoin innerJoin1 = new LogicalJoin(JoinType.INNER_JOIN, Optional.of(condition1), left, right);
        Assert.assertNotEquals(innerJoin, innerJoin1);

        Expression condition2 = new EqualTo(
                new SlotReference("a", new BigIntType(), false, Lists.newArrayList()),
                new SlotReference("b", new BigIntType(), true, Lists.newArrayList())
        );
        LogicalJoin innerJoin2 = new LogicalJoin(JoinType.INNER_JOIN, Optional.of(condition2), left, right);
        Assert.assertNotEquals(innerJoin, innerJoin2);
    }

    @Test
    public void testLogicalOlapScan() {
        LogicalOlapScan olapScan = new LogicalOlapScan(new Table(TableType.OLAP), Lists.newArrayList());
        Assert.assertEquals(olapScan, olapScan);

        LogicalOlapScan olapScan1 = new LogicalOlapScan(new Table(TableType.OLAP), Lists.newArrayList());
        Assert.assertEquals(olapScan, olapScan1);
    }

    @Test
    public void testLogicalProject(@Mocked Plan child) {
        // TODO: Depend on NamedExpression Equals
        SlotReference aSlot = new SlotReference("a", new BigIntType(), true, Lists.newArrayList());
        List<NamedExpression> projects = ImmutableList.of(aSlot);
        LogicalProject logicalProject = new LogicalProject(projects, child);
        Assert.assertEquals(logicalProject, logicalProject);

        LogicalProject logicalProjectWithSameSlot = new LogicalProject(ImmutableList.of(aSlot), child);
        Assert.assertEquals(logicalProject, logicalProjectWithSameSlot);

        SlotReference a1Slot = new SlotReference("a", new BigIntType(), true, Lists.newArrayList());
        LogicalProject a1LogicalProject = new LogicalProject(ImmutableList.of(a1Slot), child);
        Assert.assertNotEquals(logicalProject, a1LogicalProject);

        SlotReference bSlot = new SlotReference("b", new BigIntType(), true, Lists.newArrayList());
        LogicalProject bLogicalProject1 = new LogicalProject(ImmutableList.of(bSlot), child);
        Assert.assertNotEquals(logicalProject, bLogicalProject1);
    }

    @Test
    public void testLogicalSort(@Mocked Plan child) {
        // TODO: Depend on List<OrderKey> Equals
        List<OrderKey> orderKeyList = Lists.newArrayList();
        LogicalSort logicalSort = new LogicalSort(orderKeyList, child);
        Assert.assertEquals(logicalSort, logicalSort);

        List<OrderKey> orderKeyListClone = Lists.newArrayList();
        LogicalSort logicalSortClone = new LogicalSort(orderKeyListClone, child);
        Assert.assertEquals(logicalSort, logicalSortClone);
    }

    /* *************************** Physical *************************** */
    @Test
    public void testPhysicalAggregate(@Mocked Plan child, @Mocked LogicalProperties logicalProperties) {
        List<Expression> groupByExprList = Lists.newArrayList();
        SlotReference slotReference = new SlotReference("a", new BigIntType(), true, Lists.newArrayList());
        List<NamedExpression> outputExpressionList = ImmutableList.of(slotReference);
        List<Expression> partitionExprList = Lists.newArrayList();
        AggPhase aggPhase = AggPhase.LOCAL;
        boolean usingStream = true;

        PhysicalAggregate physicalAggregate = new PhysicalAggregate(groupByExprList, outputExpressionList,
                partitionExprList, aggPhase, usingStream, logicalProperties, child);
        Assert.assertEquals(physicalAggregate, physicalAggregate);
    }

    @Test
    public void testPhysicalFilter(@Mocked Plan child, @Mocked LogicalProperties logicalProperties) {
        Expression predicate = new EqualTo(Literal.of(1), Literal.of(2));

        PhysicalFilter physicalFilter = new PhysicalFilter(predicate, logicalProperties, child);
        Assert.assertEquals(physicalFilter, physicalFilter);
    }

    @Test
    public void testPhysicalJoin(@Mocked Plan left, @Mocked Plan right, @Mocked LogicalProperties logicalProperties) {
        Expression expression = new EqualTo(Literal.of(1), Literal.of(2));

        PhysicalHashJoin innerJoin = new PhysicalHashJoin(JoinType.INNER_JOIN, Optional.of(expression),
                logicalProperties, left,
                right);
        Assert.assertEquals(innerJoin, innerJoin);
    }

    @Test
    public void testPhysicalOlapScan(@Mocked LogicalProperties logicalProperties, @Mocked OlapTable olapTable) {
        List<String> qualifier = Lists.newArrayList();

        PhysicalOlapScan olapScan = new PhysicalOlapScan(olapTable, qualifier, Optional.empty(), logicalProperties);

        Assert.assertEquals(olapScan, olapScan);
    }

    @Test
    public void testPhysicalProject(@Mocked Plan child, @Mocked LogicalProperties logicalProperties) {

        // TODO: Depend on NamedExpression Equals
        SlotReference aSlot = new SlotReference("a", new BigIntType(), true, Lists.newArrayList());
        List<NamedExpression> projects = ImmutableList.of(aSlot);
        PhysicalProject physicalProject = new PhysicalProject(projects, logicalProperties, child);
        Assert.assertEquals(physicalProject, physicalProject);

        PhysicalProject physicalProjectWithSameSlot = new PhysicalProject(ImmutableList.of(aSlot), logicalProperties,
                child);
        Assert.assertEquals(physicalProject, physicalProjectWithSameSlot);

        SlotReference a1Slot = new SlotReference("a", new BigIntType(), true, Lists.newArrayList());
        PhysicalProject a1PhysicalProject = new PhysicalProject(ImmutableList.of(a1Slot), logicalProperties, child);
        Assert.assertNotEquals(physicalProject, a1PhysicalProject);
    }

    @Test
    public void testPhysicalSort(@Mocked Plan child, @Mocked LogicalProperties logicalProperties) {
        // TODO: Depend on List<OrderKey> Equals
        List<OrderKey> orderKeyList = Lists.newArrayList();

        PhysicalHeapSort physicalHeapSort = new PhysicalHeapSort(orderKeyList, -1, 0, logicalProperties, child);
        Assert.assertEquals(physicalHeapSort, physicalHeapSort);

        List<OrderKey> orderKeyListClone = Lists.newArrayList();
        PhysicalHeapSort physicalHeapSortClone = new PhysicalHeapSort(orderKeyListClone, -1, 0, logicalProperties,
                child);
        Assert.assertEquals(physicalHeapSort, physicalHeapSortClone);
    }
}
