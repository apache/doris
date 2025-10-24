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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class PushDownFilterThroughSetOperationTest {

    private List<NamedExpression> outputs = ImmutableList.of(
            new SlotReference(new ExprId(1), "c1", TinyIntType.INSTANCE, true, Collections.emptyList()),
            new SlotReference(new ExprId(2), "c2", StringType.INSTANCE, true, Collections.emptyList())
    );

    private List<SlotReference> regulatorChildOutputs1 = ImmutableList.of(
            new SlotReference(new ExprId(11), "c1", TinyIntType.INSTANCE, true, Collections.emptyList()),
            new SlotReference(new ExprId(12), "c2", StringType.INSTANCE, true, Collections.emptyList())
    );
    private List<SlotReference> regulatorChildOutputs2 = ImmutableList.of(
            new SlotReference(new ExprId(21), "c1", TinyIntType.INSTANCE, true, Collections.emptyList()),
            new SlotReference(new ExprId(22), "c2", StringType.INSTANCE, true, Collections.emptyList())
    );

    private List<List<SlotReference>> regulatorChildrenOutputs = ImmutableList.of(
            regulatorChildOutputs1, regulatorChildOutputs2
    );

    private List<NamedExpression> constantExpr1 = ImmutableList.of(
            new SlotReference(new ExprId(41), "c1", TinyIntType.INSTANCE, true, Collections.emptyList()),
            new SlotReference(new ExprId(42), "c2", StringType.INSTANCE, true, Collections.emptyList())
    );
    private List<NamedExpression> constantExpr2 = ImmutableList.of(
            new SlotReference(new ExprId(51), "c1", TinyIntType.INSTANCE, true, Collections.emptyList()),
            new SlotReference(new ExprId(52), "c2", StringType.INSTANCE, true, Collections.emptyList())
    );
    private List<List<NamedExpression>> constantExprs = ImmutableList.of(constantExpr1, constantExpr2);

    private Plan child1 = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(
            new SlotReference(new ExprId(11), "c1", TinyIntType.INSTANCE, true, Collections.emptyList()),
            new SlotReference(new ExprId(12), "c2", StringType.INSTANCE, true, Collections.emptyList()),
            new SlotReference(new ExprId(13), "c3", SmallIntType.INSTANCE, true, Collections.emptyList())
    ));
    private Plan child2 = new LogicalOneRowRelation(new RelationId(2), ImmutableList.of(
            new SlotReference(new ExprId(22), "c2", StringType.INSTANCE, true, Collections.emptyList()),
            new SlotReference(new ExprId(23), "c3", SmallIntType.INSTANCE, true, Collections.emptyList()),
            new SlotReference(new ExprId(21), "c1", TinyIntType.INSTANCE, true, Collections.emptyList())
    ));
    private List<Plan> children = ImmutableList.of(child1, child2);

    @Test
    public void testAddFiltersToNewChildrenWithUnion() {
        List<Plan> newChildren = Lists.newArrayList();
        List<List<SlotReference>> newRegularChildrenOutputs = Lists.newArrayList();
        List<List<NamedExpression>> newConstantExprs = new ArrayList<>();
        LogicalSetOperation setOperation = new LogicalUnion(Qualifier.ALL, outputs, regulatorChildrenOutputs, ImmutableList.of(), false, children);
        LogicalFilter<?> filter = new LogicalFilter<Plan>(Collections.emptySet(), setOperation);

        try (MockedStatic<EliminateFilter> mockedEliminateFilter = Mockito.mockStatic(EliminateFilter.class)) {
            mockedEliminateFilter
                    .when(() -> EliminateFilter.eliminateFilterOnOneRowRelation(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> invocation.getArgument(0));

            PushDownFilterThroughSetOperation.addFiltersToNewChildren(
                    setOperation, filter, children, regulatorChildrenOutputs, null,
                    newChildren, newRegularChildrenOutputs, newConstantExprs,
                    (rowIndex, columnIndex) -> setOperation.getRegularChildOutput(rowIndex).get(columnIndex),
                    Function.identity());

            Assertions.assertEquals(2, newChildren.size());
            Assertions.assertInstanceOf(LogicalFilter.class, newChildren.get(0));
            Assertions.assertInstanceOf(LogicalFilter.class, newChildren.get(1));
            Assertions.assertEquals(regulatorChildrenOutputs, newRegularChildrenOutputs);
        }
    }

    @Test
    public void testAddFiltersToNewChildrenWithUnionWithoutNewConstantOutput() {
        List<Plan> newChildren = Lists.newArrayList();
        List<List<SlotReference>> newRegularChildrenOutputs = Lists.newArrayList();
        LogicalSetOperation setOperation = new LogicalUnion(Qualifier.ALL, outputs, regulatorChildrenOutputs, ImmutableList.of(), false, children);
        LogicalFilter<?> filter = new LogicalFilter<Plan>(Collections.emptySet(), setOperation);

        try (MockedStatic<EliminateFilter> mockedEliminateFilter = Mockito.mockStatic(EliminateFilter.class)) {
            mockedEliminateFilter
                    .when(() -> EliminateFilter.eliminateFilterOnOneRowRelation(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> invocation.getArgument(0));

            PushDownFilterThroughSetOperation.addFiltersToNewChildren(
                    setOperation, filter, children, regulatorChildrenOutputs, null,
                    newChildren, newRegularChildrenOutputs, null,
                    (rowIndex, columnIndex) -> setOperation.getRegularChildOutput(rowIndex).get(columnIndex),
                    Function.identity());

            Assertions.assertEquals(2, newChildren.size());
            Assertions.assertInstanceOf(LogicalFilter.class, newChildren.get(0));
            Assertions.assertInstanceOf(LogicalFilter.class, newChildren.get(1));
            Assertions.assertEquals(regulatorChildrenOutputs, newRegularChildrenOutputs);
        }
    }

    @Test
    public void testAddFiltersToNewChildrenWithUnionWithConstant() {
        List<Plan> newChildren = Lists.newArrayList();
        List<List<SlotReference>> newRegularChildrenOutputs = Lists.newArrayList();
        List<List<NamedExpression>> newConstantExprs = new ArrayList<>();
        LogicalSetOperation setOperation = new LogicalUnion(Qualifier.ALL, outputs, regulatorChildrenOutputs, ImmutableList.of(), false, children);
        LogicalFilter<?> filter = new LogicalFilter<Plan>(Collections.emptySet(), setOperation);

        try (MockedStatic<EliminateFilter> mockedEliminateFilter = Mockito.mockStatic(EliminateFilter.class)) {
            mockedEliminateFilter
                    .when(() -> EliminateFilter.eliminateFilterOnOneRowRelation(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> {
                        LogicalFilter<?> input = invocation.getArgument(0);
                        LogicalPlan child = (LogicalPlan) input.child();
                        if (child.equals(child1)) {
                            return child;
                        }
                        if (child.equals(child2)) {
                            return new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
                        }
                        LogicalOneRowRelation relation = (LogicalOneRowRelation) child;
                        if (relation.getExpressions().equals(constantExpr1)) {
                            return relation;
                        }
                        if (relation.getExpressions().equals(constantExpr2)) {
                            return input;
                        }
                        return input;
                    });

            PushDownFilterThroughSetOperation.addFiltersToNewChildren(
                    setOperation, filter, children, regulatorChildrenOutputs, null,
                    newChildren, newRegularChildrenOutputs, newConstantExprs,
                    (rowIndex, columnIndex) -> setOperation.getRegularChildOutput(rowIndex).get(columnIndex),
                    Function.identity());

            PushDownFilterThroughSetOperation.addFiltersToNewChildren(
                    setOperation, filter, constantExprs, regulatorChildrenOutputs, null,
                    newChildren, newRegularChildrenOutputs, newConstantExprs,
                    (rowIndex, columnIndex) -> setOperation.getRegularChildOutput(rowIndex).get(columnIndex),
                    selectConstants -> new LogicalOneRowRelation(
                            new RelationId(100), selectConstants)
            );

            Assertions.assertEquals(1, newChildren.size());
            Assertions.assertInstanceOf(LogicalFilter.class, newChildren.get(0));
            Assertions.assertEquals(1, newRegularChildrenOutputs.size());
            Assertions.assertEquals(ImmutableList.of(constantExpr2), newRegularChildrenOutputs);
            Assertions.assertEquals(2, newConstantExprs.size());
            Assertions.assertEquals(ImmutableList.of(regulatorChildOutputs1, constantExpr1), newConstantExprs);
        }
    }

    @Test
    public void testAddFiltersToNewChildrenWithUnionWithConstantWithMergeProjectInvalid() {
        List<Plan> newChildren = Lists.newArrayList();
        List<List<SlotReference>> newRegularChildrenOutputs = Lists.newArrayList();
        List<List<NamedExpression>> newConstantExprs = new ArrayList<>();
        LogicalSetOperation setOperation = new LogicalUnion(Qualifier.ALL, outputs, regulatorChildrenOutputs, ImmutableList.of(), false, children);
        LogicalFilter<?> filter = new LogicalFilter<Plan>(Collections.emptySet(), setOperation);

        try (MockedStatic<EliminateFilter> mockedEliminateFilter = Mockito.mockStatic(EliminateFilter.class);
                MockedStatic<PlanUtils> mockedPlanUtils = Mockito.mockStatic(PlanUtils.class)) {
            mockedEliminateFilter
                    .when(() -> EliminateFilter.eliminateFilterOnOneRowRelation(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> {
                        LogicalFilter<?> input = invocation.getArgument(0);
                        LogicalPlan child = (LogicalPlan) input.child();
                        if (child.equals(child1)) {
                            return child;
                        }
                        if (child.equals(child2)) {
                            return new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
                        }
                        LogicalOneRowRelation relation = (LogicalOneRowRelation) child;
                        if (relation.getExpressions().equals(constantExpr1)) {
                            return relation;
                        }
                        if (relation.getExpressions().equals(constantExpr2)) {
                            return input;
                        }
                        return input;
                    });

            mockedPlanUtils
                    .when(() -> PlanUtils.tryMergeProjections(Mockito.any(), Mockito.any()))
                    .thenReturn(Optional.empty());

            PushDownFilterThroughSetOperation.addFiltersToNewChildren(
                    setOperation, filter, children, regulatorChildrenOutputs, null,
                    newChildren, newRegularChildrenOutputs, newConstantExprs,
                    (rowIndex, columnIndex) -> setOperation.getRegularChildOutput(rowIndex).get(columnIndex),
                    Function.identity());

            PushDownFilterThroughSetOperation.addFiltersToNewChildren(
                    setOperation, filter, constantExprs, regulatorChildrenOutputs, null,
                    newChildren, newRegularChildrenOutputs, newConstantExprs,
                    (rowIndex, columnIndex) -> setOperation.getRegularChildOutput(rowIndex).get(columnIndex),
                    selectConstants -> new LogicalOneRowRelation(
                            new RelationId(100), selectConstants)
            );

            Assertions.assertEquals(2, newChildren.size());
            Assertions.assertInstanceOf(LogicalOneRowRelation.class, newChildren.get(0));
            Assertions.assertInstanceOf(LogicalFilter.class, newChildren.get(1));
            Assertions.assertEquals(2, newRegularChildrenOutputs.size());
            Assertions.assertEquals(ImmutableList.of(regulatorChildOutputs1, constantExpr2), newRegularChildrenOutputs);
            Assertions.assertEquals(1, newConstantExprs.size());
            Assertions.assertEquals(ImmutableList.of(constantExpr1), newConstantExprs);
        }
    }

    @Test
    public void testAddFiltersToNewChildrenWithExcept() {
        List<Plan> newChildren = Lists.newArrayList();
        List<List<SlotReference>> newRegularChildrenOutputs = Lists.newArrayList();
        LogicalSetOperation setOperation = new LogicalExcept(Qualifier.ALL, outputs, regulatorChildrenOutputs, children);
        LogicalFilter<?> filter = new LogicalFilter<Plan>(Collections.emptySet(), setOperation);

        try (MockedStatic<EliminateFilter> mockedEliminateFilter = Mockito.mockStatic(EliminateFilter.class)) {
            mockedEliminateFilter
                    .when(() -> EliminateFilter.eliminateFilterOnOneRowRelation(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> invocation.getArgument(0));

            PushDownFilterThroughSetOperation.addFiltersToNewChildren(
                    setOperation, filter, children, regulatorChildrenOutputs, null,
                    newChildren, newRegularChildrenOutputs, null,
                    (rowIndex, columnIndex) -> setOperation.getRegularChildOutput(rowIndex).get(columnIndex),
                    Function.identity());

            Assertions.assertEquals(2, newChildren.size());
            Assertions.assertInstanceOf(LogicalFilter.class, newChildren.get(0));
            Assertions.assertInstanceOf(LogicalFilter.class, newChildren.get(1));
            Assertions.assertEquals(regulatorChildrenOutputs, newRegularChildrenOutputs);
        }
    }
}
