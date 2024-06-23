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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalInlineTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSqlCache;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSqlCache;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;

/**
 * Base class for the processing of logical and physical plan.
 *
 * @param <R> Return type of each visit method.
 * @param <C> Context type.
 */
public abstract class PlanVisitor<R, C> implements CommandVisitor<R, C>, RelationVisitor<R, C>, SinkVisitor<R, C> {

    public abstract R visit(Plan plan, C context);

    // *******************************
    // commands
    // *******************************

    @Override
    public R visitCommand(Command command, C context) {
        return visit(command, context);
    }

    // *******************************
    // relations
    // *******************************

    @Override
    public R visitLogicalRelation(LogicalRelation relation, C context) {
        return visit(relation, context);
    }

    @Override
    public R visitPhysicalRelation(PhysicalRelation physicalRelation, C context) {
        return visit(physicalRelation, context);
    }

    // *******************************
    // sinks
    // *******************************

    @Override
    public R visitLogicalSink(LogicalSink<? extends Plan> logicalSink, C context) {
        return visit(logicalSink, context);
    }

    @Override
    public R visitPhysicalSink(PhysicalSink<? extends Plan> physicalSink, C context) {
        return visit(physicalSink, context);
    }

    // *******************************
    // Logical plans
    // *******************************
    public R visitLogicalSqlCache(LogicalSqlCache sqlCache, C context) {
        return visit(sqlCache, context);
    }

    public R visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, C context) {
        return visit(aggregate, context);
    }

    public R visitLogicalApply(LogicalApply<? extends Plan, ? extends Plan> apply, C context) {
        return visit(apply, context);
    }

    public R visitLogicalAssertNumRows(LogicalAssertNumRows<? extends Plan> assertNumRows, C context) {
        return visit(assertNumRows, context);
    }

    public R visitLogicalCheckPolicy(LogicalCheckPolicy<? extends Plan> checkPolicy, C context) {
        return visit(checkPolicy, context);
    }

    public R visitLogicalCTE(LogicalCTE<? extends Plan> cte, C context) {
        return visit(cte, context);
    }

    public R visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor, C context) {
        return visit(cteAnchor, context);
    }

    public R visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, C context) {
        return visit(cteConsumer, context);
    }

    public R visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer, C context) {
        return visit(cteProducer, context);
    }

    public R visitLogicalFilter(LogicalFilter<? extends Plan> filter, C context) {
        return visit(filter, context);
    }

    public R visitLogicalGenerate(LogicalGenerate<? extends Plan> generate, C context) {
        return visit(generate, context);
    }

    public R visitGroupPlan(GroupPlan groupPlan, C context) {
        return visit(groupPlan, context);
    }

    public R visitLogicalHaving(LogicalHaving<? extends Plan> having, C context) {
        return visit(having, context);
    }

    public R visitLogicalInlineTable(LogicalInlineTable logicalInlineTable, C context) {
        return visit(logicalInlineTable, context);
    }

    public R visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, C context) {
        return visit(join, context);
    }

    public R visitLogicalLimit(LogicalLimit<? extends Plan> limit, C context) {
        return visit(limit, context);
    }

    public R visitLogicalPartitionTopN(LogicalPartitionTopN<? extends Plan> partitionTopN, C context) {
        return visit(partitionTopN, context);
    }

    public R visitLogicalProject(LogicalProject<? extends Plan> project, C context) {
        return visit(project, context);
    }

    public R visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, C context) {
        return visit(repeat, context);
    }

    public R visitLogicalSelectHint(LogicalSelectHint<? extends Plan> hint, C context) {
        return visit(hint, context);
    }

    public R visitLogicalSetOperation(LogicalSetOperation setOperation, C context) {
        return visit(setOperation, context);
    }

    public R visitLogicalExcept(LogicalExcept except, C context) {
        return visitLogicalSetOperation(except, context);
    }

    public R visitLogicalIntersect(LogicalIntersect intersect, C context) {
        return visitLogicalSetOperation(intersect, context);
    }

    public R visitLogicalUnion(LogicalUnion union, C context) {
        return visitLogicalSetOperation(union, context);
    }

    public R visitLogicalSort(LogicalSort<? extends Plan> sort, C context) {
        return visit(sort, context);
    }

    public R visitLogicalSubQueryAlias(LogicalSubQueryAlias<? extends Plan> alias, C context) {
        return visit(alias, context);
    }

    public R visitLogicalView(LogicalView<? extends Plan> alias, C context) {
        return visit(alias, context);
    }

    public R visitLogicalTopN(LogicalTopN<? extends Plan> topN, C context) {
        return visit(topN, context);
    }

    public R visitLogicalDeferMaterializeTopN(LogicalDeferMaterializeTopN<? extends Plan> topN, C context) {
        return visit(topN, context);
    }

    public R visitLogicalWindow(LogicalWindow<? extends Plan> window, C context) {
        return visit(window, context);
    }

    // *******************************
    // Physical plans
    // *******************************
    public R visitPhysicalSqlCache(PhysicalSqlCache sqlCache, C context) {
        return visit(sqlCache, context);
    }

    public R visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> agg, C context) {
        return visit(agg, context);
    }

    public R visitPhysicalStorageLayerAggregate(PhysicalStorageLayerAggregate storageLayerAggregate, C context) {
        return storageLayerAggregate.getRelation().accept(this, context);
    }

    public R visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows, C context) {
        return visit(assertNumRows, context);
    }

    public R visitPhysicalCTEAnchor(
            PhysicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor, C context) {
        return visit(cteAnchor, context);
    }

    public R visitPhysicalCTEProducer(PhysicalCTEProducer<? extends Plan> cteProducer, C context) {
        return visit(cteProducer, context);
    }

    public R visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, C context) {
        return visit(filter, context);
    }

    public R visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, C context) {
        return visit(generate, context);
    }

    public R visitAbstractPhysicalJoin(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join, C context) {
        return visit(join, context);
    }

    public R visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin, C context) {
        return visitAbstractPhysicalJoin(hashJoin, context);
    }

    public R visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin, C context) {
        return visitAbstractPhysicalJoin(nestedLoopJoin, context);
    }

    public R visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, C context) {
        return visit(limit, context);
    }

    public R visitPhysicalPartitionTopN(PhysicalPartitionTopN<? extends Plan> partitionTopN, C context) {
        return visit(partitionTopN, context);
    }

    public R visitPhysicalProject(PhysicalProject<? extends Plan> project, C context) {
        return visit(project, context);
    }

    public R visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, C context) {
        return visit(repeat, context);
    }

    public R visitPhysicalSetOperation(PhysicalSetOperation setOperation, C context) {
        return visit(setOperation, context);
    }

    public R visitPhysicalExcept(PhysicalExcept except, C context) {
        return visitPhysicalSetOperation(except, context);
    }

    public R visitPhysicalIntersect(PhysicalIntersect intersect, C context) {
        return visitPhysicalSetOperation(intersect, context);
    }

    public R visitPhysicalUnion(PhysicalUnion union, C context) {
        return visitPhysicalSetOperation(union, context);
    }

    public R visitAbstractPhysicalSort(AbstractPhysicalSort<? extends Plan> sort, C context) {
        return visit(sort, context);
    }

    public R visitPhysicalQuickSort(PhysicalQuickSort<? extends Plan> sort, C context) {
        return visitAbstractPhysicalSort(sort, context);
    }

    public R visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, C context) {
        return visitAbstractPhysicalSort(topN, context);
    }

    public R visitPhysicalDeferMaterializeTopN(PhysicalDeferMaterializeTopN<? extends Plan> topN, C context) {
        return visitAbstractPhysicalSort(topN, context);
    }

    public R visitPhysicalWindow(PhysicalWindow<? extends Plan> window, C context) {
        return visit(window, context);
    }

    // *******************************
    // Physical enforcer
    // *******************************

    public R visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute, C context) {
        return visit(distribute, context);
    }
}
