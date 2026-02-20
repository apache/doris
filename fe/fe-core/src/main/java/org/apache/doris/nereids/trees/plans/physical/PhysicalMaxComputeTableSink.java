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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalDatabase;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHiveTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.MustLocalSortOrderSpec;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** physical maxcompute table sink */
public class PhysicalMaxComputeTableSink<CHILD_TYPE extends Plan> extends PhysicalBaseExternalTableSink<CHILD_TYPE> {

    /**
     * constructor
     */
    public PhysicalMaxComputeTableSink(MaxComputeExternalDatabase database,
                                       MaxComputeExternalTable targetTable,
                                       List<Column> cols,
                                       List<NamedExpression> outputExprs,
                                       Optional<GroupExpression> groupExpression,
                                       LogicalProperties logicalProperties,
                                       CHILD_TYPE child) {
        this(database, targetTable, cols, outputExprs, groupExpression, logicalProperties,
                PhysicalProperties.GATHER, null, child);
    }

    /**
     * constructor
     */
    public PhysicalMaxComputeTableSink(MaxComputeExternalDatabase database,
                                       MaxComputeExternalTable targetTable,
                                       List<Column> cols,
                                       List<NamedExpression> outputExprs,
                                       Optional<GroupExpression> groupExpression,
                                       LogicalProperties logicalProperties,
                                       PhysicalProperties physicalProperties,
                                       Statistics statistics,
                                       CHILD_TYPE child) {
        super(PlanType.PHYSICAL_MAX_COMPUTE_TABLE_SINK, database, targetTable, cols, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalMaxComputeTableSink<>(
                (MaxComputeExternalDatabase) database, (MaxComputeExternalTable) targetTable,
                cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalMaxComputeTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalMaxComputeTableSink<>(
                (MaxComputeExternalDatabase) database, (MaxComputeExternalTable) targetTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalMaxComputeTableSink<>(
                (MaxComputeExternalDatabase) database, (MaxComputeExternalTable) targetTable, cols, outputExprs,
                groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalMaxComputeTableSink<>(
                (MaxComputeExternalDatabase) database, (MaxComputeExternalTable) targetTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        Set<String> partitionNames = ((MaxComputeExternalTable) targetTable).getPartitionColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toSet());
        if (!partitionNames.isEmpty()) {
            List<Integer> columnIdx = new ArrayList<>();
            List<Column> fullSchema = targetTable.getFullSchema();
            for (int i = 0; i < fullSchema.size(); i++) {
                Column column = fullSchema.get(i);
                if (partitionNames.contains(column.getName())) {
                    columnIdx.add(i);
                }
            }
            List<ExprId> exprIds = columnIdx.stream()
                    .map(idx -> child().getOutput().get(idx).getExprId())
                    .collect(Collectors.toList());
            DistributionSpecHiveTableSinkHashPartitioned shuffleInfo
                    = new DistributionSpecHiveTableSinkHashPartitioned();
            shuffleInfo.setOutputColExprIds(exprIds);
            // Require local sort by partition columns so that rows for the same partition
            // are grouped together. MaxCompute Storage API streams dynamic partition data
            // and will close a partition writer once it sees a different partition;
            // unsorted data causes "writer has been closed" errors.
            List<OrderKey> orderKeys = columnIdx.stream()
                    .map(idx -> new OrderKey(child().getOutput().get(idx), true, false))
                    .collect(Collectors.toList());
            return new PhysicalProperties(shuffleInfo)
                    .withOrderSpec(new MustLocalSortOrderSpec(orderKeys));
        }
        return PhysicalProperties.SINK_RANDOM_PARTITIONED;
    }
}
