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
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** abstract physical hive sink */
public class PhysicalHiveTableSink<CHILD_TYPE extends Plan> extends PhysicalSink<CHILD_TYPE> implements Sink {

    private final HMSExternalDatabase database;
    private final HMSExternalTable targetTable;
    private final List<Column> cols;
    private final Set<String> hivePartitionKeys;

    /**
     * constructor
     */
    public PhysicalHiveTableSink(HMSExternalDatabase database,
                                 HMSExternalTable targetTable,
                                 List<Column> cols,
                                 List<NamedExpression> outputExprs,
                                 Optional<GroupExpression> groupExpression,
                                 LogicalProperties logicalProperties,
                                 CHILD_TYPE child,
                                 Set<String> hivePartitionKeys) {
        this(database, targetTable, cols, outputExprs, groupExpression, logicalProperties,
                PhysicalProperties.GATHER, null, child, hivePartitionKeys);
    }

    /**
     * constructor
     */
    public PhysicalHiveTableSink(HMSExternalDatabase database,
                                 HMSExternalTable targetTable,
                                 List<Column> cols,
                                 List<NamedExpression> outputExprs,
                                 Optional<GroupExpression> groupExpression,
                                 LogicalProperties logicalProperties,
                                 PhysicalProperties physicalProperties,
                                 Statistics statistics,
                                 CHILD_TYPE child,
                                 Set<String> hivePartitionKeys) {
        super(PlanType.PHYSICAL_HIVE_TABLE_SINK, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.database = Objects.requireNonNull(database, "database != null in PhysicalHiveTableSink");
        this.targetTable = Objects.requireNonNull(targetTable, "targetTable != null in PhysicalHiveTableSink");
        this.cols = Utils.copyRequiredList(cols);
        this.hivePartitionKeys = hivePartitionKeys;
    }

    public HMSExternalDatabase getDatabase() {
        return database;
    }

    public HMSExternalTable getTargetTable() {
        return targetTable;
    }

    public List<Column> getCols() {
        return cols;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalHiveTableSink<>(database, targetTable, cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0), hivePartitionKeys);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalHiveTableSink(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalHiveTableSink<>(database, targetTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), child(), hivePartitionKeys);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalHiveTableSink<>(database, targetTable, cols, outputExprs,
                groupExpression, logicalProperties.get(), children.get(0), hivePartitionKeys);
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalHiveTableSink<>(database, targetTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child(), hivePartitionKeys);
    }

    /**
     * get output physical properties
     */
    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        Set<String> hivePartitionKeys = targetTable.getRemoteTable()
                .getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(Collectors.toSet());
        if (!hivePartitionKeys.isEmpty()) {
            List<Integer> columnIdx = new ArrayList<>();
            List<Column> fullSchema = targetTable.getFullSchema();
            for (int i = 0; i < fullSchema.size(); i++) {
                Column column = fullSchema.get(i);
                if (hivePartitionKeys.contains(column.getName())) {
                    columnIdx.add(i);
                }
            }
            DistributionSpecTableSinkHashPartitioned shuffleInfo =
                    (DistributionSpecTableSinkHashPartitioned) PhysicalProperties.SINK_HASH_PARTITIONED
                            .getDistributionSpec();
            List<ExprId> exprIds = columnIdx.stream()
                    .map(idx -> child().getOutput().get(idx).getExprId())
                    .collect(Collectors.toList());
            shuffleInfo.setOutputColExprIds(exprIds);
            return PhysicalProperties.SINK_HASH_PARTITIONED;
        }
        return PhysicalProperties.SINK_RANDOM_PARTITIONED;
    }
}
