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
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Physical Paimon table sink.
 */
public class PhysicalPaimonTableSink<CHILD_TYPE extends Plan>
        extends PhysicalBaseExternalTableSink<CHILD_TYPE> {

    public PhysicalPaimonTableSink(PaimonExternalDatabase database,
                                    PaimonExternalTable targetTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    CHILD_TYPE child) {
        this(database, targetTable, cols, outputExprs, groupExpression, logicalProperties,
                PhysicalProperties.SINK_RANDOM_PARTITIONED, null, child);
    }

    public PhysicalPaimonTableSink(PaimonExternalDatabase database,
                                    PaimonExternalTable targetTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    PhysicalProperties physicalProperties,
                                    Statistics statistics,
                                    CHILD_TYPE child) {
        super(PlanType.PHYSICAL_PAIMON_TABLE_SINK, database, targetTable, cols, outputExprs,
                groupExpression, logicalProperties, physicalProperties, statistics, child);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, (PaimonExternalTable) targetTable,
                cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0)));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, (PaimonExternalTable) targetTable,
                cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, child()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, (PaimonExternalTable) targetTable,
                cols, outputExprs, groupExpression,
                logicalProperties.get(), physicalProperties, statistics, children.get(0)));
    }

    @Override
    public PhysicalPaimonTableSink<Plan> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics stats) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, (PaimonExternalTable) targetTable,
                cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, stats, child()));
    }

    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        List<String> primaryKeys = ((PaimonExternalTable) targetTable)
                .getPaimonTable(Optional.empty()).primaryKeys();
        if (primaryKeys.isEmpty()) {
            return PhysicalProperties.SINK_RANDOM_PARTITIONED;
        }

        List<Slot> outputSlots = child().getOutput();
        Preconditions.checkState(cols.size() == outputSlots.size(),
                "Paimon sink columns must match child output");
        Map<String, ExprId> columnExprIds = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < cols.size(); i++) {
            columnExprIds.put(cols.get(i).getName(), outputSlots.get(i).getExprId());
        }

        List<ExprId> primaryKeyExprIds = new ArrayList<>(primaryKeys.size());
        for (String primaryKey : primaryKeys) {
            primaryKeyExprIds.add(Preconditions.checkNotNull(
                    columnExprIds.get(primaryKey),
                    "Paimon primary-key column is missing from sink output"));
        }
        return PhysicalProperties.createHash(primaryKeyExprIds, ShuffleType.REQUIRE);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalPaimonTableSink(this, context);
    }
}
