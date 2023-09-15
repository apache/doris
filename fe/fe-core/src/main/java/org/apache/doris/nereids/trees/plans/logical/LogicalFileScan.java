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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Logical file scan for external catalog.
 */
public class LogicalFileScan extends LogicalCatalogRelation {

    // TODO remove this conjuncts when old planner is removed
    @Getter
    private final Set<Expression> conjuncts;
    @Getter
    private final SelectedPartitions selectedPartitions;

    /**
     * Constructor for LogicalFileScan.
     */
    public LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            Set<Expression> conjuncts, SelectedPartitions selectedPartitions) {
        super(id, PlanType.LOGICAL_FILE_SCAN, table, qualifier,
                groupExpression, logicalProperties);
        this.conjuncts = conjuncts;
        this.selectedPartitions = selectedPartitions;
    }

    public LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                Sets.newHashSet(), SelectedPartitions.EMPTY);
    }

    @Override
    public ExternalTable getTable() {
        Preconditions.checkArgument(table instanceof ExternalTable,
                "LogicalFileScan's table must be ExternalTable, but table is " + table.getClass().getSimpleName());
        return (ExternalTable) table;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalFileScan",
                "qualified", qualifiedName(),
                "output", getOutput()
        );
    }

    @Override
    public LogicalFileScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier, groupExpression,
                Optional.of(getLogicalProperties()), conjuncts, selectedPartitions);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                groupExpression, logicalProperties, conjuncts, selectedPartitions);
    }

    public LogicalFileScan withConjuncts(Set<Expression> conjuncts) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier, groupExpression,
                Optional.of(getLogicalProperties()), conjuncts, selectedPartitions);
    }

    public LogicalFileScan withSelectedPartitions(SelectedPartitions selectedPartitions) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier, groupExpression,
                Optional.of(getLogicalProperties()), conjuncts, selectedPartitions);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFileScan(this, context);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && Objects.equals(conjuncts, ((LogicalFileScan) o).conjuncts)
                && Objects.equals(selectedPartitions, ((LogicalFileScan) o).selectedPartitions);
    }

    /**
     * SelectedPartitions contains the selected partitions and the total partition number.
     * Mainly for hive table partition pruning.
     */
    public static class SelectedPartitions {
        public static SelectedPartitions EMPTY = new SelectedPartitions(0, Maps.newHashMap(), false);
        /**
         * total partition number
         */
        public long totalPartitionNum = 0;
        /**
         * partition id -> partition item
         */
        public Map<Long, PartitionItem> selectedPartitions;
        /**
         * true means the result is after partition pruning
         * false means the partition pruning is not processed.
         */
        public boolean isPartitionPruned;

        /**
         * Constructor for SelectedPartitions.
         */
        public SelectedPartitions(long totalPartitionNum, Map<Long, PartitionItem> selectedPartitions,
                boolean isPartitionPruned) {
            this.totalPartitionNum = totalPartitionNum;
            this.selectedPartitions = selectedPartitions;
            this.isPartitionPruned = isPartitionPruned;
            if (this.selectedPartitions == null) {
                this.selectedPartitions = Maps.newHashMap();
            }
        }

        @Override
        public boolean equals(Object o) {
            return isPartitionPruned == ((SelectedPartitions) o).isPartitionPruned && Objects.equals(
                    selectedPartitions.keySet(), ((SelectedPartitions) o).selectedPartitions.keySet());
        }
    }
}

