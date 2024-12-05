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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Logical scan for external JDBC catalog and JDBC table.
 */
public class LogicalJdbcScan extends LogicalCatalogRelation {

    private final Map<String, Set<List<String>>> colToSubPathsMap;
    private final Map<Slot, Map<List<String>, SlotReference>> subPathToSlotMap;
    private final Map<Pair<Long, String>, Slot> cacheSlotWithSlotName = Maps.newHashMap();

    /**
     * Constructor for LogicalJdbcScan with full parameters.
     */
    public LogicalJdbcScan(RelationId id, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            Map<String, Set<List<String>>> colToSubPathsMap) {
        super(id, PlanType.LOGICAL_JDBC_SCAN, table, qualifier, groupExpression, logicalProperties);
        this.colToSubPathsMap = colToSubPathsMap;
        this.subPathToSlotMap = Maps.newHashMap();
    }

    /**
     * Constructor for LogicalJdbcScan with minimum parameters.
     */
    public LogicalJdbcScan(RelationId id, TableIf table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(), ImmutableMap.of());
    }

    @Override
    public TableIf getTable() {
        Preconditions.checkArgument(
                table instanceof ExternalTable || table instanceof JdbcTable,
                String.format("Table %s is neither ExternalTable nor JdbcTable", table.getName())
        );
        return table;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalJdbcScan",
                "qualified", qualifiedName(),
                "output", getOutput()
        );
    }

    /**
     * Get the mapping of sub-paths to corresponding slot references.
     */
    public Map<Slot, Map<List<String>, SlotReference>> getSubPathToSlotMap() {
        this.getOutput();
        return subPathToSlotMap;
    }

    @Override
    public List<Slot> computeOutput() {
        List<Column> baseSchema = table.getBaseSchema(true);
        List<SlotReference> slotFromColumn = createSlotsVectorized(baseSchema);

        ImmutableList.Builder<Slot> slots = ImmutableList.builder();
        for (int i = 0; i < baseSchema.size(); i++) {
            final int index = i;
            Column col = baseSchema.get(i);
            Pair<Long, String> key = Pair.of(0L, col.getName());
            Slot slot = cacheSlotWithSlotName.computeIfAbsent(key, k -> slotFromColumn.get(index));
            slots.add(slot);
            if (colToSubPathsMap.containsKey(key.getValue())) {
                for (List<String> subPath : colToSubPathsMap.get(key.getValue())) {
                    if (!subPath.isEmpty()) {
                        SlotReference slotReference = SlotReference.fromColumn(
                                table, baseSchema.get(i), qualified()).withSubPath(subPath);
                        slots.add(slotReference);
                        subPathToSlotMap.computeIfAbsent(slot, k -> Maps.newHashMap())
                                .put(subPath, slotReference);
                    }

                }
            }
        }
        return slots.build();
    }

    private List<SlotReference> createSlotsVectorized(List<Column> columns) {
        List<String> qualified = qualified();
        SlotReference[] slots = new SlotReference[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            slots[i] = SlotReference.fromColumn(table, columns.get(i), qualified);
        }
        return Arrays.asList(slots);
    }

    /**
     * Create a new LogicalJdbcScan with updated column-to-sub-paths mapping.
     */
    public LogicalJdbcScan withColToSubPathsMap(Map<String, Set<List<String>>> colToSubPathsMaps) {
        return new LogicalJdbcScan(
                this.relationId,
                this.table,
                this.qualifier,
                this.groupExpression,
                Optional.empty(),
                colToSubPathsMaps
        );
    }

    @Override
    public LogicalJdbcScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalJdbcScan(
                this.relationId,
                this.table,
                this.qualifier,
                groupExpression,
                Optional.of(this.getLogicalProperties()),
                this.colToSubPathsMap
        );
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalJdbcScan(
                this.relationId,
                this.table,
                this.qualifier,
                groupExpression,
                logicalProperties,
                this.colToSubPathsMap
        );
    }

    @Override
    public LogicalJdbcScan withRelationId(RelationId relationId) {
        return new LogicalJdbcScan(
                relationId,
                this.table,
                this.qualifier,
                Optional.empty(),
                Optional.empty(),
                this.colToSubPathsMap
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalJdbcScan(this, context);
    }
}
