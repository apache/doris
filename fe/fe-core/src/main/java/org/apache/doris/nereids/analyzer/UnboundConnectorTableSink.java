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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generic unbound table sink for plugin-driven connector catalogs.
 * Used when the catalog is a PluginDrivenExternalCatalog.
 */
public class UnboundConnectorTableSink<CHILD_TYPE extends Plan> extends UnboundBaseExternalTableSink<CHILD_TYPE> {

    // Static partition spec from INSERT ... PARTITION(col=val); null when none. Mirrors
    // UnboundMaxComputeTableSink so plugin-driven MaxCompute keeps static-partition / overwrite
    // semantics after the cutover. Consumed via the PluginDrivenInsertCommandContext.
    private final Map<String, Expression> staticPartitionKeyValues;

    // Rewrite (compaction) marker. Mirrors UnboundIcebergTableSink.rewrite: carried through bind so the
    // neutral connector sink chain (Logical/Physical) can force single-node GATHER output for a
    // rewrite_data_files INSERT-SELECT (controls output file count). Defaults false; set true only by the
    // distributed rewrite coordinator. Always false for ordinary INSERT, so this is dormant pre-cutover.
    private final boolean rewrite;

    public UnboundConnectorTableSink(List<String> nameParts, List<String> colNames, List<String> hints,
                                     List<String> partitions, CHILD_TYPE child) {
        this(nameParts, colNames, hints, partitions, DMLCommandType.NONE,
                Optional.empty(), Optional.empty(), child, null);
    }

    public UnboundConnectorTableSink(List<String> nameParts,
                                     List<String> colNames,
                                     List<String> hints,
                                     List<String> partitions,
                                     DMLCommandType dmlCommandType,
                                     Optional<GroupExpression> groupExpression,
                                     Optional<LogicalProperties> logicalProperties,
                                     CHILD_TYPE child) {
        this(nameParts, colNames, hints, partitions, dmlCommandType,
                groupExpression, logicalProperties, child, null);
    }

    /**
     * constructor with static partition
     */
    public UnboundConnectorTableSink(List<String> nameParts,
                                     List<String> colNames,
                                     List<String> hints,
                                     List<String> partitions,
                                     DMLCommandType dmlCommandType,
                                     Optional<GroupExpression> groupExpression,
                                     Optional<LogicalProperties> logicalProperties,
                                     CHILD_TYPE child,
                                     Map<String, Expression> staticPartitionKeyValues) {
        this(nameParts, colNames, hints, partitions, dmlCommandType,
                groupExpression, logicalProperties, child, staticPartitionKeyValues, false);
    }

    /**
     * constructor with static partition and rewrite flag
     */
    public UnboundConnectorTableSink(List<String> nameParts,
                                     List<String> colNames,
                                     List<String> hints,
                                     List<String> partitions,
                                     DMLCommandType dmlCommandType,
                                     Optional<GroupExpression> groupExpression,
                                     Optional<LogicalProperties> logicalProperties,
                                     CHILD_TYPE child,
                                     Map<String, Expression> staticPartitionKeyValues,
                                     boolean rewrite) {
        super(nameParts, PlanType.LOGICAL_UNBOUND_CONNECTOR_TABLE_SINK, ImmutableList.of(), groupExpression,
                logicalProperties, colNames, dmlCommandType, child, hints, partitions);
        this.staticPartitionKeyValues = staticPartitionKeyValues != null
                ? ImmutableMap.copyOf(staticPartitionKeyValues)
                : null;
        this.rewrite = rewrite;
    }

    public Map<String, Expression> getStaticPartitionKeyValues() {
        return staticPartitionKeyValues;
    }

    public boolean isRewrite() {
        return rewrite;
    }

    public boolean hasStaticPartition() {
        return staticPartitionKeyValues != null && !staticPartitionKeyValues.isEmpty();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundConnectorTableSink(this, context);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "UnboundConnectorTableSink only accepts one child");
        return new UnboundConnectorTableSink<>(nameParts, colNames, hints, partitions,
            dmlCommandType, groupExpression, Optional.empty(), children.get(0), staticPartitionKeyValues, rewrite);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new UnboundConnectorTableSink<>(nameParts, colNames, hints, partitions,
            dmlCommandType, groupExpression, Optional.of(getLogicalProperties()), child(),
            staticPartitionKeyValues, rewrite);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new UnboundConnectorTableSink<>(nameParts, colNames, hints, partitions,
            dmlCommandType, groupExpression, logicalProperties, children.get(0), staticPartitionKeyValues, rewrite);
    }
}
