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
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHiveTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.MustLocalSortOrderSpec;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Physical table sink for plugin-driven connector catalogs.
 */
public class PhysicalConnectorTableSink<CHILD_TYPE extends Plan> extends PhysicalBaseExternalTableSink<CHILD_TYPE> {

    // Rewrite (compaction) marker, threaded from LogicalConnectorTableSink.isRewrite. When set,
    // getRequirePhysicalProperties() short-circuits to GATHER (single writer) so a rewrite_data_files
    // INSERT-SELECT controls its output file count even on a partitioned table — the override must win
    // over the partition-shuffle / parallel-write arms below. Carried as a sink field (no ConnectContext,
    // no instanceof Iceberg). Defaults false → behavior is byte-identical for ordinary connector writes.
    private final boolean isRewrite;

    /**
     * constructor
     */
    public PhysicalConnectorTableSink(ExternalDatabase database,
                                      ExternalTable targetTable,
                                      List<Column> cols,
                                      List<NamedExpression> outputExprs,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties,
                                      boolean isRewrite,
                                      CHILD_TYPE child) {
        this(database, targetTable, cols, outputExprs, groupExpression, logicalProperties,
                PhysicalProperties.GATHER, null, isRewrite, child);
    }

    /**
     * constructor
     */
    public PhysicalConnectorTableSink(ExternalDatabase database,
                                      ExternalTable targetTable,
                                      List<Column> cols,
                                      List<NamedExpression> outputExprs,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties,
                                      PhysicalProperties physicalProperties,
                                      Statistics statistics,
                                      boolean isRewrite,
                                      CHILD_TYPE child) {
        super(PlanType.PHYSICAL_CONNECTOR_TABLE_SINK, database, targetTable, cols, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.isRewrite = isRewrite;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalConnectorTableSink<>(
                (ExternalDatabase) database, (ExternalTable) targetTable, cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, isRewrite, children.get(0)));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalConnectorTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalConnectorTableSink<>(
                (ExternalDatabase) database, (ExternalTable) targetTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), isRewrite, child()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalConnectorTableSink<>(
                (ExternalDatabase) database, (ExternalTable) targetTable, cols, outputExprs,
                groupExpression, logicalProperties.get(), isRewrite, children.get(0)));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalConnectorTableSink<>(
                (ExternalDatabase) database, (ExternalTable) targetTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, isRewrite, child()));
    }

    /**
     * Get required physical properties for sink distribution. Generalizes the legacy
     * {@code PhysicalMaxComputeTableSink.getRequirePhysicalProperties()} 3-branch behavior, gated
     * by connector capabilities so non-partitioned connectors (JDBC, ES) keep the GATHER default:
     *
     * <ul>
     *   <li><b>Dynamic-partition write</b> (a partition column is present in {@code cols}) when the
     *       connector declares {@code SINK_REQUIRE_PARTITION_LOCAL_SORT}: hash-distribute by the
     *       partition columns and require a mandatory local sort on them. Streaming partition
     *       writers (MaxCompute Storage API) close the previous partition writer once a different
     *       partition value appears; un-grouped rows cause "writer has been closed".</li>
     *   <li><b>Non-partitioned / all-static-partition write</b> when the connector declares
     *       {@code SUPPORTS_PARALLEL_WRITE}: {@code SINK_RANDOM_PARTITIONED} (parallel writers).</li>
     *   <li><b>Otherwise</b> (e.g. JDBC, ES): {@code GATHER} (single writer) for transactional
     *       safety.</li>
     * </ul>
     *
     * <p><b>Index by full schema, not {@code cols}.</b> For a positional-write connector (one declaring
     * {@code SINK_REQUIRE_FULL_SCHEMA_ORDER}, e.g. MaxCompute), {@code BindSink.bindConnectorTableSink}
     * projects the child to <em>full-schema</em> order (any unmentioned / static-partition columns filled
     * in), exactly like legacy {@code bindMaxComputeTableSink},
     * because the BE writer strips the trailing partition columns by position. So {@code child().getOutput()}
     * is aligned 1:1 with {@code targetTable.getFullSchema()}, while {@code cols} excludes the static
     * partition columns and may be in a different (user-specified) order. Partition columns are therefore
     * located by their position in the full schema. (An earlier revision indexed by {@code cols}, which
     * mislocated the dynamic column whenever {@code cols} order diverged from the full schema — the
     * partial-static {@code PARTITION(p1='x') SELECT ..., p2} and reordered-explicit-list cases.)</p>
     */
    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        // Rewrite (compaction) writes must gather to a single writer to control the output file count;
        // this neutral flag wins over the partition-shuffle / parallel-write arms below. Mirrors
        // PhysicalIcebergTableSink's isUseGatherForIcebergRewrite short-circuit, but carried as a sink
        // field (no ConnectContext access, no instanceof Iceberg).
        if (isRewrite) {
            return PhysicalProperties.GATHER;
        }
        if (!(targetTable instanceof PluginDrivenExternalTable)) {
            return PhysicalProperties.GATHER;
        }
        PluginDrivenExternalTable table = (PluginDrivenExternalTable) targetTable;

        if (table.requirePartitionLocalSortOnWrite()) {
            Set<String> partitionNames = table.getPartitionColumns().stream()
                    .map(Column::getName)
                    .collect(Collectors.toSet());
            if (!partitionNames.isEmpty()) {
                // A partition column present in cols == its value comes from the query == a
                // dynamic-partition write (static partition cols are excluded from cols by
                // BindSink.bindConnectorTableSink). If any remains, this is a dynamic / partial-static
                // write that must be hash-distributed and locally sorted by partition columns.
                Set<String> colNames = cols.stream()
                        .map(Column::getName)
                        .collect(Collectors.toSet());
                boolean hasDynamicPartition = partitionNames.stream().anyMatch(colNames::contains);
                if (hasDynamicPartition) {
                    // Index by FULL-SCHEMA position, NOT cols. For a static / partial-static write the
                    // bind layer projects the child to full schema (static partition cols filled), so
                    // child().getOutput() is aligned 1:1 with the full schema while cols excludes the
                    // static partition cols. Indexing by full-schema position is required to hash/sort
                    // by the correct (dynamic) column in the partial-static case. Mirrors legacy
                    // PhysicalMaxComputeTableSink.
                    List<Integer> columnIdx = new ArrayList<>();
                    List<Column> fullSchema = targetTable.getFullSchema();
                    for (int i = 0; i < fullSchema.size(); i++) {
                        if (partitionNames.contains(fullSchema.get(i).getName())) {
                            columnIdx.add(i);
                        }
                    }
                    List<ExprId> exprIds = columnIdx.stream()
                            .map(idx -> child().getOutput().get(idx).getExprId())
                            .collect(Collectors.toList());
                    DistributionSpecHiveTableSinkHashPartitioned shuffleInfo
                            = new DistributionSpecHiveTableSinkHashPartitioned();
                    shuffleInfo.setOutputColExprIds(exprIds);
                    // Local sort by partition columns so rows for the same partition are grouped
                    // together before the streaming partition writer (MaxCompute Storage API closes a
                    // partition writer once a different partition value appears).
                    List<OrderKey> orderKeys = columnIdx.stream()
                            .map(idx -> new OrderKey(child().getOutput().get(idx), true, false))
                            .collect(Collectors.toList());
                    return new PhysicalProperties(shuffleInfo)
                            .withOrderSpec(new MustLocalSortOrderSpec(orderKeys));
                }
                // Partition columns exist but none in cols == all partitions statically specified;
                // fall through to the parallel/gather branch (no sort/shuffle needed).
            }
        }

        if (table.supportsParallelWrite()) {
            return PhysicalProperties.SINK_RANDOM_PARTITIONED;
        }
        return PhysicalProperties.GATHER;
    }
}
