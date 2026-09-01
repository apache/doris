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
import org.apache.doris.common.Config;
import org.apache.doris.connector.spi.write.ConnectorWriteDistribution;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecExternalTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecHiveTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.DistributionSpecPaimonTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.MustLocalSortOrderSpec;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Physical table sink for plugin-driven connector catalogs.
 */
public class PhysicalConnectorTableSink<CHILD_TYPE extends Plan> extends PhysicalBaseExternalTableSink<CHILD_TYPE> {

    private final List<Column> boundTargetSchema;
    private final List<Column> boundPartitionColumns;
    private final String boundWriteMetadataIdentity;

    // Rewrite (compaction) marker, threaded from LogicalConnectorTableSink.isRewrite. When set,
    // getRequirePhysicalProperties() short-circuits to GATHER (single writer) so a rewrite_data_files
    // INSERT-SELECT controls its output file count even on a partitioned table — the override must win
    // over the partition-shuffle / parallel-write arms below. Carried as a sink field (no ConnectContext,
    // no instanceof Iceberg). Defaults false → behavior is byte-identical for ordinary connector writes.
    private final boolean isRewrite;
    private final DMLCommandType dmlCommandType;

    /**
     * constructor
     */
    public PhysicalConnectorTableSink(ExternalDatabase database,
                                      ExternalTable targetTable,
                                      List<Column> boundTargetSchema,
                                      List<Column> cols,
                                      List<NamedExpression> outputExprs,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties,
                                      boolean isRewrite,
                                      CHILD_TYPE child) {
        this(database, targetTable, boundTargetSchema, ImmutableList.of(), cols, outputExprs,
                groupExpression, logicalProperties, isRewrite, child);
    }

    public PhysicalConnectorTableSink(ExternalDatabase database,
                                      ExternalTable targetTable,
                                      List<Column> boundTargetSchema,
                                      List<Column> boundPartitionColumns,
                                      List<Column> cols,
                                      List<NamedExpression> outputExprs,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties,
                                      boolean isRewrite,
                                      CHILD_TYPE child) {
        this(database, targetTable, boundTargetSchema, boundPartitionColumns, null, cols, outputExprs,
                groupExpression, logicalProperties, isRewrite, child);
    }

    /** Builds a physical sink with the write generation captured during sink binding. */
    public PhysicalConnectorTableSink(ExternalDatabase database,
                                      ExternalTable targetTable,
                                      List<Column> boundTargetSchema,
                                      List<Column> boundPartitionColumns,
                                      String boundWriteMetadataIdentity,
                                      List<Column> cols,
                                      List<NamedExpression> outputExprs,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties,
                                      boolean isRewrite,
                                      CHILD_TYPE child) {
        this(database, targetTable, boundTargetSchema, boundPartitionColumns, boundWriteMetadataIdentity,
                cols, outputExprs, groupExpression, logicalProperties,
                PhysicalProperties.GATHER, null, isRewrite, child);
    }

    /**
     * constructor
     */
    public PhysicalConnectorTableSink(ExternalDatabase database,
                                      ExternalTable targetTable,
                                      List<Column> boundTargetSchema,
                                      List<Column> cols,
                                      List<NamedExpression> outputExprs,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties,
                                      PhysicalProperties physicalProperties,
                                      Statistics statistics,
                                      boolean isRewrite,
                                      CHILD_TYPE child) {
        this(database, targetTable, boundTargetSchema, ImmutableList.of(), cols, outputExprs,
                groupExpression, logicalProperties, physicalProperties, statistics, isRewrite, child);
    }

    public PhysicalConnectorTableSink(ExternalDatabase database,
                                      ExternalTable targetTable,
                                      List<Column> boundTargetSchema,
                                      List<Column> boundPartitionColumns,
                                      List<Column> cols,
                                      List<NamedExpression> outputExprs,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties,
                                      PhysicalProperties physicalProperties,
                                      Statistics statistics,
                                      boolean isRewrite,
                                      CHILD_TYPE child) {
        this(database, targetTable, boundTargetSchema, boundPartitionColumns, null, cols, outputExprs,
                groupExpression, logicalProperties, physicalProperties, statistics, isRewrite, child);
    }

    /** Builds a physical sink with the write generation captured during sink binding. */
    public PhysicalConnectorTableSink(ExternalDatabase database,
                                      ExternalTable targetTable,
                                      List<Column> boundTargetSchema,
                                      List<Column> boundPartitionColumns,
                                      String boundWriteMetadataIdentity,
                                      List<Column> cols,
                                      List<NamedExpression> outputExprs,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties,
                                      PhysicalProperties physicalProperties,
                                      Statistics statistics,
                                      boolean isRewrite,
                                      CHILD_TYPE child) {
        this(database, targetTable, boundTargetSchema, boundPartitionColumns,
                boundWriteMetadataIdentity, cols, outputExprs, groupExpression, logicalProperties,
                physicalProperties, statistics, isRewrite, DMLCommandType.NONE, child);
    }

    /** Builds a physical connector sink carrying its row-level DML operation. */
    public PhysicalConnectorTableSink(ExternalDatabase database,
                                      ExternalTable targetTable,
                                      List<Column> boundTargetSchema,
                                      List<Column> boundPartitionColumns,
                                      String boundWriteMetadataIdentity,
                                      List<Column> cols,
                                      List<NamedExpression> outputExprs,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties,
                                      PhysicalProperties physicalProperties,
                                      Statistics statistics,
                                      boolean isRewrite,
                                      DMLCommandType dmlCommandType,
                                      CHILD_TYPE child) {
        super(PlanType.PHYSICAL_CONNECTOR_TABLE_SINK, database, targetTable, cols, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.boundTargetSchema = ImmutableList.copyOf(boundTargetSchema);
        this.boundPartitionColumns = ImmutableList.copyOf(boundPartitionColumns);
        this.boundWriteMetadataIdentity = boundWriteMetadataIdentity;
        this.isRewrite = isRewrite;
        this.dmlCommandType = dmlCommandType;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalConnectorTableSink<>(
                (ExternalDatabase) database, (ExternalTable) targetTable, boundTargetSchema,
                boundPartitionColumns, boundWriteMetadataIdentity, cols,
                outputExprs, groupExpression, getLogicalProperties(), physicalProperties, statistics,
                isRewrite, dmlCommandType, children.get(0)));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalConnectorTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalConnectorTableSink<>(
                (ExternalDatabase) database, (ExternalTable) targetTable, boundTargetSchema, boundPartitionColumns,
                boundWriteMetadataIdentity, cols,
                outputExprs, groupExpression, getLogicalProperties(), PhysicalProperties.GATHER, null,
                isRewrite, dmlCommandType, child()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalConnectorTableSink<>(
                (ExternalDatabase) database, (ExternalTable) targetTable, boundTargetSchema, boundPartitionColumns,
                boundWriteMetadataIdentity, cols,
                outputExprs, groupExpression, logicalProperties.get(), PhysicalProperties.GATHER, null,
                isRewrite, dmlCommandType, children.get(0)));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalConnectorTableSink<>(
                (ExternalDatabase) database, (ExternalTable) targetTable, boundTargetSchema, boundPartitionColumns,
                boundWriteMetadataIdentity, cols,
                outputExprs, groupExpression, getLogicalProperties(), physicalProperties, statistics,
                isRewrite, dmlCommandType, child()));
    }

    public List<Column> getBoundTargetSchema() {
        return boundTargetSchema;
    }

    public List<Column> getBoundPartitionColumns() {
        return boundPartitionColumns;
    }

    public String getBoundWriteMetadataIdentity() {
        return boundWriteMetadataIdentity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PhysicalConnectorTableSink<?> that = (PhysicalConnectorTableSink<?>) o;
        return isRewrite == that.isRewrite
                && dmlCommandType == that.dmlCommandType
                && Objects.equals(boundTargetSchema, that.boundTargetSchema)
                && Objects.equals(boundPartitionColumns, that.boundPartitionColumns)
                && Objects.equals(boundWriteMetadataIdentity, that.boundWriteMetadataIdentity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), boundTargetSchema, boundPartitionColumns,
                boundWriteMetadataIdentity, isRewrite, dmlCommandType);
    }

    /**
     * Whether this sink is a distributed {@code rewrite_data_files} (compaction) write. The neutral
     * translator threads {@code WriteOperation.REWRITE} onto the connector write handle when set, and
     * {@link #getRequirePhysicalProperties} short-circuits to GATHER.
     */
    public boolean isRewrite() {
        return isRewrite;
    }

    public DMLCommandType getDmlCommandType() {
        return dmlCommandType;
    }

    /**
     * Get required physical properties for sink distribution. Generalizes the legacy
     * {@code PhysicalMaxComputeTableSink.getRequirePhysicalProperties()} 3-branch behavior, gated
     * by connector capabilities so non-partitioned connectors (JDBC, ES) keep the GATHER default:
     *
     * <ul>
     *   <li><b>Dynamic-partition write</b> (a partition column is present in {@code cols}) when the
     *       connector's write provider returns {@code true} from {@code requiresPartitionLocalSort()}:
     *       hash-distribute by the partition columns and require a mandatory local sort on them.
     *       Streaming partition writers (MaxCompute Storage API) close the previous partition writer
     *       once a different partition value appears; un-grouped rows cause "writer has been closed".</li>
     *   <li><b>Non-partitioned / all-static-partition write</b> when the connector's write provider
     *       returns {@code true} from {@code requiresParallelWrite()}: {@code SINK_RANDOM_PARTITIONED}
     *       (parallel writers).</li>
     *   <li><b>Otherwise</b> (e.g. JDBC, ES): {@code GATHER} (single writer) for transactional
     *       safety.</li>
     * </ul>
     *
     * <p><b>Index by full schema, not {@code cols}.</b> For a positional-write connector (one whose write
     * provider returns {@code true} from {@code requiresFullSchemaWriteOrder()}, e.g. MaxCompute),
     * {@code BindSink.bindConnectorTableSink} projects the child to <em>full-schema</em> order (any
     * unmentioned / static-partition columns filled in), exactly like legacy {@code bindMaxComputeTableSink},
     * because the BE writer strips the trailing partition columns by position. So {@code child().getOutput()}
     * is aligned 1:1 with {@code boundTargetSchema}, while {@code cols} excludes the static
     * partition columns and may be in a different (user-specified) order. Partition columns are therefore
     * located by their position in the full schema. (An earlier revision indexed by {@code cols}, which
     * mislocated the dynamic column whenever {@code cols} order diverged from the full schema — the
     * partial-static {@code PARTITION(p1='x') SELECT ..., p2} and reordered-explicit-list cases.)</p>
     */
    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        // Rewrite (compaction) writes must gather to a single writer to control the output file count;
        // this neutral flag wins over the partition-shuffle / parallel-write arms below. Carried as a
        // sink field (no ConnectContext access, no instanceof Iceberg).
        if (isRewrite) {
            return PhysicalProperties.GATHER;
        }
        if (!(targetTable instanceof PluginDrivenExternalTable)) {
            return PhysicalProperties.GATHER;
        }
        PluginDrivenExternalTable table = (PluginDrivenExternalTable) targetTable;

        Optional<ConnectorWriteDistribution> connectorDistribution =
                table.getConnectorWriteDistribution();
        if (connectorDistribution.isPresent()) {
            return toPhysicalProperties(connectorDistribution.get());
        }

        if (table.requirePartitionLocalSortOnWrite()) {
            Set<String> partitionNames = boundPartitionColumns.stream()
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
                    List<Column> fullSchema = boundTargetSchema;
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

        if (table.requirePartitionHashOnWrite()) {
            Set<String> partitionNames = boundPartitionColumns.stream()
                    .map(Column::getName)
                    .collect(Collectors.toSet());
            if (!partitionNames.isEmpty()) {
                // Hash-distribute by partition columns with NO local sort (byte-exact to legacy
                // PhysicalHiveTableSink.getRequirePhysicalProperties): same partition value -> same writer
                // instance keeps the output file count at ~one-per-partition, and the hive file writer buffers a
                // per-partition writer so — unlike the MaxCompute arm above — no MustLocalSortOrderSpec is added.
                // Index by full-schema position, which is aligned 1:1 with child output because a connector
                // declaring requiresPartitionHashWrite also declares requiresFullSchemaWriteOrder.
                List<Integer> columnIdx = new ArrayList<>();
                List<Column> fullSchema = boundTargetSchema;
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
                return new PhysicalProperties(shuffleInfo);
            }
        }

        if (table.supportsParallelWrite()) {
            return PhysicalProperties.SINK_RANDOM_PARTITIONED;
        }
        return PhysicalProperties.GATHER;
    }

    private PhysicalProperties toPhysicalProperties(ConnectorWriteDistribution distribution) {
        switch (distribution.getMode()) {
            case EXECUTION_ANY:
                return PhysicalProperties.EXECUTION_ANY;
            case GATHER:
                return PhysicalProperties.GATHER;
            case EXTERNAL_UNPARTITIONED:
                return PhysicalProperties.SINK_RANDOM_PARTITIONED;
            case HASH:
                return PhysicalProperties.createHash(
                        routeExprIds(distribution.getRouteColumns()), ShuffleType.REQUIRE);
            case PAIMON_FIXED_BUCKET:
                if (Config.be_exec_version
                        < DistributionSpecExternalTableSinkHashPartitioned.MIN_BE_EXEC_VERSION) {
                    return PhysicalProperties.GATHER;
                }
                return new PhysicalProperties(new DistributionSpecPaimonTableSinkHashPartitioned(
                        routeExprIds(distribution.getRouteColumns()), distribution.getNumBuckets(),
                        distribution.getPartitionFieldIndexes(), distribution.getBucketFieldIndexes()));
            default:
                throw new IllegalStateException("Unsupported connector write distribution: "
                        + distribution.getMode());
        }
    }

    private List<ExprId> routeExprIds(List<String> routeColumns) {
        List<Slot> output = child().getOutput();
        int offset = isChangelogRowChange() ? 1 : 0;
        Preconditions.checkState(boundTargetSchema.size() + offset == output.size(),
                "Connector sink schema must match child output for routed writes");
        Map<String, ExprId> outputByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < boundTargetSchema.size(); i++) {
            outputByName.put(boundTargetSchema.get(i).getName(), output.get(i + offset).getExprId());
        }
        List<ExprId> exprIds = new ArrayList<>(routeColumns.size());
        for (String column : routeColumns) {
            exprIds.add(Preconditions.checkNotNull(outputByName.get(column),
                    "Connector route column is missing from sink output: " + column));
        }
        return exprIds;
    }

    private boolean isChangelogRowChange() {
        return dmlCommandType == DMLCommandType.DELETE
                || dmlCommandType == DMLCommandType.UPDATE
                || dmlCommandType == DMLCommandType.MERGE;
    }
}
