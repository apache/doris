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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecExternalTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.DistributionSpecIcebergTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/** physical iceberg sink */
public class PhysicalIcebergTableSink<CHILD_TYPE extends Plan> extends PhysicalBaseExternalTableSink<CHILD_TYPE> {
    private final Table targetIcebergTable;

    /**
     * constructor
     */
    public PhysicalIcebergTableSink(IcebergExternalDatabase database,
                                    IcebergExternalTable targetTable,
                                    Table targetIcebergTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    CHILD_TYPE child) {
        this(database, targetTable, targetIcebergTable, cols, outputExprs, groupExpression, logicalProperties,
                PhysicalProperties.GATHER, null, child);
    }

    /**
     * constructor
     */
    public PhysicalIcebergTableSink(IcebergExternalDatabase database,
                                    IcebergExternalTable targetTable,
                                    Table targetIcebergTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    PhysicalProperties physicalProperties,
                                    Statistics statistics,
                                    CHILD_TYPE child) {
        super(PlanType.PHYSICAL_ICEBERG_TABLE_SINK, database, targetTable, cols, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.targetIcebergTable = Objects.requireNonNull(
                targetIcebergTable, "targetIcebergTable != null in PhysicalIcebergTableSink");
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalIcebergTableSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalIcebergTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalIcebergTableSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalIcebergTableSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalIcebergTableSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, groupExpression, getLogicalProperties(),
                physicalProperties, statistics, child());
    }

    public Table getTargetIcebergTable() {
        return targetIcebergTable;
    }

    /**
     * get output physical properties
     */
    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        // For Iceberg rewrite operations with small data volume,
        // use GATHER distribution to collect data to a single node
        // This helps minimize the number of output files
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.getStatementContext() != null
                && connectContext.getStatementContext().isUseGatherForIcebergRewrite()) {
            return PhysicalProperties.GATHER;
        }

        if (targetIcebergTable.spec().isPartitioned()) {
            if (Config.be_exec_version
                    < DistributionSpecExternalTableSinkHashPartitioned.MIN_BE_EXEC_VERSION) {
                return PhysicalProperties.GATHER;
            }
            DistributionSpecIcebergTableSinkHashPartitioned distributionSpec
                    = buildPartitionDistributionSpec();
            // An unsupported transform must not silently hash its raw source column as though it
            // were the final Iceberg partition value. GATHER is the safe fallback until the
            // transform is supported.
            return distributionSpec == null
                    ? PhysicalProperties.GATHER
                    : new PhysicalProperties(distributionSpec);
        }
        return PhysicalProperties.EXTERNAL_TABLE_SINK_UNPARTITIONED;
    }

    private DistributionSpecIcebergTableSinkHashPartitioned buildPartitionDistributionSpec() {
        List<org.apache.doris.nereids.trees.expressions.Slot> outputSlots = child().getOutput();
        if (cols.size() != outputSlots.size()) {
            return null;
        }

        Map<String, ExprId> columnExprIds = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Column> columnsByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < cols.size(); i++) {
            columnExprIds.put(cols.get(i).getName(), outputSlots.get(i).getExprId());
            columnsByName.put(cols.get(i).getName(), cols.get(i));
        }

        List<ExprId> sourceExprIds = new ArrayList<>();
        List<String> transforms = new ArrayList<>();
        for (PartitionField field : targetIcebergTable.spec().fields()) {
            Types.NestedField sourceField = targetIcebergTable.schema().findField(field.sourceId());
            if (sourceField == null) {
                return null;
            }
            ExprId sourceExprId = columnExprIds.get(sourceField.name());
            Column sourceColumn = columnsByName.get(sourceField.name());
            String transform = field.transform().toString();
            if (sourceExprId == null || sourceColumn == null
                    || !supportsPartitionTransform(transform, sourceColumn.getDataType())) {
                return null;
            }
            sourceExprIds.add(sourceExprId);
            transforms.add(transform);
        }
        if (sourceExprIds.isEmpty()) {
            return null;
        }
        return new DistributionSpecIcebergTableSinkHashPartitioned(sourceExprIds, transforms);
    }

    private boolean supportsPartitionTransform(String transform, PrimitiveType sourceType) {
        if ("identity".equals(transform) || "void".equals(transform)) {
            return true;
        }
        if ("year".equals(transform) || "month".equals(transform) || "day".equals(transform)) {
            return sourceType == PrimitiveType.DATEV2 || sourceType == PrimitiveType.DATETIMEV2;
        }
        if ("hour".equals(transform)) {
            return sourceType == PrimitiveType.DATETIMEV2;
        }
        if (transform.startsWith("bucket[") && transform.endsWith("]")) {
            switch (sourceType) {
                case INT:
                case BIGINT:
                case VARCHAR:
                case CHAR:
                case STRING:
                case DATEV2:
                case DATETIMEV2:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                    return true;
                default:
                    return false;
            }
        }
        if (transform.startsWith("truncate[") && transform.endsWith("]")) {
            switch (sourceType) {
                case INT:
                case BIGINT:
                case VARCHAR:
                case CHAR:
                case STRING:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }
}
