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
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecExternalTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecPaimonTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;

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
    private final PaimonWriteTarget writeTarget;
    private final DMLCommandType dmlCommandType;

    public PhysicalPaimonTableSink(PaimonExternalDatabase database,
                                    PaimonWriteTarget writeTarget,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    DMLCommandType dmlCommandType,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    CHILD_TYPE child) {
        this(database, writeTarget, cols, outputExprs, dmlCommandType, groupExpression, logicalProperties,
                PhysicalProperties.EXTERNAL_TABLE_SINK_UNPARTITIONED, null, child);
    }

    public PhysicalPaimonTableSink(PaimonExternalDatabase database,
                                    PaimonWriteTarget writeTarget,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    DMLCommandType dmlCommandType,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    PhysicalProperties physicalProperties,
                                    Statistics statistics,
                                    CHILD_TYPE child) {
        super(PlanType.PHYSICAL_PAIMON_TABLE_SINK, database, writeTarget.getDorisTable(), cols, outputExprs,
                groupExpression, logicalProperties, physicalProperties, statistics, child);
        this.writeTarget = writeTarget;
        this.dmlCommandType = dmlCommandType;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, writeTarget, cols, outputExprs, dmlCommandType,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, writeTarget, cols, outputExprs, dmlCommandType,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, writeTarget, cols, outputExprs, dmlCommandType,
                groupExpression, logicalProperties.get(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalPaimonTableSink<Plan> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics stats) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, writeTarget, cols, outputExprs, dmlCommandType,
                groupExpression, getLogicalProperties(), physicalProperties, stats, child());
    }

    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        FileStoreTable paimonTable = writeTarget.getTable();
        DistributionSpecPaimonTableSinkHashPartitioned fixedBucketSpec
                = buildFixedBucketDistributionSpec(paimonTable);
        if (fixedBucketSpec != null) {
            return new PhysicalProperties(fixedBucketSpec);
        }
        if (paimonTable.bucketMode() == BucketMode.BUCKET_UNAWARE) {
            // Bucket-unaware tables are append-only and Paimon deliberately gives each writer
            // an empty, no-compaction file store. No bucket ownership is shared, so explicit
            // random distribution is safe. ScaleWriter can be evaluated independently later.
            return PhysicalProperties.EXECUTION_ANY;
        }
        if (requiresSingleWriter(paimonTable)) {
            return PhysicalProperties.GATHER;
        }

        List<String> primaryKeys = paimonTable.primaryKeys();
        if (primaryKeys.isEmpty()) {
            // Retain adaptive distribution for other append modes which are already safe without
            // bucket ownership. BUCKET_UNAWARE is handled above with explicit random routing.
            return PhysicalProperties.EXTERNAL_TABLE_SINK_UNPARTITIONED;
        }

        List<Slot> outputSlots = child().getOutput();
        int columnOffset = isChangelogWrite() ? 1 : 0;
        Preconditions.checkState(cols.size() + columnOffset == outputSlots.size(),
                "Paimon sink columns must match child output");
        Map<String, ExprId> columnExprIds = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < cols.size(); i++) {
            columnExprIds.put(cols.get(i).getName(), outputSlots.get(i + columnOffset).getExprId());
        }

        List<ExprId> primaryKeyExprIds = new ArrayList<>(primaryKeys.size());
        for (String primaryKey : primaryKeys) {
            primaryKeyExprIds.add(Preconditions.checkNotNull(
                    columnExprIds.get(primaryKey),
                    "Paimon primary-key column is missing from sink output"));
        }
        return PhysicalProperties.createHash(primaryKeyExprIds, ShuffleType.REQUIRE);
    }

    /**
     * Whether this sink must use one writer to preserve Paimon write semantics.
     *
     * <p>HASH_DYNAMIC and KEY_DYNAMIC gather to one writer within the current INSERT because
     * their stateful assigners cannot safely be shared. This does not serialize independent jobs;
     * concurrent jobs writing the same dynamic-bucket partition remain unsupported. Fixed-bucket
     * tables use concurrent writers only when Doris can reproduce Paimon's stateless route.
     */
    public boolean requiresSingleWriter() {
        if (buildFixedBucketDistributionSpec(writeTarget.getTable()) != null) {
            return false;
        }
        return requiresSingleWriter(writeTarget.getTable());
    }

    static boolean requiresSingleWriter(FileStoreTable paimonTable) {
        BucketMode bucketMode = paimonTable.bucketMode();
        CoreOptions coreOptions = CoreOptions.fromMap(paimonTable.options());
        if (bucketMode == BucketMode.BUCKET_UNAWARE) {
            return false;
        }
        if (bucketMode == BucketMode.HASH_DYNAMIC
                || bucketMode == BucketMode.KEY_DYNAMIC
                // When the native fixed-bucket route is unavailable, a primary-key table must
                // not let independent writers own the same bucket. An append-only writer has the
                // same fallback requirement while automatic compaction is enabled.
                || (bucketMode == BucketMode.HASH_FIXED
                        && (!paimonTable.primaryKeys().isEmpty() || !coreOptions.writeOnly()))) {
            return true;
        }

        return !coreOptions.writeOnly()
                && (coreOptions.needLookup()
                        || coreOptions.changelogProducer()
                                == CoreOptions.ChangelogProducer.FULL_COMPACTION);
    }

    private DistributionSpecPaimonTableSinkHashPartitioned buildFixedBucketDistributionSpec(
            FileStoreTable paimonTable) {
        if (Config.be_exec_version
                < DistributionSpecExternalTableSinkHashPartitioned.MIN_BE_EXEC_VERSION) {
            return null;
        }
        return buildFixedBucketDistributionSpec(paimonTable, cols, child().getOutput());
    }

    static DistributionSpecPaimonTableSinkHashPartitioned buildFixedBucketDistributionSpec(
            FileStoreTable paimonTable, List<Column> sinkColumns, List<Slot> sinkOutput) {
        if (paimonTable.bucketMode() != BucketMode.HASH_FIXED) {
            return null;
        }

        TableSchema schema = paimonTable.schema();
        CoreOptions coreOptions = CoreOptions.fromMap(schema.options());
        if (coreOptions.bucketFunctionType() != CoreOptions.BucketFunctionType.DEFAULT) {
            return null;
        }

        if (schema.numBuckets() <= 0 || schema.bucketKeys().isEmpty()
                || sinkColumns.size() != sinkOutput.size()) {
            return null;
        }

        Map<String, Slot> outputByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < sinkColumns.size(); i++) {
            if (outputByName.put(sinkColumns.get(i).getName(), sinkOutput.get(i)) != null) {
                return null;
            }
        }
        Map<String, DataField> fieldsByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (DataField field : schema.fields()) {
            fieldsByName.put(field.name(), field);
        }

        List<ExprId> routeExprIds = new ArrayList<>();
        Map<String, Integer> routeIndexes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<Integer> partitionFieldIndexes = appendRouteFields(
                schema.partitionKeys(), outputByName, fieldsByName, routeExprIds, routeIndexes);
        List<Integer> bucketFieldIndexes = appendRouteFields(
                schema.bucketKeys(), outputByName, fieldsByName, routeExprIds, routeIndexes);
        if (partitionFieldIndexes == null || bucketFieldIndexes == null
                || bucketFieldIndexes.isEmpty()) {
            return null;
        }
        return new DistributionSpecPaimonTableSinkHashPartitioned(
                routeExprIds, schema.numBuckets(), partitionFieldIndexes, bucketFieldIndexes);
    }

    private static List<Integer> appendRouteFields(List<String> fieldNames,
            Map<String, Slot> outputByName, Map<String, DataField> fieldsByName,
            List<ExprId> routeExprIds, Map<String, Integer> routeIndexes) {
        ImmutableList.Builder<Integer> indexes = ImmutableList.builder();
        for (String fieldName : fieldNames) {
            Slot slot = outputByName.get(fieldName);
            DataField field = fieldsByName.get(fieldName);
            if (slot == null || field == null
                    // TableWriteImpl applies schema defaults before extracting the final route,
                    // while the native Exchange sees the original value, including explicit NULL.
                    || field.defaultValue() != null
                    || !supportsNativeRouting(field.type().getTypeRoot(), slot.getDataType())) {
                return null;
            }
            Integer index = routeIndexes.get(fieldName);
            if (index == null) {
                index = routeExprIds.size();
                routeExprIds.add(slot.getExprId());
                routeIndexes.put(fieldName, index);
            }
            indexes.add(index);
        }
        return indexes.build();
    }

    private static boolean supportsNativeRouting(DataTypeRoot paimonType, DataType dorisType) {
        switch (paimonType) {
            case BOOLEAN:
                return dorisType.isBooleanType();
            case TINYINT:
                return dorisType.isTinyIntType();
            case SMALLINT:
                return dorisType.isSmallIntType();
            case INTEGER:
                return dorisType.isIntegerType();
            case BIGINT:
                return dorisType.isBigIntType();
            case FLOAT:
                return dorisType.isFloatType();
            case DOUBLE:
                return dorisType.isDoubleType();
            case CHAR:
            case VARCHAR:
                return dorisType.isStringLikeType();
            case BINARY:
            case VARBINARY:
                return dorisType.isStringLikeType() || dorisType.isVarBinaryType();
            default:
                return false;
        }
    }

    public PaimonWriteTarget getWriteTarget() {
        return writeTarget;
    }

    public DMLCommandType getDmlCommandType() {
        return dmlCommandType;
    }

    public boolean isChangelogWrite() {
        return dmlCommandType == DMLCommandType.UPDATE
                || dmlCommandType == DMLCommandType.DELETE
                || dmlCommandType == DMLCommandType.MERGE;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalPaimonTableSink(this, context);
    }
}
