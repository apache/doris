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
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecExternalTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.DistributionSpecExternalTableSinkHashPartitioned.PaimonFixedBucketRouteInfo;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.statistics.Statistics;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** physical paimon sink */
public class PhysicalPaimonTableSink<CHILD_TYPE extends Plan> extends PhysicalBaseExternalTableSink<CHILD_TYPE> {

    /**
     * constructor
     */
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

    /**
     * constructor
     */
    public PhysicalPaimonTableSink(PaimonExternalDatabase database,
                                   PaimonExternalTable targetTable,
                                   List<Column> cols,
                                   List<NamedExpression> outputExprs,
                                   Optional<GroupExpression> groupExpression,
                                   LogicalProperties logicalProperties,
                                   PhysicalProperties physicalProperties,
                                   Statistics statistics,
                                   CHILD_TYPE child) {
        super(PlanType.PHYSICAL_PAIMON_TABLE_SINK, database, targetTable, cols, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, (PaimonExternalTable) targetTable,
                cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0)));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalPaimonTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, (PaimonExternalTable) targetTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, (PaimonExternalTable) targetTable, cols, outputExprs,
                groupExpression, logicalProperties.get(), physicalProperties, statistics, children.get(0)));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, (PaimonExternalTable) targetTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child()));
    }

    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        PaimonExternalTable paimonExternalTable = (PaimonExternalTable) targetTable;
        Optional<MvccSnapshot> snapshot = MvccUtil.getSnapshotFromContext(targetTable);
        org.apache.paimon.table.Table paimonTable = paimonExternalTable.getPaimonTable(snapshot);
        BucketMode bucketMode = null;
        if (paimonTable instanceof FileStoreTable) {
            FileStoreTable fileStoreTable = (FileStoreTable) paimonTable;
            bucketMode = fileStoreTable.bucketMode();
            if (bucketMode == BucketMode.HASH_DYNAMIC
                    || bucketMode == BucketMode.KEY_DYNAMIC
                        || bucketMode == BucketMode.POSTPONE_MODE) {
                throw new UnsupportedOperationException("Unsupported Paimon bucket mode for write: " + bucketMode);
            }
        }

        List<Column> partitionColumns = paimonExternalTable.getPartitionColumns(snapshot);
        List<String> partitionColumnNames = new ArrayList<>();
        for (Column partitionColumn : partitionColumns) {
            partitionColumnNames.add(partitionColumn.getName());
        }

        Map<String, ExprId> columnExprIdMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, DataType> columnDataTypeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        int outputSize = child().getOutput().size();
        for (int i = 0; i < cols.size() && i < outputSize; i++) {
            Slot slot = child().getOutput().get(i);
            columnExprIdMap.put(cols.get(i).getName(), slot.getExprId());
            columnDataTypeMap.put(cols.get(i).getName(), slot.getDataType());
        }

        List<ExprId> partitionExprIds = new ArrayList<>();
        for (String partitionColumn : partitionColumnNames) {
            ExprId exprId = columnExprIdMap.get(partitionColumn);
            if (exprId == null) {
                return PhysicalProperties.SINK_RANDOM_PARTITIONED;
            }
            partitionExprIds.add(exprId);
        }

        PaimonFixedBucketRouteInfo paimonRouteInfo = null;
        if (bucketMode == BucketMode.HASH_FIXED) {
            paimonRouteInfo = buildPaimonFixedBucketRouteInfo(
                    (FileStoreTable) paimonTable, columnExprIdMap, columnDataTypeMap);
        } else if (partitionExprIds.isEmpty()) {
            return PhysicalProperties.SINK_RANDOM_PARTITIONED;
        }

        DistributionSpecExternalTableSinkHashPartitioned shuffleInfo =
                new DistributionSpecExternalTableSinkHashPartitioned();
        shuffleInfo.setOutputColExprIds(partitionExprIds.stream().distinct().collect(Collectors.toList()));
        if (bucketMode == BucketMode.HASH_FIXED) {
            shuffleInfo.setExternalSinkHashMode(
                    DistributionSpecExternalTableSinkHashPartitioned.ExternalSinkHashMode.STRICT_HASH);
            shuffleInfo.setPaimonFixedBucketRouteInfo(paimonRouteInfo);
        }
        return new PhysicalProperties(shuffleInfo);
    }

    private PaimonFixedBucketRouteInfo buildPaimonFixedBucketRouteInfo(FileStoreTable fileStoreTable,
            Map<String, ExprId> columnExprIdMap, Map<String, DataType> columnDataTypeMap) {
        TableSchema schema = fileStoreTable.schema();
        List<String> bucketKeys = schema.bucketKeys();
        if (bucketKeys.isEmpty()) {
            throw new UnsupportedOperationException("Paimon fixed bucket write requires bucket keys");
        }

        List<ExprId> bucketKeyExprIds = new ArrayList<>();
        List<DataType> bucketKeyTypes = new ArrayList<>();
        for (String bucketKey : bucketKeys) {
            ExprId exprId = columnExprIdMap.get(bucketKey);
            if (exprId == null) {
                throw new UnsupportedOperationException(
                        "Paimon fixed bucket write requires bucket key in sink output: " + bucketKey);
            }
            bucketKeyExprIds.add(exprId);
            bucketKeyTypes.add(columnDataTypeMap.get(bucketKey));
        }

        CoreOptions.BucketFunctionType bucketFunctionType =
                new CoreOptions(schema.options()).bucketFunctionType();
        if (bucketFunctionType == CoreOptions.BucketFunctionType.MOD) {
            if (bucketKeyTypes.size() != 1
                    || !(bucketKeyTypes.get(0).isIntegerType() || bucketKeyTypes.get(0).isBigIntType())) {
                throw new UnsupportedOperationException(
                        "Paimon mod bucket write requires exactly one INT or BIGINT bucket key");
            }
            return new PaimonFixedBucketRouteInfo(schema.numBuckets(),
                    PaimonFixedBucketRouteInfo.BucketFunctionType.MOD, bucketKeyExprIds);
        } else if (bucketFunctionType == CoreOptions.BucketFunctionType.DEFAULT) {
            for (DataType bucketKeyType : bucketKeyTypes) {
                if (!supportDefaultBucketKeyType(bucketKeyType)) {
                    throw new UnsupportedOperationException(String.format(Locale.ROOT,
                            "Unsupported Paimon default bucket key type for write routing: %s", bucketKeyType));
                }
            }
            return new PaimonFixedBucketRouteInfo(schema.numBuckets(),
                    PaimonFixedBucketRouteInfo.BucketFunctionType.DEFAULT, bucketKeyExprIds);
        }
        throw new UnsupportedOperationException(
                "Unsupported Paimon bucket function for write routing: " + bucketFunctionType);
    }

    private boolean supportDefaultBucketKeyType(DataType dataType) {
        return dataType.isIntegerLikeType();
    }
}
