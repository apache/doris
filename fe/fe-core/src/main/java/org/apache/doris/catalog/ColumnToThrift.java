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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TAggregationType;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TPatternType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ColumnToThrift {

    public static void setIndexFlag(TColumn tColumn, OlapTable olapTable) {
        Set<String> bfColumns = olapTable.getCopiedBfColumns();
        if (bfColumns != null && bfColumns.contains(tColumn.getColumnName())) {
            tColumn.setIsBloomFilterColumn(true);
        }
    }

    public static TColumn toThrift(Column column) {
        TColumn tColumn = new TColumn();
        tColumn.setColumnName(column.getNonShadowName());

        TColumnType tColumnType = new TColumnType();
        tColumnType.setType(column.getDataType().toThrift());
        tColumnType.setLen(column.getStrLen());
        tColumnType.setPrecision(column.getPrecision());
        tColumnType.setScale(column.getScale());
        tColumnType.setVariantMaxSubcolumnsCount(column.getVariantMaxSubcolumnsCount());
        tColumnType.setVariantEnableDocMode(column.getVariantEnableDocMode());
        tColumnType.setIndexLen(column.getOlapColumnIndexSize());
        tColumn.setColumnType(tColumnType);

        if (column.getAggregationType() != null) {
            tColumn.setAggregationType(column.getAggregationType().toThrift());
        } else {
            tColumn.setAggregationType(TAggregationType.NONE);
        }

        tColumn.setIsKey(column.isKey());
        tColumn.setIsAllowNull(column.isAllowNull());
        tColumn.setIsAutoIncrement(column.isAutoInc());
        tColumn.setIsOnUpdateCurrentTimestamp(column.hasOnUpdateDefaultValue());
        tColumn.setDefaultValue(
                column.getRealDefaultValue() == null ? column.getDefaultValue() : column.getRealDefaultValue());
        tColumn.setVisible(column.isVisible());
        toChildrenThrift(column, tColumn);

        tColumn.setColUniqueId(column.getUniqueId());

        if (column.getType().isAggStateType()) {
            AggStateType aggState = (AggStateType) column.getType();
            tColumn.setAggregation(aggState.getFunctionName());
            tColumn.setResultIsNullable(aggState.getResultIsNullable());
            if (column.getChildren() != null) {
                for (Column child : column.getChildren()) {
                    tColumn.addToChildrenColumn(toThrift(child));
                }
            }
            tColumn.setBeExecVersion(Config.be_exec_version);
        }
        tColumn.setClusterKeyId(column.getClusterKeyId());
        tColumn.setVariantEnableTypedPathsToSparse(column.getVariantEnableTypedPathsToSparse());
        tColumn.setVariantMaxSparseColumnStatisticsSize(column.getVariantMaxSparseColumnStatisticsSize());
        tColumn.setVariantSparseHashShardCount(column.getVariantSparseHashShardCount());
        tColumn.setVariantEnableDocMode(column.getVariantEnableDocMode());
        tColumn.setVariantDocMaterializationMinRows(column.getvariantDocMaterializationMinRows());
        tColumn.setVariantDocHashShardCount(column.getVariantDocShardCount());
        tColumn.setVariantEnableNestedGroup(column.getVariantEnableNestedGroup());
        // ATTN:
        // Currently, this `toThrift()` method is only used from CreateReplicaTask.
        // And CreateReplicaTask does not need `defineExpr` field.
        // The `defineExpr` is only used when creating `TAlterMaterializedViewParam`, which is in `AlterReplicaTask`.
        // And when creating `TAlterMaterializedViewParam`, the `defineExpr` is certainly analyzed.
        // If we need to use `defineExpr` and call defineExpr.treeToThrift(),
        // make sure it is analyzed, or NPE will thrown.
        return tColumn;
    }

    public static void setChildrenTColumn(Column children, TColumn tColumn) {
        TColumn childrenTColumn = new TColumn();
        childrenTColumn.setColumnName(children.getName());

        TColumnType childrenTColumnType = new TColumnType();
        childrenTColumnType.setType(children.getDataType().toThrift());
        childrenTColumnType.setLen(children.getStrLen());
        childrenTColumnType.setPrecision(children.getPrecision());
        childrenTColumnType.setScale(children.getScale());
        childrenTColumnType.setIndexLen(children.getOlapColumnIndexSize());

        childrenTColumn.setColumnType(childrenTColumnType);
        childrenTColumn.setIsAllowNull(children.isAllowNull());
        // TODO: If we don't set the aggregate type for children, the type will be
        //  considered as TAggregationType::SUM after deserializing in BE.
        //  For now, we make children inherit the aggregate type from their parent.
        if (tColumn.getAggregationType() != null) {
            childrenTColumn.setAggregationType(tColumn.getAggregationType());
        }
        if (children.getFieldPatternType() != null) {
            childrenTColumn.setPatternType(toThriftPatternType(children.getFieldPatternType()));
        }
        childrenTColumn.setClusterKeyId(children.getClusterKeyId());

        tColumn.children_column.add(childrenTColumn);
        toChildrenThrift(children, childrenTColumn);
    }

    public static void addChildren(Column column, TColumn tColumn) {
        if (column.getChildren() != null) {
            List<Column> childrenColumns = column.getChildren();
            tColumn.setChildrenColumn(new ArrayList<>());
            for (Column c : childrenColumns) {
                setChildrenTColumn(c, tColumn);
            }
        }
    }

    public static void toChildrenThrift(Column column, TColumn tColumn) {
        if (column.getType().isArrayType()) {
            Column children = column.getChildren().get(0);
            tColumn.setChildrenColumn(new ArrayList<>());
            setChildrenTColumn(children, tColumn);
        } else if (column.getType().isMapType()) {
            Column k = column.getChildren().get(0);
            Column v = column.getChildren().get(1);
            tColumn.setChildrenColumn(new ArrayList<>());
            setChildrenTColumn(k, tColumn);
            setChildrenTColumn(v, tColumn);
        } else if (column.getType().isStructType()) {
            List<Column> childrenColumns = column.getChildren();
            tColumn.setChildrenColumn(new ArrayList<>());
            for (Column children : childrenColumns) {
                setChildrenTColumn(children, tColumn);
            }
        } else if (column.getType().isVariantType()) {
            // variant may contain predefined structured fields
            addChildren(column, tColumn);
        }
    }

    public static TPatternType toThriftPatternType(PatternType pt) {
        if (pt == null) {
            return null;
        }
        switch (pt) {
            case MATCH_NAME:
                return TPatternType.MATCH_NAME;
            case MATCH_NAME_GLOB:
                return TPatternType.MATCH_NAME_GLOB;
            default:
                throw new IllegalArgumentException("Unknown pattern type: " + pt);
        }
    }
}
