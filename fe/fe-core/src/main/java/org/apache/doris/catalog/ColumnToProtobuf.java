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
import org.apache.doris.common.DdlException;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.proto.OlapFile.PatternTypePB;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.Set;

public class ColumnToProtobuf {
    public static OlapFile.ColumnPB toPb(Column column, Set<String> bfColumns, List<Index> indexes)
            throws DdlException {
        OlapFile.ColumnPB.Builder builder = OlapFile.ColumnPB.newBuilder();

        // when doing schema change, some modified column has a prefix in name.
        // this prefix is only used in FE, not visible to BE, so we should remove this prefix.
        String name = column.getName();
        builder.setName(name.startsWith(Column.SHADOW_NAME_PREFIX)
                ? name.substring(Column.SHADOW_NAME_PREFIX.length()) : name);

        builder.setUniqueId(column.getUniqueId());
        builder.setType(column.getDataType().toThrift().name());
        builder.setIsKey(column.isKey());
        if (column.getFieldPatternType() != null) {
            switch (column.getFieldPatternType()) {
                case MATCH_NAME:
                    builder.setPatternType(PatternTypePB.MATCH_NAME);
                    break;
                case MATCH_NAME_GLOB:
                    builder.setPatternType(PatternTypePB.MATCH_NAME_GLOB);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown PatternType: " + column.getFieldPatternType());
            }
        }
        if (column.getAggregationType() != null) {
            if (column.getType().isAggStateType()) {
                AggStateType aggState = (AggStateType) column.getType();
                builder.setAggregation(aggState.getFunctionName());
                builder.setResultIsNullable(aggState.getResultIsNullable());
                builder.setBeExecVersion(Config.be_exec_version);
                if (column.getChildren() != null) {
                    for (Column child : column.getChildren()) {
                        builder.addChildrenColumns(toPb(child, Sets.newHashSet(), Lists.newArrayList()));
                    }
                }
            } else {
                builder.setAggregation(column.getAggregationType().toString());
            }
        } else {
            builder.setAggregation("NONE");
        }
        builder.setIsNullable(column.isAllowNull());
        if (column.getDefaultValue() != null) {
            builder.setDefaultValue(ByteString.copyFrom(column.getDefaultValue().getBytes()));
        }
        builder.setPrecision(column.getPrecision());
        builder.setFrac(column.getScale());

        int length = getFieldLengthByType(column.getDataType(), column.getStrLen());

        builder.setLength(length);
        builder.setIndexLength(length);
        if (column.getDataType() == PrimitiveType.VARCHAR
                || column.getDataType() == PrimitiveType.STRING) {
            builder.setIndexLength(column.getOlapColumnIndexSize());
        }

        if (bfColumns != null && bfColumns.contains(column.getName())) {
            builder.setIsBfColumn(true);
        } else {
            builder.setIsBfColumn(false);
        }
        builder.setVisible(column.isVisible());

        if (column.getType().isArrayType()) {
            Column child = column.getChildren().get(0);
            builder.addChildrenColumns(toPb(child, Sets.newHashSet(), Lists.newArrayList()));
        } else if (column.getType().isMapType()) {
            Column k = column.getChildren().get(0);
            builder.addChildrenColumns(toPb(k, Sets.newHashSet(), Lists.newArrayList()));
            Column v = column.getChildren().get(1);
            builder.addChildrenColumns(toPb(v, Sets.newHashSet(), Lists.newArrayList()));
        } else if (column.getType().isStructType()) {
            List<Column> childrenColumns = column.getChildren();
            for (Column c : childrenColumns) {
                builder.addChildrenColumns(toPb(c, Sets.newHashSet(), Lists.newArrayList()));
            }
        } else if (column.getType().isVariantType()) {
            builder.setVariantMaxSubcolumnsCount(column.getVariantMaxSubcolumnsCount());
            builder.setVariantEnableTypedPathsToSparse(column.getVariantEnableTypedPathsToSparse());
            builder.setVariantMaxSparseColumnStatisticsSize(column.getVariantMaxSparseColumnStatisticsSize());
            builder.setVariantSparseHashShardCount(column.getVariantSparseHashShardCount());
            builder.setVariantEnableDocMode(column.getVariantEnableDocMode());
            builder.setVariantDocMaterializationMinRows(column.getvariantDocMaterializationMinRows());
            builder.setVariantDocHashShardCount(column.getVariantDocShardCount());
            builder.setVariantEnableNestedGroup(column.getVariantEnableNestedGroup());
            // variant may contain predefined structured fields
            addChildren(column, builder);
        }

        return builder.build();
    }

    public static void addChildren(Column column, OlapFile.ColumnPB.Builder builder) throws DdlException {
        if (column.getChildren() != null) {
            List<Column> childrenColumns = column.getChildren();
            for (Column c : childrenColumns) {
                builder.addChildrenColumns(toPb(c, Sets.newHashSet(), Lists.newArrayList()));
            }
        }
    }

    static int getFieldLengthByType(PrimitiveType type, int stringLength) throws DdlException {
        switch (type) {
            case TINYINT:
            case BOOLEAN:
                return 1;
            case SMALLINT:
                return 2;
            case INT:
                return 4;
            case BIGINT:
                return 8;
            case LARGEINT:
                return 16;
            case DATE:
                return 3;
            case DATEV2:
                return 4;
            case DATETIME:
                return 8;
            case DATETIMEV2:
            case TIMESTAMPTZ:
                return 8;
            case FLOAT:
                return 4;
            case DOUBLE:
                return 8;
            case QUANTILE_STATE:
            case BITMAP:
                return 16;
            case CHAR:
                return stringLength;
            case VARCHAR:
            case HLL:
            case AGG_STATE:
                return stringLength + 2; // sizeof(OLAP_VARCHAR_MAX_LENGTH)
            case STRING:
                return stringLength + 4; // sizeof(OLAP_STRING_MAX_LENGTH)
            case JSONB:
                return stringLength + 4; // sizeof(OLAP_JSONB_MAX_LENGTH)
            case ARRAY:
                return 65535; // OLAP_ARRAY_MAX_LENGTH
            case DECIMAL32:
                return 4;
            case DECIMAL64:
                return 8;
            case DECIMAL128:
                return 16;
            case DECIMAL256:
                return 32;
            case DECIMALV2:
                return 12; // use 12 bytes in olap engine.
            case STRUCT:
                return 65535;
            case MAP:
                return 65535;
            case IPV4:
                return 4;
            case IPV6:
                return 16;
            case VARIANT:
                return stringLength + 4; // sizeof(OLAP_STRING_MAX_LENGTH)
            default:
                throw new DdlException("unknown field type. type: " + type);
        }
    }
}
