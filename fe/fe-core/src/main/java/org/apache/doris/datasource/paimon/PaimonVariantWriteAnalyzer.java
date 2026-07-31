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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantType;

import java.util.List;
import java.util.Map;

/** Analysis checks for the V2-only Paimon Variant write protocol. */
public final class PaimonVariantWriteAnalyzer {
    private PaimonVariantWriteAnalyzer() {
    }

    /**
     * Rejects a disabled V2 protocol and legacy Variant inputs before sink coercion.
     */
    public static void validate(
            PaimonWriteTarget writeTarget,
            List<Column> writeColumns,
            Map<String, NamedExpression> columnToOutput,
            boolean enableVariantV2) throws AnalysisException {
        boolean targetContainsVariant = writeTarget.getColumnTypes().values().stream()
                .map(DataType::fromCatalogType)
                .anyMatch(VariantType::containsVariant);
        if (!targetContainsVariant) {
            return;
        }
        if (!enableVariantV2) {
            throw new AnalysisException(
                    "Paimon VARIANT write only supports Variant V2; "
                            + "set enable_variant_v2=true");
        }

        for (Column column : writeColumns) {
            NamedExpression output = columnToOutput.get(column.getName());
            Type targetCatalogType = writeTarget.getColumnTypes().get(column.getName());
            if (output == null || targetCatalogType == null) {
                continue;
            }
            rejectLegacyVariant(
                    output.getDataType(),
                    DataType.fromCatalogType(targetCatalogType),
                    column.getName());
        }
    }

    private static void rejectLegacyVariant(
            DataType sourceType, DataType targetType, String path) throws AnalysisException {
        if (targetType instanceof VariantType) {
            if (sourceType instanceof VariantType
                    && !((VariantType) sourceType).isComputeV2()) {
                throw new AnalysisException(
                        "Paimon VARIANT write only supports Variant V2, but input column '"
                                + path + "' is Variant V1");
            }
            return;
        }
        if (sourceType instanceof ArrayType && targetType instanceof ArrayType) {
            rejectLegacyVariant(
                    ((ArrayType) sourceType).getItemType(),
                    ((ArrayType) targetType).getItemType(),
                    path + "[]");
            return;
        }
        if (sourceType instanceof MapType && targetType instanceof MapType) {
            MapType sourceMap = (MapType) sourceType;
            MapType targetMap = (MapType) targetType;
            rejectLegacyVariant(sourceMap.getKeyType(), targetMap.getKeyType(), path + ".key");
            rejectLegacyVariant(sourceMap.getValueType(), targetMap.getValueType(), path + ".value");
            return;
        }
        if (sourceType instanceof StructType && targetType instanceof StructType) {
            List<StructField> sourceFields = ((StructType) sourceType).getFields();
            List<StructField> targetFields = ((StructType) targetType).getFields();
            int fieldCount = Math.min(sourceFields.size(), targetFields.size());
            for (int i = 0; i < fieldCount; i++) {
                rejectLegacyVariant(
                        sourceFields.get(i).getDataType(),
                        targetFields.get(i).getDataType(),
                        path + "." + targetFields.get(i).getName());
            }
        }
    }
}
