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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantType;

import java.util.List;
import java.util.Optional;

/** Analysis checks for the Variant V2-only Iceberg write path. */
public final class IcebergVariantWriteAnalyzer {
    private IcebergVariantWriteAnalyzer() {
    }

    /** Rejects legacy Variant leaves before sink coercion hides their physical representation. */
    public static void validate(
            List<Column> targetColumns, List<? extends NamedExpression> sourceColumns) {
        for (int i = 0; i < targetColumns.size(); ++i) {
            DataType targetType = DataType.fromCatalogType(targetColumns.get(i).getType());
            if (VariantType.containsVariant(targetType)) {
                validateVariantConversion(
                        sourceColumns.get(i).getDataType(), targetType,
                        targetColumns.get(i).getName());
            }
        }
    }

    /**
     * Selects the target used before an inline table computes a common type for each VALUES
     * column. Variant sources must remain visible until sink analysis can reject legacy leaves.
     */
    public static Optional<DataType> resolveInlineCoercionTarget(
            DataType targetType, NamedExpression value) {
        if (!VariantType.containsVariant(targetType)) {
            return Optional.of(targetType);
        }
        try {
            if (VariantType.containsVariant(value.getDataType())) {
                return Optional.empty();
            }
        } catch (UnboundException ignored) {
            // Expression analysis resolves the source after the target cast is attached.
        }
        return Optional.of(targetType);
    }

    static void validateVariantConversion(
            DataType sourceType, DataType targetType, String path) {
        if (!VariantType.containsVariant(targetType)) {
            return;
        }
        if (targetType instanceof VariantType) {
            validateVariantSource(sourceType, path);
            return;
        }
        if (sourceType instanceof ArrayType && targetType instanceof ArrayType) {
            validateVariantConversion(
                    ((ArrayType) sourceType).getItemType(),
                    ((ArrayType) targetType).getItemType(), path + "[]");
            return;
        }
        if (sourceType instanceof MapType && targetType instanceof MapType) {
            MapType sourceMap = (MapType) sourceType;
            MapType targetMap = (MapType) targetType;
            validateVariantConversion(
                    sourceMap.getKeyType(), targetMap.getKeyType(), path + ".key");
            validateVariantConversion(
                    sourceMap.getValueType(), targetMap.getValueType(), path + ".value");
            return;
        }
        if (sourceType instanceof StructType && targetType instanceof StructType) {
            List<StructField> sourceFields = ((StructType) sourceType).getFields();
            List<StructField> targetFields = ((StructType) targetType).getFields();
            int fieldCount = Math.min(sourceFields.size(), targetFields.size());
            for (int i = 0; i < fieldCount; ++i) {
                validateVariantConversion(
                        sourceFields.get(i).getDataType(), targetFields.get(i).getDataType(),
                        path + "." + targetFields.get(i).getName());
            }
            return;
        }

        // A shape-changing cast bypasses the matching container branches. Validate the complete
        // source against the Variant V2 leaf conversion contract before adding the sink cast.
        validateVariantSource(sourceType, path);
    }

    private static void validateVariantSource(DataType sourceType, String path) {
        if (VariantType.isLegacyVariant(sourceType)) {
            throw new AnalysisException(
                    "Writing legacy Doris VARIANT to Iceberg VARIANT column '"
                            + path + "' is not supported");
        }
        if (sourceType instanceof ArrayType) {
            validateVariantSource(((ArrayType) sourceType).getItemType(), path + "[]");
            return;
        }
        if (!VariantType.isSupportedComputeV2CastSource(sourceType)) {
            throw new AnalysisException(
                    "Iceberg VARIANT write cannot convert input column '" + path
                            + "' from " + sourceType.toSql() + " to Variant V2");
        }
    }
}
