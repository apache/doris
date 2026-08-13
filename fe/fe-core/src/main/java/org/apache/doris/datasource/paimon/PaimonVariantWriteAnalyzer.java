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
import org.apache.doris.common.Config;
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

/** Analysis checks for the V2-only Paimon Variant write protocol. */
public final class PaimonVariantWriteAnalyzer {
    private PaimonVariantWriteAnalyzer() {
    }

    /**
     * Rejects a disabled V2 protocol and unsupported Variant V2 conversions before sink coercion.
     */
    public static void validate(
            PaimonWriteTarget writeTarget,
            List<Column> writeColumns,
            List<NamedExpression> sourceOutputs) throws AnalysisException {
        if (writeColumns.size() != sourceOutputs.size()) {
            throw new IllegalArgumentException("Paimon write columns and source outputs must align");
        }
        for (int i = 0; i < writeColumns.size(); i++) {
            Column column = writeColumns.get(i);
            Type targetCatalogType = writeTarget.getColumnTypes().get(column.getName());
            if (targetCatalogType == null) {
                continue;
            }
            DataType targetType = DataType.fromCatalogType(targetCatalogType);
            if (!VariantType.containsVariant(targetType)) {
                continue;
            }
            if (!Config.enable_variant_v2) {
                throw new AnalysisException(
                        "Paimon VARIANT write only supports Variant V2; "
                                + "set FE config enable_variant_v2=true");
            }
            validateVariantConversion(
                    sourceOutputs.get(i).getDataType(), targetType, column.getName());
        }
    }

    /**
     * Selects the target used before an inline table computes a common type for each VALUES
     * column. An empty result defers coercion until sink binding can validate the resolved source.
     */
    public static Optional<DataType> resolveInlineCoercionTarget(
            DataType targetType, NamedExpression value) {
        if (!VariantType.containsVariant(targetType)) {
            return Optional.of(targetType);
        }
        if (!Config.enable_variant_v2) {
            return Optional.empty();
        }
        try {
            if (VariantType.containsVariant(value.getDataType())) {
                // Preserve an already resolved Variant source for final sink validation.
                return Optional.empty();
            }
        } catch (UnboundException ignored) {
            // Expression analysis will resolve this source after the target cast is attached.
        }
        // Preserve each non-Variant VALUES row before common-type coercion. Otherwise (1), ('x')
        // would first become STRING and the integer would be encoded as a Variant string.
        return Optional.of(VariantType.toComputeV2(targetType));
    }

    private static void validateVariantConversion(
            DataType sourceType, DataType targetType, String path) throws AnalysisException {
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
                    ((ArrayType) targetType).getItemType(),
                    path + "[]");
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
            for (int i = 0; i < fieldCount; i++) {
                validateVariantConversion(
                        sourceFields.get(i).getDataType(),
                        targetFields.get(i).getDataType(),
                        path + "." + targetFields.get(i).getName());
            }
            return;
        }

        // A shape-changing cast can bypass the matching container branches above. Validate its
        // complete source against the leaf conversion contract before sink coercion adds a cast.
        validateVariantSource(sourceType, path);
    }

    private static void validateVariantSource(
            DataType sourceType, String path) throws AnalysisException {
        if (sourceType instanceof ArrayType) {
            validateVariantSource(((ArrayType) sourceType).getItemType(), path + "[]");
            return;
        }
        if (!VariantType.isSupportedComputeV2CastSource(sourceType)) {
            throw new AnalysisException(
                    "Paimon VARIANT write cannot convert input column '" + path
                            + "' from " + sourceType.toSql() + " to Variant V2");
        }
    }
}
