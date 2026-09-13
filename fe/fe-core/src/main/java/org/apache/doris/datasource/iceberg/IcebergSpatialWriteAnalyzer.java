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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.types.DataType;

import java.util.List;
import java.util.Objects;

/** Analysis checks that preserve Iceberg spatial type parameters during writes. */
public final class IcebergSpatialWriteAnalyzer {
    private IcebergSpatialWriteAnalyzer() {
    }

    /**
     * Rejects an INSERT source whose spatial kind or parameters differ from the Iceberg target
     * before sink coercion can erase that distinction.
     */
    public static void validate(
            List<Column> targetColumns, List<? extends NamedExpression> sourceColumns) {
        if (targetColumns.size() != sourceColumns.size()) {
            throw new AnalysisException("Iceberg spatial write target and source columns are not aligned");
        }
        for (int i = 0; i < targetColumns.size(); ++i) {
            Type targetCatalogType = targetColumns.get(i).getType();
            if (!(targetCatalogType instanceof ScalarType)
                    || !((ScalarType) targetCatalogType).isSpatialType()) {
                continue;
            }
            validateSpatialConversion(sourceColumns.get(i).getDataType().toCatalogDataType(),
                    (ScalarType) targetCatalogType, targetColumns.get(i).getName());
        }
    }

    /** Validates each MERGE action before a target cast can hide its spatial source type. */
    public static void validateMergeActions(
            List<Column> targetColumns, List<? extends NamedExpression> sourceColumns) {
        if (targetColumns.size() != sourceColumns.size()) {
            throw new AnalysisException("Iceberg spatial write target and source columns are not aligned");
        }
        for (int i = 0; i < targetColumns.size(); ++i) {
            Type targetCatalogType = targetColumns.get(i).getType();
            if (targetCatalogType instanceof ScalarType && ((ScalarType) targetCatalogType).isSpatialType()) {
                validateMergeActionExpression(sourceColumns.get(i), (ScalarType) targetCatalogType,
                        targetColumns.get(i).getName());
            }
        }
    }

    private static void validateMergeActionExpression(
            Expression expression, ScalarType targetType, String columnName) {
        if (expression instanceof Alias) {
            validateMergeActionExpression(expression.child(0), targetType, columnName);
            return;
        }
        DataType targetDataType = DataType.fromCatalogType(targetType);
        if (expression instanceof If && expression.getDataType().equals(targetDataType)) {
            If ifExpression = (If) expression;
            validateMergeActionExpression(ifExpression.getTrueValue(), targetType, columnName);
            validateMergeActionExpression(ifExpression.getFalseValue(), targetType, columnName);
            return;
        }
        if (expression instanceof Cast && expression.getDataType().equals(targetDataType)) {
            validateSpatialConversion(expression.child(0).getDataType().toCatalogDataType(), targetType, columnName);
            return;
        }
        validateSpatialConversion(expression.getDataType().toCatalogDataType(), targetType, columnName);
    }

    static void validateSpatialConversion(Type sourceType, ScalarType targetType, String columnName) {
        if (sourceType.isNull()) {
            return;
        }
        if (!(sourceType instanceof ScalarType) || !((ScalarType) sourceType).isSpatialType()) {
            throw new AnalysisException("Iceberg spatial write cannot convert input column '" + columnName
                    + "' from " + sourceType.toSql() + " to " + targetType.toSql());
        }
        ScalarType sourceSpatialType = (ScalarType) sourceType;
        if (sourceSpatialType.getPrimitiveType() != targetType.getPrimitiveType()) {
            throw new AnalysisException("Iceberg spatial write type mismatch for column '" + columnName
                    + "': source " + sourceSpatialType.toSql() + " does not match target " + targetType.toSql());
        }
        if (!Objects.equals(sourceSpatialType.getSpatialCrs(), targetType.getSpatialCrs())) {
            throw new AnalysisException("Iceberg spatial write CRS mismatch for column '" + columnName
                    + "': source " + sourceSpatialType.getSpatialCrs() + " does not match target "
                    + targetType.getSpatialCrs());
        }
        if (!Objects.equals(sourceSpatialType.getSpatialAlgorithm(), targetType.getSpatialAlgorithm())) {
            throw new AnalysisException("Iceberg spatial write algorithm mismatch for column '" + columnName
                    + "': source " + sourceSpatialType.getSpatialAlgorithm() + " does not match target "
                    + targetType.getSpatialAlgorithm());
        }
    }
}
