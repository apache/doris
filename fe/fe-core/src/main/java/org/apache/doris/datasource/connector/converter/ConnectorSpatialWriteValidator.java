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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.types.DataType;

import java.util.List;
import java.util.Objects;

/**
 * Preserves the kind and parameters of spatial connector columns before sink coercion.
 * A spatial value is an opaque WKB payload at execution time, so a generic cast cannot
 * safely convert between CRS, edge algorithms, or Geometry and Geography.
 */
public final class ConnectorSpatialWriteValidator {
    private ConnectorSpatialWriteValidator() {
    }

    public static void validate(List<Column> targetColumns,
            List<? extends NamedExpression> sourceColumns) {
        if (targetColumns.size() != sourceColumns.size()) {
            throw new AnalysisException("Spatial connector write target and source columns are not aligned");
        }
        for (int i = 0; i < targetColumns.size(); i++) {
            validate(targetColumns.get(i), sourceColumns.get(i));
        }
    }

    public static void validate(Column targetColumn, Expression source) {
        Type targetType = targetColumn.getType();
        if (targetType.typeContainsSpatialType()) {
            validateExpression(source, targetType, targetColumn.getName());
        }
    }

    private static void validateExpression(Expression expression, Type targetType, String columnName) {
        if (!targetType.typeContainsSpatialType()) {
            return;
        }
        if (expression instanceof Alias) {
            validateExpression(expression.child(0), targetType, columnName);
            return;
        }
        if (expression.isNullLiteral()) {
            return;
        }
        DataType targetDataType = DataType.fromCatalogType(targetType);
        if (expression instanceof If && expression.getDataType().equals(targetDataType)) {
            If branch = (If) expression;
            validateExpression(branch.getTrueValue(), targetType, columnName);
            validateExpression(branch.getFalseValue(), targetType, columnName);
            return;
        }
        if (expression instanceof Cast && expression.getDataType().equals(targetDataType)) {
            validateExpression(expression.child(0), targetType, columnName);
            return;
        }
        if (expression instanceof CaseWhen && expression.getDataType().equals(targetDataType)) {
            CaseWhen caseWhen = (CaseWhen) expression;
            caseWhen.getWhenClauses().forEach(clause ->
                    validateExpression(clause.getResult(), targetType, columnName));
            caseWhen.getDefaultValue().ifPresent(value -> validateExpression(value, targetType, columnName));
            return;
        }
        if (expression instanceof Coalesce && expression.getDataType().equals(targetDataType)) {
            expression.children().forEach(child -> validateExpression(child, targetType, columnName));
            return;
        }
        // INSERT VALUES can fold array/map/struct constructors containing typed NULLs to literals.
        // Inspect their values as well, rather than rejecting a NULL leaf's original CRS/type.
        if (expression instanceof ArrayLiteral && targetType instanceof ArrayType) {
            ((ArrayLiteral) expression).getValue().forEach(element ->
                    validateExpression(element, ((ArrayType) targetType).getItemType(), columnName + "[]"));
            return;
        }
        if (expression instanceof MapLiteral && targetType instanceof MapType) {
            MapType target = (MapType) targetType;
            ((MapLiteral) expression).getValue().forEach((key, value) -> {
                validateExpression(key, target.getKeyType(), columnName + ".key");
                validateExpression(value, target.getValueType(), columnName + ".value");
            });
            return;
        }
        if (expression instanceof StructLiteral && targetType instanceof StructType) {
            StructType target = (StructType) targetType;
            StructLiteral literal = (StructLiteral) expression;
            if (literal.getValue().size() != target.getFields().size()) {
                throw new AnalysisException("Spatial connector write struct fields do not match: " + columnName);
            }
            for (int i = 0; i < target.getFields().size(); i++) {
                validateExpression(literal.getValue().get(i), target.getFields().get(i).getType(),
                        columnName + "." + target.getFields().get(i).getName());
            }
            return;
        }
        // Inspect constructor inputs before their casts can erase a nested leaf's CRS identity.
        if (expression instanceof Array && targetType instanceof ArrayType) {
            for (Expression element : ((Array) expression).getArguments()) {
                validateExpression(element, ((ArrayType) targetType).getItemType(), columnName + "[]");
            }
            return;
        }
        if (expression instanceof CreateMap && targetType instanceof MapType) {
            CreateMap map = (CreateMap) expression;
            MapType target = (MapType) targetType;
            for (int i = 0; i < map.arity(); i += 2) {
                validateExpression(map.getArgument(i), target.getKeyType(), columnName + ".key");
                validateExpression(map.getArgument(i + 1), target.getValueType(), columnName + ".value");
            }
            return;
        }
        if ((expression instanceof CreateStruct || expression instanceof CreateNamedStruct)
                && targetType instanceof StructType) {
            StructType target = (StructType) targetType;
            int step = expression instanceof CreateNamedStruct ? 2 : 1;
            if (expression.arity() != target.getFields().size() * step) {
                throw new AnalysisException("Spatial connector write struct fields do not match: " + columnName);
            }
            for (int i = 0; i < target.getFields().size(); i++) {
                validateExpression(expression.child(i * step + step - 1), target.getFields().get(i).getType(),
                        columnName + "." + target.getFields().get(i).getName());
            }
            return;
        }
        validateType(expression.getDataType().toCatalogDataType(), targetType, columnName);
    }

    private static void validateType(Type sourceType, Type targetType, String columnName) {
        if (sourceType.isNull() || !targetType.typeContainsSpatialType()) {
            return;
        }
        if (targetType instanceof ArrayType && sourceType instanceof ArrayType) {
            validateType(((ArrayType) sourceType).getItemType(), ((ArrayType) targetType).getItemType(),
                    columnName + "[]");
            return;
        }
        if (targetType instanceof MapType && sourceType instanceof MapType) {
            validateType(((MapType) sourceType).getKeyType(), ((MapType) targetType).getKeyType(), columnName + ".key");
            validateType(((MapType) sourceType).getValueType(), ((MapType) targetType).getValueType(),
                    columnName + ".value");
            return;
        }
        if (targetType instanceof StructType && sourceType instanceof StructType) {
            StructType source = (StructType) sourceType;
            StructType target = (StructType) targetType;
            if (source.getFields().size() != target.getFields().size()) {
                throw new AnalysisException("Spatial connector write struct fields do not match: " + columnName);
            }
            for (int i = 0; i < target.getFields().size(); i++) {
                validateType(source.getFields().get(i).getType(), target.getFields().get(i).getType(),
                        columnName + "." + target.getFields().get(i).getName());
            }
            return;
        }
        if (!(targetType instanceof ScalarType) || !((ScalarType) targetType).isSpatialType()) {
            throw new AnalysisException("Spatial connector write cannot convert input column '" + columnName
                    + "' from " + sourceType.toSql() + " to " + targetType.toSql());
        }
        validateSpatialType(sourceType, (ScalarType) targetType, columnName);
    }

    private static void validateSpatialType(Type sourceType, ScalarType targetType, String columnName) {
        if (sourceType.isNull()) {
            return;
        }
        if (!(sourceType instanceof ScalarType) || !((ScalarType) sourceType).isSpatialType()) {
            throw new AnalysisException("Spatial connector write cannot convert input column '" + columnName
                    + "' from " + sourceType.toSql() + " to " + targetType.toSql());
        }
        ScalarType source = (ScalarType) sourceType;
        if (source.getPrimitiveType() != targetType.getPrimitiveType()) {
            throw new AnalysisException("Spatial connector write type mismatch for column '" + columnName
                    + "': source " + source.toSql() + " does not match target " + targetType.toSql());
        }
        if (!Objects.equals(source.getSpatialCrs(), targetType.getSpatialCrs())) {
            throw new AnalysisException("Spatial connector write CRS mismatch for column '" + columnName
                    + "': source " + source.getSpatialCrs() + " does not match target "
                    + targetType.getSpatialCrs());
        }
        if (!Objects.equals(source.getSpatialAlgorithm(), targetType.getSpatialAlgorithm())) {
            throw new AnalysisException("Spatial connector write algorithm mismatch for column '" + columnName
                    + "': source " + source.getSpatialAlgorithm() + " does not match target "
                    + targetType.getSpatialAlgorithm());
        }
    }
}
