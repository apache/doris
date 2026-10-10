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

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapEntries;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapFromEntries;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.UuidLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Applies connector-declared textual input semantics before ordinary sink type coercion. */
public final class ConnectorWriteValueConverter {
    private ConnectorWriteValueConverter() {
    }

    public static Expression convert(Column column, Expression input) {
        if (column.getConnectorStringWriteType() == null) {
            return input;
        }
        return convert(input, DataType.fromCatalogType(column.getConnectorStringWriteType()),
                DataType.fromCatalogType(column.getType()));
    }

    private static Expression convert(Expression input, DataType semanticType, DataType targetType) {
        if (!needsConversion(input.getDataType(), semanticType)) {
            return input;
        }
        if (semanticType instanceof ArrayType) {
            DataType elementSemantic = ((ArrayType) semanticType).getItemType();
            DataType elementTarget = ((ArrayType) targetType).getItemType();
            ArrayItemReference item = new ArrayItemReference("item", input);
            return new ArrayMap(new Lambda(Collections.singletonList(item.getName()),
                    convert(item.toSlot(), elementSemantic, elementTarget), Collections.singletonList(item)));
        }
        if (semanticType instanceof MapType) {
            MapType semantics = (MapType) semanticType;
            MapType target = (MapType) targetType;
            if (input instanceof CreateMap) {
                List<Expression> children = new ArrayList<>();
                for (int i = 0; i < input.arity(); i++) {
                    DataType semantic = i % 2 == 0 ? semantics.getKeyType() : semantics.getValueType();
                    DataType targetChild = i % 2 == 0 ? target.getKeyType() : target.getValueType();
                    Expression child = input.child(i);
                    // A NULL-only map value has already been assigned TINYINT by generic function binding.
                    children.add(child instanceof NullLiteral ? new NullLiteral(targetChild)
                            : convert(child, semantic, targetChild));
                }
                return TypeCoercionUtils.processBoundFunction(new CreateMap(children.toArray(new Expression[0])));
            }
            if (input instanceof MapLiteral) {
                // Preserve NULL leaves before collection binding assigns placeholder types.
                return ((MapLiteral) input).checkedCastWithStrictChecking(targetType);
            }
            // Convert entries together so volatile map expressions are evaluated once and keys stay paired.
            Expression entries = TypeCoercionUtils.processBoundFunction(new MapEntries(input));
            return TypeCoercionUtils.processBoundFunction(new MapFromEntries(
                    convert(entries, mapEntryArrayType(semantics), mapEntryArrayType(target))));
        }
        if (semanticType instanceof StructType) {
            List<StructField> semanticFields = ((StructType) semanticType).getFields();
            List<StructField> targetFields = ((StructType) targetType).getFields();
            List<Expression> fields = new ArrayList<>();
            for (int i = 0; i < semanticFields.size(); i++) {
                Expression field = input instanceof StructLiteral ? ((StructLiteral) input).getValue().get(i)
                        : TypeCoercionUtils.processBoundFunction(new ElementAt(input, new IntegerLiteral(i + 1)));
                fields.add(new StringLiteral(targetFields.get(i).getName()));
                fields.add(convert(field, semanticFields.get(i).getDataType(), targetFields.get(i).getDataType()));
            }
            Expression result = TypeCoercionUtils.processBoundFunction(
                    new CreateNamedStruct(fields.toArray(new Expression[0])));
            // Reconstructing a nullable struct must not turn NULL into a non-null struct of NULL fields.
            return input.nullable() ? TypeCoercionUtils.processBoundFunction(
                    new If(new IsNull(input), new NullLiteral(result.getDataType()), result)) : result;
        }
        if (input instanceof StringLikeLiteral) {
            return new UuidLiteral(((StringLikeLiteral) input).getStringValue());
        }
        // Reject malformed UUID text on write instead of silently replacing it with NULL.
        return new Cast(input, targetType, false, true);
    }

    private static ArrayType mapEntryArrayType(MapType type) {
        return ArrayType.of(new StructType(java.util.Arrays.asList(
                new StructField("key", type.getKeyType(), true, ""),
                new StructField("value", type.getValueType(), true, ""))));
    }

    private static boolean needsConversion(DataType input, DataType semantic) {
        if (semantic instanceof UuidType) {
            return input.isStringLikeType();
        }
        if (semantic instanceof ArrayType && input instanceof ArrayType) {
            return needsConversion(((ArrayType) input).getItemType(), ((ArrayType) semantic).getItemType());
        }
        if (semantic instanceof MapType && input instanceof MapType) {
            return needsConversion(((MapType) input).getKeyType(), ((MapType) semantic).getKeyType())
                    || needsConversion(((MapType) input).getValueType(), ((MapType) semantic).getValueType());
        }
        if (semantic instanceof StructType && input instanceof StructType) {
            List<StructField> inputs = ((StructType) input).getFields();
            List<StructField> semantics = ((StructType) semantic).getFields();
            if (inputs.size() != semantics.size()) {
                // Ordinary sink coercion reports the incompatible struct arity.
                return false;
            }
            for (int i = 0; i < inputs.size(); i++) {
                if (needsConversion(inputs.get(i).getDataType(), semantics.get(i).getDataType())) {
                    return true;
                }
            }
        }
        return false;
    }

}
