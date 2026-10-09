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
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Replace;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Unhex;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.UuidLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

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
            UUID uuid = new UuidLiteral(((StringLikeLiteral) input).getStringValue()).getValue();
            return new VarBinaryLiteral((VarBinaryType) targetType, ByteBuffer.allocate(16)
                    .putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array());
        }
        // Validate UUID text strictly before decoding canonical hex. A raw string-to-binary cast
        // would write 36 text bytes, and a permissive UUID cast would silently turn bad input into NULL.
        Expression uuidText = new Cast(new Cast(input, semanticType, false, true), StringType.INSTANCE);
        Expression hex = TypeCoercionUtils.processBoundFunction(
                new Replace(uuidText, new StringLiteral("-"), new StringLiteral("")));
        Expression bytes = TypeCoercionUtils.processBoundFunction(new Unhex(hex));
        return new Cast(bytes, targetType);
    }

    private static boolean needsConversion(DataType input, DataType semantic) {
        if (semantic instanceof UuidType) {
            return input.isStringLikeType();
        }
        if (semantic instanceof ArrayType && input instanceof ArrayType) {
            return needsConversion(((ArrayType) input).getItemType(), ((ArrayType) semantic).getItemType());
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
