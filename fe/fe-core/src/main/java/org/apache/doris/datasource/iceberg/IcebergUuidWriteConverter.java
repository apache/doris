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

import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
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
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Validates UUID text before ordinary Iceberg sink coercion, including nested leaves. */
public final class IcebergUuidWriteConverter {
    private IcebergUuidWriteConverter() {
    }

    public static Expression convert(Expression input, DataType targetType) {
        return TypeCoercionUtils.castIfNotSameType(convertUuid(input, targetType), targetType);
    }

    public static boolean containsUuid(DataType type) {
        if (type instanceof ArrayType) {
            return containsUuid(((ArrayType) type).getItemType());
        }
        if (type instanceof MapType) {
            return containsUuid(((MapType) type).getKeyType()) || containsUuid(((MapType) type).getValueType());
        }
        if (type instanceof StructType) {
            return ((StructType) type).getFields().stream().anyMatch(field -> containsUuid(field.getDataType()));
        }
        return type.isUuidType();
    }

    private static Expression convertUuid(Expression input, DataType targetType) {
        if (!needsConversion(input.getDataType(), targetType)) {
            return input;
        }
        // Parse serialized containers with UUID leaves kept as text. Nullable complex SerDes
        // otherwise consume malformed UUIDs as NULL before the write validator can see them.
        input = TypeCoercionUtils.castIfNotSameType(input,
                uuidIntermediateType(input.getDataType(), targetType, true));
        if (targetType instanceof ArrayType) {
            DataType elementTarget = ((ArrayType) targetType).getItemType();
            ArrayItemReference item = new ArrayItemReference("item", input);
            return new ArrayMap(new Lambda(Collections.singletonList(item.getName()),
                    convertUuid(item.toSlot(), elementTarget), Collections.singletonList(item)));
        }
        if (targetType instanceof MapType) {
            MapType target = (MapType) targetType;
            if (input instanceof CreateMap || input instanceof MapLiteral) {
                List<Expression> source = new ArrayList<>();
                if (input instanceof MapLiteral) {
                    ((MapLiteral) input).getValue().forEach((key, value) -> {
                        source.add(key);
                        source.add(value);
                    });
                } else {
                    source.addAll(input.children());
                }
                List<Expression> children = new ArrayList<>();
                for (int i = 0; i < source.size(); i++) {
                    DataType targetChild = i % 2 == 0 ? target.getKeyType() : target.getValueType();
                    Expression child = source.get(i);
                    // A NULL-only map value has already been assigned TINYINT by generic function binding.
                    children.add(child instanceof NullLiteral ? new NullLiteral(targetChild)
                            : convertUuid(child, targetChild));
                }
                return TypeCoercionUtils.processBoundFunction(new CreateMap(children.toArray(new Expression[0])));
            }
            // Evaluate the map once, but change only UUID text leaves in the strict cast.
            // Narrowing non-UUID siblings here would override ordinary sink cast semantics.
            return new Cast(input, uuidIntermediateType(input.getDataType(), targetType, false), false, true);
        }
        if (targetType instanceof StructType) {
            if (!(input instanceof SlotReference) && !(input instanceof StructLiteral)) {
                // Bind the record once before null testing and field extraction; VALUES inputs may
                // be volatile and have no source slot yet. The singleton array supplies that binding.
                Expression singleton = TypeCoercionUtils.processBoundFunction(new Array(input));
                Expression converted = convertUuid(singleton, ArrayType.of(targetType));
                return TypeCoercionUtils.processBoundFunction(new ElementAt(converted, new IntegerLiteral(1)));
            }
            List<StructField> targetFields = ((StructType) targetType).getFields();
            List<Expression> fields = new ArrayList<>();
            for (int i = 0; i < targetFields.size(); i++) {
                Expression field = input instanceof StructLiteral ? ((StructLiteral) input).getValue().get(i)
                        : TypeCoercionUtils.processBoundFunction(new ElementAt(input, new IntegerLiteral(i + 1)));
                fields.add(new StringLiteral(targetFields.get(i).getName()));
                fields.add(convertUuid(field, targetFields.get(i).getDataType()));
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

    private static boolean needsConversion(DataType input, DataType semantic) {
        if (input.isStringLikeType() && containsUuid(semantic)) {
            return true;
        }
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

    /** Preserve source siblings while parsing containers or validating UUID text leaves. */
    private static DataType uuidIntermediateType(DataType input, DataType target, boolean parseContainers) {
        if (input.isStringLikeType() && containsUuid(target)) {
            if (target.isUuidType()) {
                return parseContainers ? input : target;
            }
            return parseContainers ? uuidTextType(target) : input;
        }
        if (input instanceof ArrayType && target instanceof ArrayType) {
            return ArrayType.of(uuidIntermediateType(((ArrayType) input).getItemType(),
                    ((ArrayType) target).getItemType(), parseContainers));
        }
        if (input instanceof MapType && target instanceof MapType) {
            return MapType.of(uuidIntermediateType(((MapType) input).getKeyType(),
                    ((MapType) target).getKeyType(), parseContainers),
                    uuidIntermediateType(((MapType) input).getValueType(),
                            ((MapType) target).getValueType(), parseContainers));
        }
        if (input instanceof StructType && target instanceof StructType) {
            List<StructField> sourceFields = ((StructType) input).getFields();
            List<StructField> targetFields = ((StructType) target).getFields();
            if (sourceFields.size() == targetFields.size()) {
                List<StructField> fields = new ArrayList<>();
                for (int i = 0; i < sourceFields.size(); i++) {
                    StructField source = sourceFields.get(i);
                    fields.add(source.withDataType(uuidIntermediateType(source.getDataType(),
                            targetFields.get(i).getDataType(), parseContainers)));
                }
                return new StructType(fields);
            }
        }
        return input;
    }

    private static DataType uuidTextType(DataType type) {
        if (type.isUuidType()) {
            return StringType.INSTANCE;
        }
        if (type instanceof ArrayType) {
            return ArrayType.of(uuidTextType(((ArrayType) type).getItemType()));
        }
        if (type instanceof MapType) {
            return MapType.of(uuidTextType(((MapType) type).getKeyType()),
                    uuidTextType(((MapType) type).getValueType()));
        }
        if (type instanceof StructType) {
            List<StructField> fields = new ArrayList<>();
            for (StructField field : ((StructType) type).getFields()) {
                fields.add(field.withDataType(uuidTextType(field.getDataType())));
            }
            return new StructType(fields);
        }
        return type;
    }

}
