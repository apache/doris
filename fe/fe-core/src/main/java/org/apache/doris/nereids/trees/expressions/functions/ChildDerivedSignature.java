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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A function whose signature embeds type metadata derived directly from its children.
 *
 * <p>This hook runs only after the framework has reused an already-resolved signature. Implementations rebuild the
 * same function shape from the current children; they must not search overloads or rerun generic signature
 * computation. This keeps binding, coercion, and precision decisions frozen while allowing nested complex-type
     * metadata to follow equivalent child rewrites.</p>
 */
public interface ChildDerivedSignature extends ComputeSignature {

    /**
     * Refresh only nested container metadata from a rewritten child while retaining resolved scalar leaf types.
     *
     * <p>The resolved type owns overload, coercion, scalar precision, container kind, and struct arity. The current
     * type contributes struct field names and nullability within that frozen shape. An existing scalar leaf is safe
     * when the current child already has the resolved/coerced type, or when it still has the exact immediate-origin
     * raw type that produced the frozen binding. A different leaf or incompatible container shape fails closed.</p>
     */
    static DataType refreshNestedTypeMetadata(
            DataType resolvedType, DataType currentType, DataType immediateOriginType) {
        return mergeNestedTypeMetadata(
                resolvedType, ImmutableList.of(currentType), ImmutableList.of(immediateOriginType))
                .orElseThrow(() -> new AnalysisException(
                        "Cannot safely refresh nested metadata for incompatible resolved, current, and origin types: "
                                + resolvedType + ", " + currentType + ", and " + immediateOriginType));
    }

    /**
     * Merge nested metadata from every current input while requiring the resolved scalar leaves.
     *
     * <p>This is a metadata-only counterpart to common-type inference for a reused signature. Struct fields use
     * the first current input as the canonical name/comment shape and admit NULL whenever any current input field
     * does. Container kind and struct arity must still match the resolved shape; otherwise no frozen scalar binding
     * exists for the new layout and the caller must fail closed instead of recomputing a signature. A typed NULL is
     * accepted because its type already equals the frozen binding. A bare NULL is accepted only when that exact bare
     * NULL was already present in the immediate origin; introducing a new bare NULL fails closed.</p>
     */
    static Optional<DataType> mergeNestedTypeMetadata(
            DataType resolvedType, List<DataType> currentTypes, List<DataType> immediateOriginTypes) {
        if (currentTypes.isEmpty() || currentTypes.size() != immediateOriginTypes.size()) {
            return Optional.empty();
        }
        List<DataType> checkedCurrentTypes = new ArrayList<>(currentTypes.size());
        List<DataType> checkedOriginTypes = new ArrayList<>(currentTypes.size());
        for (int i = 0; i < currentTypes.size(); i++) {
            DataType currentType = currentTypes.get(i);
            DataType originType = immediateOriginTypes.get(i);
            if (hasSameTypeMetadata(resolvedType, currentType)) {
                checkedCurrentTypes.add(resolvedType);
                checkedOriginTypes.add(resolvedType);
            } else if (!resolvedType.isNullType()
                    && currentType.isNullType() && originType.isNullType()) {
                // Preserve a coercion already proved for the same bare NULL in the immediate origin.
                checkedCurrentTypes.add(resolvedType);
                checkedOriginTypes.add(resolvedType);
            } else {
                if (resolvedType.isNullType()) {
                    return Optional.empty();
                }
                checkedCurrentTypes.add(currentType);
                checkedOriginTypes.add(originType);
            }
        }
        if (checkedCurrentTypes.stream()
                .allMatch(currentType -> hasSameTypeMetadata(resolvedType, currentType))) {
            return Optional.of(resolvedType);
        }
        if (resolvedType instanceof ArrayType) {
            List<DataType> currentItemTypes = new ArrayList<>(checkedCurrentTypes.size());
            List<DataType> originItemTypes = new ArrayList<>(checkedCurrentTypes.size());
            for (int i = 0; i < checkedCurrentTypes.size(); i++) {
                DataType currentType = checkedCurrentTypes.get(i);
                DataType originType = checkedOriginTypes.get(i);
                if (!(currentType instanceof ArrayType) || !(originType instanceof ArrayType)) {
                    return Optional.empty();
                }
                currentItemTypes.add(((ArrayType) currentType).getItemType());
                originItemTypes.add(((ArrayType) originType).getItemType());
            }
            return mergeNestedTypeMetadata(
                    ((ArrayType) resolvedType).getItemType(), currentItemTypes, originItemTypes)
                    .map(ArrayType::of);
        }
        if (resolvedType instanceof MapType) {
            List<DataType> currentKeyTypes = new ArrayList<>(checkedCurrentTypes.size());
            List<DataType> currentValueTypes = new ArrayList<>(checkedCurrentTypes.size());
            List<DataType> originKeyTypes = new ArrayList<>(checkedCurrentTypes.size());
            List<DataType> originValueTypes = new ArrayList<>(checkedCurrentTypes.size());
            for (int i = 0; i < checkedCurrentTypes.size(); i++) {
                DataType currentType = checkedCurrentTypes.get(i);
                DataType originType = checkedOriginTypes.get(i);
                if (!(currentType instanceof MapType) || !(originType instanceof MapType)) {
                    return Optional.empty();
                }
                currentKeyTypes.add(((MapType) currentType).getKeyType());
                currentValueTypes.add(((MapType) currentType).getValueType());
                originKeyTypes.add(((MapType) originType).getKeyType());
                originValueTypes.add(((MapType) originType).getValueType());
            }
            Optional<DataType> keyType = mergeNestedTypeMetadata(
                    ((MapType) resolvedType).getKeyType(), currentKeyTypes, originKeyTypes);
            Optional<DataType> valueType = mergeNestedTypeMetadata(
                    ((MapType) resolvedType).getValueType(), currentValueTypes, originValueTypes);
            return keyType.isPresent() && valueType.isPresent()
                    ? Optional.of(MapType.of(keyType.get(), valueType.get())) : Optional.empty();
        }
        if (resolvedType instanceof StructType) {
            List<StructField> resolvedFields = ((StructType) resolvedType).getFields();
            List<List<StructField>> currentFields = new ArrayList<>(checkedCurrentTypes.size());
            List<List<StructField>> originFields = new ArrayList<>(checkedCurrentTypes.size());
            for (int i = 0; i < checkedCurrentTypes.size(); i++) {
                DataType currentType = checkedCurrentTypes.get(i);
                DataType originType = checkedOriginTypes.get(i);
                if (!(currentType instanceof StructType) || !(originType instanceof StructType)) {
                    return Optional.empty();
                }
                List<StructField> fields = ((StructType) currentType).getFields();
                List<StructField> oldFields = ((StructType) originType).getFields();
                if (fields.size() != resolvedFields.size() || oldFields.size() != resolvedFields.size()) {
                    return Optional.empty();
                }
                currentFields.add(fields);
                originFields.add(oldFields);
            }
            ImmutableList.Builder<StructField> mergedFields = ImmutableList.builderWithExpectedSize(
                    resolvedFields.size());
            for (int i = 0; i < resolvedFields.size(); i++) {
                List<DataType> fieldTypes = new ArrayList<>(currentFields.size());
                List<DataType> originFieldTypes = new ArrayList<>(currentFields.size());
                boolean nullable = false;
                for (int j = 0; j < currentFields.size(); j++) {
                    StructField field = currentFields.get(j).get(i);
                    fieldTypes.add(field.getDataType());
                    originFieldTypes.add(originFields.get(j).get(i).getDataType());
                    nullable |= field.isNullable();
                }
                Optional<DataType> fieldType = mergeNestedTypeMetadata(
                        resolvedFields.get(i).getDataType(), fieldTypes, originFieldTypes);
                if (!fieldType.isPresent()) {
                    return Optional.empty();
                }
                mergedFields.add(currentFields.get(0).get(i)
                        .withDataTypeAndNullable(fieldType.get(), nullable));
            }
            return Optional.of(new StructType(mergedFields.build()));
        }
        for (int i = 0; i < checkedCurrentTypes.size(); i++) {
            if (!hasSameTypeMetadata(checkedCurrentTypes.get(i), checkedOriginTypes.get(i))) {
                return Optional.empty();
            }
        }
        return Optional.of(resolvedType);
    }

    /** Derive the signature metadata owned by this function from its current children. */
    FunctionSignature deriveSignatureFromChildren(
            FunctionSignature resolvedSignature, List<Expression> immediateOriginArguments);

    @Override
    default FunctionSignature refreshDerivedSignature(
            FunctionSignature resolvedSignature, List<Expression> immediateOriginArguments) {
        FunctionSignature refreshed = deriveSignatureFromChildren(
                resolvedSignature, immediateOriginArguments);
        return hasSameSignatureMetadata(resolvedSignature, refreshed) ? resolvedSignature : refreshed;
    }

    /** Compare a signature including nested field metadata intentionally omitted from type equality. */
    static boolean hasSameSignatureMetadata(FunctionSignature left, FunctionSignature right) {
        if (left == right) {
            return true;
        }
        if (left.hasVarArgs != right.hasVarArgs
                || left.argumentsTypes.size() != right.argumentsTypes.size()
                || !hasSameTypeMetadata(left.returnType, right.returnType)) {
            return false;
        }
        for (int i = 0; i < left.argumentsTypes.size(); i++) {
            if (!hasSameTypeMetadata(left.argumentsTypes.get(i), right.argumentsTypes.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** Compare a type recursively, including all StructField metadata at every nesting level. */
    static boolean hasSameTypeMetadata(DataType left, DataType right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null || left.getClass() != right.getClass()) {
            return false;
        }
        if (left instanceof ArrayType) {
            return hasSameTypeMetadata(
                    ((ArrayType) left).getItemType(), ((ArrayType) right).getItemType());
        }
        if (left instanceof MapType) {
            MapType leftMap = (MapType) left;
            MapType rightMap = (MapType) right;
            return hasSameTypeMetadata(leftMap.getKeyType(), rightMap.getKeyType())
                    && hasSameTypeMetadata(leftMap.getValueType(), rightMap.getValueType());
        }
        if (left instanceof StructType) {
            List<StructField> leftFields = ((StructType) left).getFields();
            List<StructField> rightFields = ((StructType) right).getFields();
            if (leftFields.size() != rightFields.size()) {
                return false;
            }
            for (int i = 0; i < leftFields.size(); i++) {
                StructField leftField = leftFields.get(i);
                StructField rightField = rightFields.get(i);
                if (!leftField.hasSameMetadata(rightField)
                        || !hasSameTypeMetadata(leftField.getDataType(), rightField.getDataType())) {
                    return false;
                }
            }
            return true;
        }
        return left.equals(right);
    }
}
