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
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToArgumentType;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
     * Refresh standard Any/Follow signatures without asking every passthrough function to implement the same logic.
     * The selected signature supplies the dependency graph and the resolved signature supplies every frozen scalar
     * leaf. Only bindings that contain a struct are visited because struct fields are the complex-type metadata that
     * can change during AdjustNullable.
     */
    static FunctionSignature refreshFollowTypeMetadata(
            FunctionSignature selectedSignature, FunctionSignature resolvedSignature,
            List<Expression> immediateOriginArguments, List<Expression> currentArguments) {
        if (!selectedSignature.hasVarArgs
                && currentArguments.size() != resolvedSignature.argumentsTypes.size()) {
            return resolvedSignature;
        }

        Map<Integer, MetadataBinding> indexedBindings = new HashMap<>();
        Map<String, MetadataBinding> positionalBindings = new HashMap<>();
        for (int i = 0; i < currentArguments.size(); i++) {
            DataType resolvedType = resolvedSignature.getArgType(i);
            DataType originType = i < immediateOriginArguments.size()
                    ? immediateOriginArguments.get(i).getDataType() : resolvedType;
            collectFollowTypeMetadata(
                    selectedSignature.getArgType(i), resolvedType,
                    currentArguments.get(i).getDataType(), originType, i, "",
                    indexedBindings, positionalBindings);
        }
        if (indexedBindings.isEmpty() && positionalBindings.isEmpty()) {
            return resolvedSignature;
        }

        indexedBindings.values().forEach(MetadataBinding::merge);
        positionalBindings.values().forEach(MetadataBinding::merge);
        // Keep the formal signature shape. In particular, a one-slot vararg signature must not
        // become an N-slot vararg signature merely because this expression has N actual children.
        ImmutableList.Builder<DataType> argumentTypes = ImmutableList.builderWithExpectedSize(
                resolvedSignature.argumentsTypes.size());
        for (int i = 0; i < resolvedSignature.argumentsTypes.size(); i++) {
            argumentTypes.add(instantiateFollowTypeMetadata(
                    selectedSignature.getArgType(i), resolvedSignature.getArgType(i),
                    i, "", indexedBindings, positionalBindings, null));
        }
        ImmutableList<DataType> refreshedArgumentTypes = argumentTypes.build();
        DataType returnType = instantiateFollowTypeMetadata(
                selectedSignature.returnType, resolvedSignature.returnType,
                -1, "return", indexedBindings, positionalBindings, refreshedArgumentTypes);
        FunctionSignature refreshedSignature = resolvedSignature
                .withArgumentTypes(resolvedSignature.hasVarArgs, refreshedArgumentTypes)
                .withReturnType(returnType);
        return hasSameSignatureMetadata(resolvedSignature, refreshedSignature)
                ? resolvedSignature : refreshedSignature;
    }

    /** Collect current metadata candidates for one Any/Follow binding. */
    static void collectFollowTypeMetadata(
            DataType selectedType, DataType resolvedType, DataType currentType, DataType originType,
            int argumentIndex, String path,
            Map<Integer, MetadataBinding> indexedBindings,
            Map<String, MetadataBinding> positionalBindings) {
        if (!containsStructType(resolvedType)) {
            return;
        }
        if (selectedType instanceof AnyDataType || selectedType instanceof FollowToAnyDataType) {
            int typeIndex = selectedType instanceof AnyDataType
                    ? ((AnyDataType) selectedType).getIndex()
                    : ((FollowToAnyDataType) selectedType).getIndex();
            MetadataBinding binding = typeIndex >= 0
                    ? indexedBindings.computeIfAbsent(typeIndex, key -> new MetadataBinding())
                    : positionalBindings.computeIfAbsent(
                            argumentIndex + path, key -> new MetadataBinding());
            binding.add(resolvedType, currentType, originType);
            return;
        }
        if (selectedType instanceof ArrayType && resolvedType instanceof ArrayType) {
            DataType currentItemType = nestedArrayItemType(currentType);
            DataType originItemType = nestedArrayItemType(originType);
            collectFollowTypeMetadata(
                    ((ArrayType) selectedType).getItemType(),
                    ((ArrayType) resolvedType).getItemType(),
                    currentItemType, originItemType, argumentIndex, path + "[]",
                    indexedBindings, positionalBindings);
        } else if (selectedType instanceof MapType && resolvedType instanceof MapType) {
            DataType currentKeyType = nestedMapKeyType(currentType);
            DataType currentValueType = nestedMapValueType(currentType);
            DataType originKeyType = nestedMapKeyType(originType);
            DataType originValueType = nestedMapValueType(originType);
            collectFollowTypeMetadata(
                    ((MapType) selectedType).getKeyType(),
                    ((MapType) resolvedType).getKeyType(),
                    currentKeyType, originKeyType, argumentIndex, path + ".key",
                    indexedBindings, positionalBindings);
            collectFollowTypeMetadata(
                    ((MapType) selectedType).getValueType(),
                    ((MapType) resolvedType).getValueType(),
                    currentValueType, originValueType, argumentIndex, path + ".value",
                    indexedBindings, positionalBindings);
        }
    }

    /** Instantiate one selected-signature type from the refreshed Any/Follow bindings. */
    static DataType instantiateFollowTypeMetadata(
            DataType selectedType, DataType resolvedType, int argumentIndex, String path,
            Map<Integer, MetadataBinding> indexedBindings,
            Map<String, MetadataBinding> positionalBindings,
            List<DataType> refreshedArgumentTypes) {
        if (selectedType instanceof FollowToArgumentType) {
            int followedArgument = ((FollowToArgumentType) selectedType).argumentIndex;
            if (refreshedArgumentTypes == null || followedArgument >= refreshedArgumentTypes.size()) {
                throw new AnalysisException(
                        "Cannot refresh a FollowToArgumentType without its resolved argument");
            }
            return refreshedArgumentTypes.get(followedArgument);
        }
        if (selectedType instanceof AnyDataType || selectedType instanceof FollowToAnyDataType) {
            int typeIndex = selectedType instanceof AnyDataType
                    ? ((AnyDataType) selectedType).getIndex()
                    : ((FollowToAnyDataType) selectedType).getIndex();
            MetadataBinding binding = typeIndex >= 0
                    ? indexedBindings.get(typeIndex) : positionalBindings.get(argumentIndex + path);
            return binding == null ? resolvedType : binding.getMergedType();
        }
        if (selectedType instanceof ArrayType && resolvedType instanceof ArrayType) {
            return ArrayType.of(instantiateFollowTypeMetadata(
                    ((ArrayType) selectedType).getItemType(),
                    ((ArrayType) resolvedType).getItemType(),
                    argumentIndex, path + "[]", indexedBindings, positionalBindings,
                    refreshedArgumentTypes));
        }
        if (selectedType instanceof MapType && resolvedType instanceof MapType) {
            return MapType.of(
                    instantiateFollowTypeMetadata(
                            ((MapType) selectedType).getKeyType(),
                            ((MapType) resolvedType).getKeyType(),
                            argumentIndex, path + ".key", indexedBindings, positionalBindings,
                            refreshedArgumentTypes),
                    instantiateFollowTypeMetadata(
                            ((MapType) selectedType).getValueType(),
                            ((MapType) resolvedType).getValueType(),
                            argumentIndex, path + ".value", indexedBindings, positionalBindings,
                            refreshedArgumentTypes));
        }
        return resolvedType;
    }

    /** Return an array item type while allowing a bare NULL to be validated by the merge step. */
    static DataType nestedArrayItemType(DataType dataType) {
        if (dataType.isNullType()) {
            return dataType;
        }
        if (!(dataType instanceof ArrayType)) {
            throw new AnalysisException(
                    "Cannot refresh array metadata from a non-array type: " + dataType);
        }
        return ((ArrayType) dataType).getItemType();
    }

    /** Return a map key type while allowing a bare NULL to be validated by the merge step. */
    static DataType nestedMapKeyType(DataType dataType) {
        if (dataType.isNullType()) {
            return dataType;
        }
        if (!(dataType instanceof MapType)) {
            throw new AnalysisException(
                    "Cannot refresh map metadata from a non-map type: " + dataType);
        }
        return ((MapType) dataType).getKeyType();
    }

    /** Return a map value type while allowing a bare NULL to be validated by the merge step. */
    static DataType nestedMapValueType(DataType dataType) {
        if (dataType.isNullType()) {
            return dataType;
        }
        if (!(dataType instanceof MapType)) {
            throw new AnalysisException(
                    "Cannot refresh map metadata from a non-map type: " + dataType);
        }
        return ((MapType) dataType).getValueType();
    }

    /** Whether a type contains struct-field metadata at any nesting level. */
    static boolean containsStructType(DataType dataType) {
        if (dataType instanceof StructType) {
            return true;
        }
        if (dataType instanceof ArrayType) {
            return containsStructType(((ArrayType) dataType).getItemType());
        }
        if (dataType instanceof MapType) {
            return containsStructType(((MapType) dataType).getKeyType())
                    || containsStructType(((MapType) dataType).getValueType());
        }
        return false;
    }

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
            FunctionSignature resolvedSignature, List<Expression> immediateOriginArguments,
            List<Expression> currentArguments);

    @Override
    default FunctionSignature refreshDerivedSignature(
            FunctionSignature selectedSignature, FunctionSignature resolvedSignature,
            List<Expression> immediateOriginArguments, List<Expression> currentArguments) {
        FunctionSignature refreshed = deriveSignatureFromChildren(
                resolvedSignature, immediateOriginArguments, currentArguments);
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

    /** Metadata candidates for one Any/Follow binding in a selected signature. */
    class MetadataBinding {
        private DataType resolvedType;
        private final List<DataType> currentTypes = new ArrayList<>();
        private final List<DataType> originTypes = new ArrayList<>();
        private DataType mergedType;

        void add(DataType resolvedType, DataType currentType, DataType originType) {
            if (this.resolvedType == null) {
                this.resolvedType = resolvedType;
            } else if (!hasSameTypeMetadata(this.resolvedType, resolvedType)) {
                throw new AnalysisException(
                        "Cannot refresh one Any/Follow binding with different resolved types: "
                                + this.resolvedType + " and " + resolvedType);
            }
            currentTypes.add(currentType);
            originTypes.add(originType);
        }

        void merge() {
            mergedType = mergeNestedTypeMetadata(resolvedType, currentTypes, originTypes)
                    .orElseThrow(() -> new AnalysisException(
                            "Cannot safely refresh Any/Follow metadata for resolved type " + resolvedType));
        }

        DataType getMergedType() {
            return mergedType;
        }
    }
}
