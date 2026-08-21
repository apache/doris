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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Validates the internal ArrayMap used to evaluate map entries.
 */
public final class MapLambdaValidator {

    private MapLambdaValidator() {
    }

    // Require a bound lambda argument.
    public static Lambda requireLambda(String functionName, Expression expression) {
        if (!(expression instanceof Lambda)) {
            throw new AnalysisException(String.format(
                    "The 1st arg of %s must be lambda but is %s", functionName, expression));
        }
        return (Lambda) expression;
    }

    // A Map lambda expands one Map into the original Map, map_keys(map), and map_values(map).
    // Computed Maps are candidates for materialization. Slots are already materialized, and Map
    // literals do not contain repeated computation.
    public static boolean requiresSingleEvaluation(Expression mapExpression) {
        while (mapExpression instanceof Cast) {
            mapExpression = mapExpression.child(0);
        }
        return !(mapExpression instanceof Slot || mapExpression instanceof MapLiteral);
    }

    /**
     * Validate and return the Map shared by the first key and value references.
     */
    public static Expression extractMapExpression(String functionName, Lambda lambda) {
        List<ArrayItemReference> arguments = lambda.getLambdaArguments();
        if (arguments.size() < 2) {
            throw new AnalysisException(String.format(
                    "Internal map entry lambda of %s must have key and value inputs", functionName));
        }
        Expression keyArray = arguments.get(0).getArrayExpression();
        Expression valueArray = arguments.get(1).getArrayExpression();
        if (!(keyArray instanceof MapKeys) || !(valueArray instanceof MapValues)) {
            throw new AnalysisException(String.format(
                    "Internal map entry lambda of %s must have key and value inputs", functionName));
        }
        Expression keyMap = keyArray.child(0);
        Expression valueMap = valueArray.child(0);
        if (!keyMap.equals(valueMap)) {
            throw new AnalysisException(String.format(
                    "Map entry inputs of %s must come from the same map", functionName));
        }
        return keyMap;
    }

    // Fill only NULL_TYPE positions in a Map lambda result from the matching input key or value
    // type. For transform_values((k, v) -> [], map(1, [10])), infer ARRAY<TINYINT> for the empty
    // array. Apply the same inference to nested Array, Struct, and Map types.
    static DataType mergeNestedNullTypes(DataType outputType, DataType inputType) {
        if (outputType.isNullType()) {
            return inputType;
        } else if (outputType instanceof ArrayType && inputType instanceof ArrayType) {
            return ArrayType.of(mergeNestedNullTypes(
                    ((ArrayType) outputType).getItemType(), ((ArrayType) inputType).getItemType()));
        } else if (outputType instanceof MapType && inputType instanceof MapType) {
            return MapType.of(
                    mergeNestedNullTypes(
                            ((MapType) outputType).getKeyType(), ((MapType) inputType).getKeyType()),
                    mergeNestedNullTypes(
                            ((MapType) outputType).getValueType(), ((MapType) inputType).getValueType()));
        } else if (outputType instanceof StructType && inputType instanceof StructType) {
            List<StructField> outputFields = ((StructType) outputType).getFields();
            List<StructField> inputFields = ((StructType) inputType).getFields();
            if (outputFields.size() != inputFields.size()) {
                return outputType;
            }
            ImmutableList.Builder<StructField> fields
                    = ImmutableList.builderWithExpectedSize(outputFields.size());
            for (int i = 0; i < outputFields.size(); i++) {
                fields.add(outputFields.get(i).withDataType(mergeNestedNullTypes(
                        outputFields.get(i).getDataType(), inputFields.get(i).getDataType())));
            }
            return new StructType(fields.build());
        }
        return outputType;
    }

    /**
     * Revalidate the hidden physical arrays after optimizer rewrites.
     */
    public static void validateStablePhysicalInputs(String functionName, Lambda lambda) {
        List<ArrayItemReference> arguments = lambda.getLambdaArguments();
        if (arguments.isEmpty()) {
            throw new AnalysisException(String.format(
                    "Internal map entry lambda of %s must have key and value inputs", functionName));
        }
        // Projection CSE can replace only one of map_keys(M) and map_values(M) with a Slot. The
        // analysis-time constructor already checked their common Map lineage, so repeating that
        // structural check here would reject a valid partially materialized marker. Translation
        // only needs the key/value driver arrays to be stable; ArrayMap checks equal lengths.
        // Additional arguments are hidden arrays used to materialize nested lambda expressions.
        // They are deliberately allowed to be volatile because each hidden array is evaluated
        // once and then consumed through its item Slot by the owning lambda.
        int driverCount = Math.min(2, arguments.size());
        for (int i = 0; i < driverCount; i++) {
            ArrayItemReference argument = arguments.get(i);
            if (argument.getArrayExpression().containsVolatileExpression()) {
                throw new AnalysisException(String.format(
                        "Internal map entry input of %s must be materialized before translation",
                        functionName));
            }
        }
    }

    /**
     * Validate functions that consume both the original Map and a mapped entry array.
     */
    public static void validateOuterMapConsumer(String functionName, Expression mappedArray) {
        Expression marker = mappedArray;
        while (marker instanceof Cast) {
            marker = marker.child(0);
        }
        // CSE can materialize the whole MapEntryArrayMap in an earlier projection layer when the
        // enclosing Map function is referenced more than once. The marker was validated while
        // translating that layer, so its projected slot is a valid physical input here.
        if (marker instanceof Slot) {
            return;
        }
        if (!(marker instanceof MapEntryArrayMap)) {
            throw new AnalysisException(String.format(
                    "Mapped entry input of %s lost its internal map entry marker", functionName));
        }
        Lambda lambda = (Lambda) marker.child(0);
        validateStablePhysicalInputs(functionName, lambda);
    }

}
