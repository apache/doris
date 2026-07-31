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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.FloatType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Executable functions for array operations.
 */
public class ArrayArithmetic {

    /**
     * Compute the cross product between two 3D float arrays.
     */
    @ExecFunction(name = "array_cross_product")
    public static Expression arrayCrossProduct(ArrayLiteral array1, ArrayLiteral array2) {
        return crossProduct("array_cross_product", array1, array2);
    }

    /**
     * Alias for array_cross_product.
     */
    @ExecFunction(name = "cross_product")
    public static Expression crossProduct(ArrayLiteral array1, ArrayLiteral array2) {
        return crossProduct("cross_product", array1, array2);
    }

    private static Expression crossProduct(String functionName, ArrayLiteral array1, ArrayLiteral array2) {
        List<Literal> items1 = array1.getValue();
        List<Literal> items2 = array2.getValue();
        if (items1.size() != 3 || items2.size() != 3) {
            throw new AnalysisException("function " + functionName
                    + " requires both input arrays to have exactly 3 elements, got "
                    + items1.size() + " and " + items2.size());
        }
        validateNoNull(functionName, items1);
        validateNoNull(functionName, items2);

        float x0 = floatValue(items1.get(0));
        float x1 = floatValue(items1.get(1));
        float x2 = floatValue(items1.get(2));
        float y0 = floatValue(items2.get(0));
        float y1 = floatValue(items2.get(1));
        float y2 = floatValue(items2.get(2));

        return new ArrayLiteral(ImmutableList.of(
                new FloatLiteral(x1 * y2 - x2 * y1),
                new FloatLiteral(x2 * y0 - x0 * y2),
                new FloatLiteral(x0 * y1 - x1 * y0)),
                ArrayType.of(FloatType.INSTANCE));
    }

    private static void validateNoNull(String functionName, List<Literal> items) {
        for (Literal item : items) {
            if (item instanceof NullLiteral) {
                throw new AnalysisException("function " + functionName + " cannot have null");
            }
        }
    }

    private static float floatValue(Literal literal) {
        return ((Number) literal.getValue()).floatValue();
    }

    /**
     * Compute cosine similarity between two float arrays.
     * cosine_similarity(x, y) = dot(x, y) / (||x|| * ||y||)
     */
    @ExecFunction(name = "cosine_similarity")
    public static Expression cosineSimilarity(ArrayLiteral array1, ArrayLiteral array2) {
        List<Literal> items1 = array1.getValue();
        List<Literal> items2 = array2.getValue();

        // Check for null elements
        for (Literal item : items1) {
            if (item instanceof NullLiteral) {
                throw new AnalysisException("function cosine_similarity cannot have null");
            }
        }
        for (Literal item : items2) {
            if (item instanceof NullLiteral) {
                throw new AnalysisException("function cosine_similarity cannot have null");
            }
        }

        // Check array sizes
        if (items1.size() != items2.size()) {
            throw new AnalysisException("function cosine_similarity have different input element sizes of array: "
                    + items1.size() + " and " + items2.size());
        }

        // Handle empty arrays
        if (items1.isEmpty()) {
            return new FloatLiteral(0.0f);
        }

        // Compute dot product and squared norms
        double dotProd = 0.0;
        double squaredX = 0.0;
        double squaredY = 0.0;

        for (int i = 0; i < items1.size(); i++) {
            double x = ((Number) items1.get(i).getValue()).doubleValue();
            double y = ((Number) items2.get(i).getValue()).doubleValue();
            dotProd += x * y;
            squaredX += x * x;
            squaredY += y * y;
        }

        // Handle zero vectors
        if (squaredX == 0.0 || squaredY == 0.0) {
            return new FloatLiteral(0.0f);
        }

        float result = (float) (dotProd / Math.sqrt(squaredX * squaredY));
        return new FloatLiteral(result);
    }
}
