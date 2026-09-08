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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Utilities for building Map Lambda functions from one bound Map-entry array driver. */
final class MapLambdaFunctionUtils {

    private MapLambdaFunctionUtils() {
    }

    /** Build the function-specific body around a bound Map-entry Lambda. */
    static RewrittenMapLambda rewrite(Lambda lambda, EntryBodyBuilder bodyBuilder) {
        Expression mapExpression = extractMapExpression(lambda);
        List<ArrayItemReference> arguments = lambda.getLambdaArguments();
        ArrayItemReference entryArgument = arguments.get(0);
        Slot entrySlot = entryArgument.toSlot();
        Expression key = new ElementAt(entrySlot, new IntegerLiteral(1));
        Expression value = new ElementAt(entrySlot, new IntegerLiteral(2));

        Expression rewrittenBody = bodyBuilder.build(lambda.getLambdaFunction(), key, value, entrySlot);
        Lambda rewrittenLambda = new Lambda(
                ImmutableList.of(entryArgument.getName()), rewrittenBody, ImmutableList.of(entryArgument));
        return new RewrittenMapLambda(mapExpression, rewrittenLambda);
    }

    /** Require a bound Lambda argument. */
    static Lambda requireLambda(String functionName, Expression expression) {
        if (!(expression instanceof Lambda)) {
            throw new AnalysisException(String.format(
                    "The 1st arg of %s must be lambda but is %s", functionName, expression));
        }
        return (Lambda) expression;
    }

    private static Expression extractMapExpression(Lambda lambda) {
        List<ArrayItemReference> arguments = lambda.getLambdaArguments();
        Preconditions.checkArgument(arguments.size() == 1,
                "A bound Map Lambda must have one entry argument");
        Expression entries = arguments.get(0).getArrayExpression();
        Preconditions.checkArgument(entries instanceof MapEntries,
                "A bound Map Lambda must use a map_entries argument");
        return entries.child(0);
    }

    @FunctionalInterface
    interface EntryBodyBuilder {
        Expression build(Expression body, Expression key, Expression value, Slot entry);
    }

    /** One original Map and its one-driver entry Lambda. */
    static final class RewrittenMapLambda {
        private final Expression mapExpression;
        private final Lambda lambda;

        private RewrittenMapLambda(Expression mapExpression, Lambda lambda) {
            this.mapExpression = mapExpression;
            this.lambda = lambda;
        }

        Expression getMapExpression() {
            return mapExpression;
        }

        ArrayMap toArrayMap() {
            return new ArrayMap(lambda);
        }
    }
}
