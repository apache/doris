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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.TimeFormat;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Composite time extraction using the existing TIME formatter.
 */
public interface TimeExtract extends ExplicitlyCastableSignature, RewriteWhenAnalyze {
    String getTimeFormat();

    @Override
    default FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        // Let temporal literals select their precise type. String expressions need both parsers
        // because their values, unlike literals, are only available during execution.
        // Keep other types and volatile expressions on their historical conversion path.
        // The string fallback evaluates its source in two casts, so it must be deterministic.
        if (!getArgumentType(0).isStringLikeType() || getArgument(0).containsVolatileExpression()
                || ExpressionUtils.getLiteralAfterUnwrapNullable(getArgument(0)).isPresent()) {
            signatures = signatures.stream()
                    .filter(signature -> !signature.getArgType(0).isStringLikeType())
                    .collect(ImmutableList.toImmutableList());
        }
        return ExplicitlyCastableSignature.super.searchSignature(signatures);
    }

    @Override
    default Expression rewriteWhenAnalyze() {
        Expression argument = getArgument(0);
        if (argument.getDataType().isStringLikeType()) {
            // Preserve DATETIME parsing (including dates and timezone suffixes), then accept
            // time-only strings. Both casts use six digits so column values retain microseconds.
            argument = new Coalesce(
                    new Cast(new Cast(argument, DateTimeV2Type.MAX), TimeV2Type.MAX),
                    new Cast(argument, TimeV2Type.MAX));
        } else if (!(argument.getDataType() instanceof TimeV2Type)) {
            return (Expression) this;
        }
        return TypeCoercionUtils.processBoundFunction(new TimeFormat(argument, new VarcharLiteral(getTimeFormat())));
    }
}
