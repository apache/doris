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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Signature search for the {@code *_diff} functions, which list {@code (TIMESTAMPTZ, TIMESTAMPTZ)} first.
 *
 * <p>{@link SearchSignature} scores a string literal against the TIMESTAMPTZ overload by whether it
 * carries a time zone, and breaks a remaining tie between the temporal overloads by list order. A
 * string-typed argument that is not a literal, such as a VARCHAR column or a subquery or UNION
 * output slot, gives it nothing to score, so the call bound TIMESTAMPTZ by list order and the value
 * was cast through the session time zone (apache/doris#66120).
 *
 * <p>For that case only, when the call has a string-typed argument and no string literal to inspect,
 * the TIMESTAMPTZ overload is tried after the others, so the call binds DATETIMEV2 as it does for
 * zone-less literals. A call with an inspectable string literal is searched in list order with the
 * usual score, so results for zoned, zone-less and mixed literals do not change.
 */
public interface SearchSignatureForDateDiff extends ExplicitlyCastableSignature {

    @Override
    default FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        List<Expression> arguments = getArguments();
        if (hasStringArgument(arguments) && !hasInspectableStringLiteral(arguments)) {
            return ExplicitlyCastableSignature.super.searchSignature(withTimeStampTzLast(signatures));
        }
        return ExplicitlyCastableSignature.super.searchSignature(signatures);
    }

    /** whether any argument is string typed */
    static boolean hasStringArgument(List<Expression> arguments) {
        return arguments.stream().anyMatch(argument -> argument.getDataType().isStringLikeType());
    }

    /** whether any argument is a string literal that SearchSignature scores, by its own test */
    static boolean hasInspectableStringLiteral(List<Expression> arguments) {
        return arguments.stream().anyMatch(argument -> !argument.isNullLiteral()
                && argument.getDataType().isStringLikeType()
                && ExpressionUtils.getLiteralAfterUnwrapNullable(argument).isPresent());
    }

    /** the same signatures, with every TIMESTAMPTZ overload moved behind the others in order */
    static List<FunctionSignature> withTimeStampTzLast(List<FunctionSignature> signatures) {
        ImmutableList.Builder<FunctionSignature> others = ImmutableList.builder();
        ImmutableList.Builder<FunctionSignature> timeStampTz = ImmutableList.builder();
        for (FunctionSignature signature : signatures) {
            if (signature.argumentsTypes.stream().anyMatch(DataType::isTimeStampTzType)) {
                timeStampTz.add(signature);
            } else {
                others.add(signature);
            }
        }
        return others.addAll(timeStampTz.build()).build();
    }
}
