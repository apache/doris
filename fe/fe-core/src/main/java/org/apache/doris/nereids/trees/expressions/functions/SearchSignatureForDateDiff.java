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
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

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
 *
 * <p>A string column can hold zoned and zone-less rows together, so the overload alone does not suit
 * every row: the DATETIME string parser rejects zone offsets that the TIMESTAMPTZ one accepts, such as
 * {@code -14:21}. A call whose overload moves away from TIMESTAMPTZ therefore converts each string
 * argument through TIMESTAMPTZ, the cast it got before the move, and then to DATETIMEV2, which reads
 * the instant back in the session time zone. The rows TIMESTAMPTZ parses are still parsed, and a
 * zone-less row keeps its wall-clock time.
 */
public interface SearchSignatureForDateDiff extends ExplicitlyCastableSignature {

    @Override
    default FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        if (searchesTimeStampTzLast(getArguments())) {
            return ExplicitlyCastableSignature.super.searchSignature(withTimeStampTzLast(signatures));
        }
        return ExplicitlyCastableSignature.super.searchSignature(signatures);
    }

    @Override
    default Expression prepareBeforeTypeCoercion() {
        if (!searchesTimeStampTzLast(getArguments())) {
            return (Expression) this;
        }
        // convert only when trying TIMESTAMPTZ last is what moves the overload: beside a typed temporal
        // argument the number of casts decides, the call binds as it did, and so does its coercion
        List<FunctionSignature> signatures = getSignatures();
        if (!hasTimeStampTzArgument(ExplicitlyCastableSignature.super.searchSignature(signatures))
                || hasTimeStampTzArgument(
                        ExplicitlyCastableSignature.super.searchSignature(withTimeStampTzLast(signatures)))) {
            return (Expression) this;
        }
        // the converted arguments are no longer strings, so preparing the result again changes nothing
        return withChildren(argument -> argument.getDataType().isStringLikeType()
                ? TypeCoercionUtils.castIfNotSameType(
                        TypeCoercionUtils.castIfNotSameType(argument, TimeStampTzType.MAX), DateTimeV2Type.MAX)
                : argument);
    }

    /** whether the call has a string-typed argument and no string literal to inspect */
    static boolean searchesTimeStampTzLast(List<Expression> arguments) {
        return hasStringArgument(arguments) && !hasInspectableStringLiteral(arguments);
    }

    /** whether the signature takes a TIMESTAMPTZ argument */
    static boolean hasTimeStampTzArgument(FunctionSignature signature) {
        return signature.argumentsTypes.stream().anyMatch(DataType::isTimeStampTzType);
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
            if (hasTimeStampTzArgument(signature)) {
                timeStampTz.add(signature);
            } else {
                others.add(signature);
            }
        }
        return others.addAll(timeStampTz.build()).build();
    }
}
