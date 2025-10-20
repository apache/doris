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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GetFormat;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import com.google.common.collect.ImmutableSet;

/**
 * Binder for GET_FORMAT function.
 */
public class GetFormatFunctionBinder {

    public static final GetFormatFunctionBinder INSTANCE = new GetFormatFunctionBinder();

    private static final ImmutableSet<String> SUPPORTED_FORMATS = ImmutableSet.of("DATE", "DATETIME", "TIME");

    public static boolean isGetFormatFunction(String functionName) {
        return "GET_FORMAT".equalsIgnoreCase(functionName);
    }

    /**
     * bind get_format function that have non-expression arguments.
     *
     * @param unboundFunction unbound get_format function
     *
     * @return bound function
     */
    public Expression bind(UnboundFunction unboundFunction) {
        if (unboundFunction.arity() != 2) {
            throw new AnalysisException("Can not find function 'GET_FORMAT' with "
                    + unboundFunction.arity() + " arguments");
        }
        StringLiteral formatLiteral = parseFormatType(unboundFunction.child(0));
        Expression pattern = unboundFunction.child(1);
        return new GetFormat(formatLiteral, pattern);
    }

    private StringLiteral parseFormatType(Expression formatTypeExpr) {
        if (formatTypeExpr instanceof StringLiteral) {
            String formatType = ((StringLiteral) formatTypeExpr).getStringValue().toUpperCase();
            validateFormatType(formatType);
            return new StringLiteral(formatType);
        }
        if (formatTypeExpr instanceof SlotReference) {
            String formatType = ((SlotReference) formatTypeExpr).getName().toUpperCase();
            validateFormatType(formatType);
            return new StringLiteral(formatType);
        }
        throw new AnalysisException("Illegal first argument for GET_FORMAT: " + formatTypeExpr.toSql());
    }

    private void validateFormatType(String formatType) {
        if (!SUPPORTED_FORMATS.contains(formatType)) {
            throw new AnalysisException("Format type only support DATE, DATETIME and TIME, but get: " + formatType);
        }
    }
}
