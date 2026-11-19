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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.CaseWhenClause;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionName;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.Type;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.spi.pushdown.ConnectorLike;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Connectors match a call by its name, and a UDF may carry any name, a builtin's included; the converter has to
 * say which calls are builtins.
 */
class ExprToConnectorFunctionCallTest {

    private static Expr varchar(String value) {
        return new VarcharLiteral(value).toLegacyLiteral();
    }

    private static Expr integer(int value) {
        return new IntegerLiteral(value).toLegacyLiteral();
    }

    /** A call as Nereids translates it: the catalog function, a UDF's under its database, and its arguments. */
    private static FunctionCallExpr call(String name, Function.BinaryType binaryType, Expr... args) {
        FunctionName functionName = binaryType == Function.BinaryType.BUILTIN
                ? new FunctionName(name) : new FunctionName("udf_db", name);
        ScalarFunction function = new ScalarFunction(functionName,
                Arrays.stream(args).map(Expr::getType).collect(Collectors.toList()), Type.BOOLEAN, false,
                binaryType, true, true);
        return new FunctionCallExpr(function, new FunctionParams(false, Arrays.asList(args)), true);
    }

    @Test
    void udfsAreFlaggedWhereTheBuiltinOfTheSameNameIsNot() {
        ConnectorFunctionCall builtin = (ConnectorFunctionCall) ExprToConnectorExpressionConverter.convert(
                call("starts_with", Function.BinaryType.BUILTIN, varchar("abc"), varchar("a")));
        Assertions.assertTrue(builtin.isBuiltin());
        for (Function.BinaryType udf : Arrays.asList(Function.BinaryType.JAVA_UDF, Function.BinaryType.PYTHON_UDF)) {
            ConnectorFunctionCall converted = (ConnectorFunctionCall) ExprToConnectorExpressionConverter.convert(
                    call("starts_with", udf, varchar("abc"), varchar("a")));
            Assertions.assertFalse(converted.isBuiltin(), udf.name());
            // The name and the arguments are the builtin's, so the flag is all that tells the two apart.
            Assertions.assertEquals(builtin.getFunctionName(), converted.getFunctionName());
            Assertions.assertEquals(builtin.getArguments(), converted.getArguments());
            Assertions.assertNotEquals(builtin, converted);
        }
    }

    @Test
    void udfsNamedLikeOrRegexpStayFunctionCalls() {
        for (String name : Arrays.asList("like", "regexp")) {
            Assertions.assertInstanceOf(ConnectorLike.class, ExprToConnectorExpressionConverter.convert(
                    call(name, Function.BinaryType.BUILTIN, varchar("abc"), varchar("a%"))), name);
            ConnectorExpression udf = ExprToConnectorExpressionConverter.convert(
                    call(name, Function.BinaryType.JAVA_UDF, varchar("abc"), varchar("a%")));
            Assertions.assertInstanceOf(ConnectorFunctionCall.class, udf, name);
            Assertions.assertFalse(((ConnectorFunctionCall) udf).isBuiltin(), name);
        }
    }

    @Test
    void fallbackIsBuiltinOnlyWhenTheExpressionItRendersCallsNoUdf() {
        for (Function.BinaryType binaryType : Arrays.asList(Function.BinaryType.BUILTIN,
                Function.BinaryType.JAVA_UDF)) {
            // The converter does not model CASE, so the whole expression arrives as its SQL text.
            Expr caseExpr = new CaseExpr(Collections.singletonList(new CaseWhenClause(
                    call("starts_with", binaryType, varchar("abc"), varchar("a")), integer(1))), integer(0), false);
            ConnectorFunctionCall fallback =
                    (ConnectorFunctionCall) ExprToConnectorExpressionConverter.convert(caseExpr);
            Assertions.assertTrue(fallback.getArguments().isEmpty());
            Assertions.assertTrue(fallback.getFunctionName().contains("starts_with"), fallback.getFunctionName());
            Assertions.assertEquals(binaryType == Function.BinaryType.BUILTIN, fallback.isBuiltin(),
                    binaryType.name());
        }
    }
}
