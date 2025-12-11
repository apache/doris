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

package org.apache.doris.nereids.trees.expressions.functions.udf;

import org.apache.doris.catalog.AliasFunction;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.qe.AutoCloseSessionVariable;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * alias function
 */
public class AliasUdf extends ScalarFunction implements ExplicitlyCastableSignature {
    private final UnboundFunction unboundFunction;
    private final List<String> parameters;
    private final List<DataType> argTypes;
    private final Map<String, String> sessionVariables;

    /**
     * constructor
     */
    public AliasUdf(String name, List<DataType> argTypes, UnboundFunction unboundFunction,
            List<String> parameters, Expression... arguments) {
        this(name, argTypes, unboundFunction, parameters, null, arguments);
    }

    /**
     * constructor with session variables
     */
    public AliasUdf(String name, List<DataType> argTypes, UnboundFunction unboundFunction,
            List<String> parameters, Map<String, String> sessionVariables, Expression... arguments) {
        super(name, arguments);
        this.argTypes = argTypes;
        this.unboundFunction = unboundFunction;
        this.parameters = parameters;
        this.sessionVariables = sessionVariables;
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return ImmutableList.of(FunctionSignature.of(NullType.INSTANCE, argTypes));
    }

    public List<String> getParameters() {
        return parameters;
    }

    public UnboundFunction getUnboundFunction() {
        return unboundFunction;
    }

    public List<DataType> getArgTypes() {
        return argTypes;
    }

    public Map<String, String> getSessionVariables() {
        return sessionVariables;
    }

    @Override
    public boolean nullable() {
        return false;
    }

    /**
     * translate catalog alias function to nereids alias function
     */
    public static void translateToNereidsFunction(String dbName, AliasFunction function) {
        String functionSql = function.getOriginFunction().toSqlWithoutTbl();
        Map<String, String> sessionVariables = function.getSessionVariables();
        Expression parsedFunction;
        try (AutoCloseSessionVariable autoClose = new AutoCloseSessionVariable(ConnectContext.get(),
                sessionVariables)) {
            parsedFunction = new NereidsParser().parseExpression(functionSql);
        }
        AliasUdf aliasUdf = new AliasUdf(
                function.functionName(),
                Arrays.stream(function.getArgs()).map(DataType::fromCatalogType).collect(Collectors.toList()),
                ((UnboundFunction) parsedFunction),
                function.getParameters(),
                sessionVariables);

        AliasUdfBuilder builder = new AliasUdfBuilder(aliasUdf);
        Env.getCurrentEnv().getFunctionRegistry().addUdf(dbName, aliasUdf.getName(), builder);
    }

    @Override
    public int arity() {
        return argTypes.size();
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        return new AliasUdf(getName(), argTypes, unboundFunction, parameters, sessionVariables,
                children.toArray(new Expression[0]));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAliasUdf(this, context);
    }
}
