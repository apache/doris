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
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

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

    /**
     * constructor
     */
    public AliasUdf(String name, List<DataType> argTypes, UnboundFunction unboundFunction,
            List<String> parameters, Expression... arguments) {
        super(name, arguments);
        this.argTypes = argTypes;
        this.unboundFunction = unboundFunction;
        this.parameters = parameters;
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

    @Override
    public boolean nullable() {
        return false;
    }

    /**
     * translate catalog alias function to nereids alias function
     */
    public static void translateToNereidsFunction(String dbName, AliasFunction function) {
        String functionSql = function.getOriginFunction().toSql();
        Expression parsedFunction = new NereidsParser().parseExpression(functionSql);

        Map<String, SlotReference> replaceMap = Maps.newHashMap();
        for (int i = 0; i < function.getNumArgs(); ++i) {
            replaceMap.put(function.getParameters().get(i),
                    new SlotReference(
                            function.getParameters().get(i),
                            DataType.fromCatalogType(function.getArgs()[i])));
        }

        Expression slotBoundFunction = VirtualSlotReplacer.INSTANCE.replace(parsedFunction, replaceMap);

        AliasUdf aliasUdf = new AliasUdf(
                function.functionName(),
                Arrays.stream(function.getArgs()).map(DataType::fromCatalogType).collect(Collectors.toList()),
                ((UnboundFunction) slotBoundFunction),
                function.getParameters());

        AliasUdfBuilder builder = new AliasUdfBuilder(aliasUdf);
        Env.getCurrentEnv().getFunctionRegistry().addUdf(dbName, aliasUdf.getName(), builder);
    }

    @Override
    public int arity() {
        return argTypes.size();
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        return new AliasUdf(getName(), argTypes, unboundFunction, parameters,
                children.toArray(new Expression[0]));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAliasUdf(this, context);
    }

    private static class VirtualSlotReplacer extends DefaultExpressionRewriter<Map<String, SlotReference>> {
        public static final VirtualSlotReplacer INSTANCE = new VirtualSlotReplacer();

        public Expression replace(Expression expression, Map<String, SlotReference> context) {
            return expression.accept(this, context);
        }

        @Override
        public Expression visitUnboundSlot(UnboundSlot slot, Map<String, SlotReference> context) {
            return context.get(slot.getName());
        }
    }
}
