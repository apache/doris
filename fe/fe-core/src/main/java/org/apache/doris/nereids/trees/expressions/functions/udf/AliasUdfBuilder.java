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

import org.apache.doris.common.util.ReflectionUtils;
import org.apache.doris.nereids.rules.expression.rules.FunctionBinder;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * alias function builder
 */
public class AliasUdfBuilder extends UdfBuilder {
    private final AliasUdf aliasUdf;

    public AliasUdfBuilder(AliasUdf aliasUdf) {
        this.aliasUdf = aliasUdf;
    }

    @Override
    public List<DataType> getArgTypes() {
        return aliasUdf.getArgTypes();
    }

    @Override
    public boolean canApply(List<?> arguments) {
        if (arguments.size() != aliasUdf.arity()) {
            return false;
        }
        for (Object argument : arguments) {
            if (!(argument instanceof Expression)) {
                Optional<Class> primitiveType = ReflectionUtils.getPrimitiveType(argument.getClass());
                if (!primitiveType.isPresent() || !Expression.class.isAssignableFrom(primitiveType.get())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public BoundFunction build(String name, List<?> arguments) {
        // use AliasFunction to process TypeCoercion
        BoundFunction boundAliasFunction = ((BoundFunction) aliasUdf.withChildren(arguments.stream()
                .map(Expression.class::cast).collect(Collectors.toList())));

        Expression processedExpression = TypeCoercionUtils.processBoundFunction(boundAliasFunction);
        List<Expression> inputs = processedExpression.getArguments();

        Expression boundFunction = FunctionBinder.INSTANCE.rewrite(aliasUdf.getUnboundFunction(), null);

        // replace the placeholder slot to the input expressions.
        // adjust input, parameter and replaceMap to be corresponding.
        Map<String, SlotReference> slots = ((Set<SlotReference>) boundFunction
                .collect(SlotReference.class::isInstance))
                .stream().collect(Collectors.toMap(SlotReference::getName, k -> k, (v1, v2) -> v2));

        Map<SlotReference, Expression> replaceMap = Maps.newHashMap();
        for (int i = 0; i < inputs.size(); ++i) {
            String parameter = aliasUdf.getParameters().get(i);
            Preconditions.checkArgument(slots.containsKey(parameter));
            replaceMap.put(slots.get(parameter), inputs.get(i));
        }

        return ((BoundFunction) SlotReplacer.INSTANCE.replace(boundFunction, replaceMap));
    }

    private static class SlotReplacer extends DefaultExpressionRewriter<Map<SlotReference, Expression>> {
        public static final SlotReplacer INSTANCE = new SlotReplacer();

        public Expression replace(Expression expression, Map<SlotReference, Expression> context) {
            return expression.accept(this, context);
        }

        @Override
        public Expression visitSlotReference(SlotReference slot, Map<SlotReference, Expression> context) {
            return context.get(slot);
        }
    }
}
