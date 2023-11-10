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

package org.apache.doris.nereids.parser.trino;

import org.apache.doris.nereids.analyzer.PlaceholderExpression;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.parser.ParserContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Trino function transformer
 */
public class TrinoFnCallTransformer extends AbstractFnCallTransformer {
    private final UnboundFunction targetFunction;
    private final List<PlaceholderExpression> targetArguments;
    private final boolean variableArgument;
    private final int sourceArgumentsNum;

    /**
     * Trino function transformer, mostly this handle common function.
     */
    public TrinoFnCallTransformer(UnboundFunction targetFunction,
            boolean variableArgument,
            int sourceArgumentsNum) {
        this.targetFunction = targetFunction;
        this.variableArgument = variableArgument;
        this.sourceArgumentsNum = sourceArgumentsNum;
        PlaceholderCollector placeHolderCollector = new PlaceholderCollector(variableArgument);
        placeHolderCollector.visit(targetFunction, null);
        this.targetArguments = placeHolderCollector.getPlaceholderExpressions();
    }

    @Override
    protected boolean check(String sourceFnName,
            List<Expression> sourceFnTransformedArguments,
            ParserContext context) {
        List<Class<? extends Expression>> sourceFnTransformedArgClazz = sourceFnTransformedArguments.stream()
                .map(Expression::getClass)
                .collect(Collectors.toList());
        if (variableArgument) {
            if (targetArguments.isEmpty()) {
                return false;
            }
            Class<? extends Expression> targetArgumentClazz = targetArguments.get(0).getDelegateClazz();
            for (Expression argument : sourceFnTransformedArguments) {
                if (!targetArgumentClazz.isAssignableFrom(argument.getClass())) {
                    return false;
                }
            }
        }
        if (sourceFnTransformedArguments.size() != sourceArgumentsNum) {
            return false;
        }
        for (int i = 0; i < targetArguments.size(); i++) {
            if (!targetArguments.get(i).getDelegateClazz().isAssignableFrom(sourceFnTransformedArgClazz.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Function transform(String sourceFnName,
            List<Expression> sourceFnTransformedArguments,
            ParserContext context) {
        return targetFunction.withChildren(sourceFnTransformedArguments);
    }

    /**
     * This is the collector for placeholder expression, which placeholder expression
     * identify the expression that we want to use later but current now is not confirmed.
     */
    public static final class PlaceholderCollector extends DefaultExpressionVisitor<Void, Void> {

        private final List<PlaceholderExpression> placeholderExpressions = new ArrayList<>();
        private final boolean variableArgument;

        public PlaceholderCollector(boolean variableArgument) {
            this.variableArgument = variableArgument;
        }

        @Override
        public Void visitPlaceholderExpression(PlaceholderExpression placeholderExpression, Void context) {

            if (variableArgument) {
                placeholderExpressions.add(placeholderExpression);
                return null;
            }
            placeholderExpressions.set(placeholderExpression.getPosition() - 1, placeholderExpression);
            return null;
        }

        public List<PlaceholderExpression> getPlaceholderExpressions() {
            return placeholderExpressions;
        }
    }
}
